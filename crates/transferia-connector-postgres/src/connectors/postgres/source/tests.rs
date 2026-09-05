use std::sync::Arc;

use arrow::datatypes::DataType;

use super::config::PostgresSourceConfig;
use super::connector::{
    classify_replication_connector_error, incoming_user_schema,
    require_replication_replay_identity, validate_replication_table_identities, DiscoveredTable,
    PostgresSourceConnector,
};
use super::TableConfig;
use crate::connectors::postgres::src_stream::replication_safety_violation;
use crate::connectors::postgres::PostgresCopyFormat;
use crate::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::{RecordSemantics, SourceBehavior};
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::SourceConnector;

#[test]
fn replication_safety_violations_are_fatal_source_build_failures() {
    let error = replication_safety_violation(anyhow::anyhow!(
        "durable PostgreSQL replication identity changed"
    ));
    let classified = classify_replication_connector_error(error);
    let failure = classified
        .downcast_ref::<transferia_core::failure::DataPlaneFailure>()
        .expect("replication safety violation must retain a data-plane disposition");
    assert!(!failure.is_retryable());
}

#[test]
fn transient_source_build_failures_keep_their_original_classification() {
    let classified = classify_replication_connector_error(anyhow::anyhow!(
        "temporary PostgreSQL connection failure"
    ));
    assert!(classified
        .downcast_ref::<transferia_core::failure::DataPlaneFailure>()
        .is_none());
    assert_eq!(
        classified.to_string(),
        "temporary PostgreSQL connection failure"
    );
}

#[test]
fn replication_requires_a_nonempty_replay_identity_before_durable_state() {
    for replay_identity in [None, Some(Arc::<str>::from(""))] {
        let error = require_replication_replay_identity(replay_identity)
            .expect_err("missing replay identity must fail closed");
        assert!(crate::connectors::postgres::src_stream::is_replication_safety_violation(&error));
    }
}

#[test]
fn snapshot_and_replication_share_nullable_incoming_user_fields_without_weakening_storage() {
    let stored = transferia_core::data::schema::DatasetSchema::new(vec![
        transferia_core::data::schema::SchemaColumn::new("id".into(), DataType::Int64, false),
        transferia_core::data::schema::SchemaColumn::new("payload".into(), DataType::Utf8, true),
    ]);

    let incoming = incoming_user_schema(&stored);

    assert!(incoming.columns.iter().all(|column| column.nullable));
    assert!(!stored.columns[0].nullable);
    assert!(stored.columns[1].nullable);
}

#[test]
fn source_config_requires_explicit_plaintext_trust_and_tables() {
    let config: PostgresSourceConfig =
        serde_yaml::from_str("host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: false\ntables: []\n").unwrap();
    assert!(config.validate().is_err());
}

#[test]
fn snapshot_copy_to_format_defaults_to_binary_and_accepts_explicit_text() {
    let binary: PostgresSourceConfig = serde_yaml::from_str(
        "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  - name: events\n",
    )
    .unwrap();
    let text: PostgresSourceConfig = serde_yaml::from_str(
        "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ncopy_to_format: text\ntables:\n  - name: events\n",
    )
    .unwrap();

    assert_eq!(binary.copy_to_format, PostgresCopyFormat::Binary);
    assert_eq!(text.copy_to_format, PostgresCopyFormat::Text);
}

#[test]
fn source_rejects_the_old_connection_string() {
    assert!(serde_yaml::from_str::<PostgresSourceConfig>(
        "connection: host=localhost port=5432\ntrusted_plaintext: true\ntables: []\n"
    )
    .is_err());
}

#[test]
fn snapshot_and_replication_declare_distinct_record_semantics() {
    let snapshot: PostgresSourceConfig = serde_yaml::from_str(
        "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  - name: events\n",
    )
    .unwrap();
    let replication: PostgresSourceConfig = serde_yaml::from_str(
        "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  - name: events\nreplication:\n  plugin:\n    type: pgoutput\n    publication: transferia_publication\n",
    )
    .unwrap();

    let snapshot =
        PostgresSourceConnector::from_config(snapshot, Arc::new(MetricsRegistry::default()))
            .unwrap()
            .compatibility();
    let replication =
        PostgresSourceConnector::from_config(replication, Arc::new(MetricsRegistry::default()))
            .unwrap()
            .compatibility();

    assert_eq!(
        snapshot.source_behavior(),
        Some(SourceBehavior::FiniteAppendOnlyRows)
    );
    assert_eq!(
        snapshot.record_semantics(),
        Some(RecordSemantics::AppendOnly)
    );
    assert_eq!(
        replication.source_behavior(),
        Some(SourceBehavior::ChangelogRows)
    );
    assert_eq!(
        replication.record_semantics(),
        Some(RecordSemantics::Changelog)
    );
    assert!(snapshot.supports_delivery_type(DeliveryType::Batch));
    assert!(!snapshot.supports_delivery_type(DeliveryType::Stream));
    assert!(!snapshot.supports_delivery_type(DeliveryType::BatchAndStream));
    assert!(!replication.supports_delivery_type(DeliveryType::Batch));
    assert!(replication.supports_delivery_type(DeliveryType::Stream));
    assert!(replication.supports_delivery_type(DeliveryType::BatchAndStream));
}

#[test]
fn source_schema_declares_snapshot_and_replication_capability_overrides() {
    let schema = serde_json::to_value(schemars::schema_for!(PostgresSourceConfig)).unwrap();
    assert_eq!(
        schema.pointer("/x-ui/capabilities"),
        Some(&serde_json::json!({
            "component": "source",
            "key": "snapshot",
            "delivery_modes": ["batch"],
            "record_semantics": ["append_only"]
        }))
    );
    assert_eq!(
        schema.pointer("/$defs/PostgresReplicationConfig/x-ui/capabilities"),
        Some(&serde_json::json!({
            "component": "source",
            "key": "replication",
            "delivery_modes": ["stream", "batch_and_stream"],
            "record_semantics": ["changelog"]
        }))
    );
}

#[test]
fn replication_requires_an_exact_primary_key_or_full_old_row_identity() {
    let table = |replica_identity: &str, primary_key: bool| DiscoveredTable {
        config: TableConfig {
            schema: "public".to_owned(),
            name: "events".to_owned(),
        },
        schema: transferia_core::data::schema::DatasetSchema::new(vec![
            transferia_core::data::schema::SchemaColumn::new(
                "id".to_owned(),
                DataType::Int64,
                false,
            )
            .with_constraints(primary_key, false, None),
        ]),
        type_oids: vec![20],
        replica_identity_full: replica_identity == "f",
        replica_identity: replica_identity.to_owned(),
        relation_oid: 42,
    };

    validate_replication_table_identities(&[table("d", true)]).unwrap();
    validate_replication_table_identities(&[table("f", false)]).unwrap();

    let no_primary_key = validate_replication_table_identities(&[table("d", false)])
        .unwrap_err()
        .to_string();
    assert!(
        no_primary_key.contains("has no primary key"),
        "{no_primary_key}"
    );

    let index_identity = validate_replication_table_identities(&[table("i", true)])
        .unwrap_err()
        .to_string();
    assert!(
        index_identity.contains("USING INDEX")
            && index_identity.contains("primary-key row identity"),
        "{index_identity}"
    );

    let no_identity = validate_replication_table_identities(&[table("n", true)])
        .unwrap_err()
        .to_string();
    assert!(no_identity.contains("IDENTITY NOTHING"), "{no_identity}");
}
