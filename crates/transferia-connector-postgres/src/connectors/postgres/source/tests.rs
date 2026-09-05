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
        serde_yaml::from_str("host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: false\ntables: {rules: []}\n").unwrap();
    assert!(config.validate().is_err());
}

#[test]
fn snapshot_copy_to_format_defaults_to_binary_and_accepts_explicit_text() {
    let binary: PostgresSourceConfig = serde_yaml::from_str(
        "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  rules:\n    - include: public.events\n",
    )
    .unwrap();
    let text: PostgresSourceConfig = serde_yaml::from_str(
        "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ncopy_to_format: text\ntables:\n  rules:\n    - include: public.events\n",
    )
    .unwrap();

    assert_eq!(binary.copy_to_format, PostgresCopyFormat::Binary);
    assert_eq!(text.copy_to_format, PostgresCopyFormat::Text);
}

#[test]
fn source_rejects_the_old_connection_string() {
    assert!(serde_yaml::from_str::<PostgresSourceConfig>(
        "connection: host=localhost port=5432\ntrusted_plaintext: true\ntables: {rules: []}\n"
    )
    .is_err());
}

#[test]
fn delivery_type_alone_selects_postgres_record_semantics() {
    for settings in [
        "",
        "\nreplication:\n  plugin:\n    type: pgoutput\n    publication: transferia_publication\n",
    ] {
        let config: PostgresSourceConfig = serde_yaml::from_str(&format!(
            "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  rules:\n    - include: public.events\n{settings}",
        )).unwrap();
        let connector =
            PostgresSourceConnector::from_config(config, Arc::new(MetricsRegistry::default()))
                .unwrap();
        for mode in [
            DeliveryType::Batch,
            DeliveryType::Stream,
            DeliveryType::BatchAndStream,
        ] {
            let descriptor = connector.compatibility(mode);
            assert!(descriptor.supports_delivery_type(mode));
            assert_eq!(
                descriptor.record_semantics(),
                Some(if mode == DeliveryType::Batch {
                    RecordSemantics::AppendOnly
                } else {
                    RecordSemantics::Changelog
                })
            );
            assert_eq!(
                descriptor.source_behavior(),
                Some(if mode == DeliveryType::Batch {
                    SourceBehavior::FiniteAppendOnlyRows
                } else {
                    SourceBehavior::ChangelogRows
                })
            );
        }
    }
}

#[test]
fn source_schema_inlines_replication_plugin_only_for_replication_modes() {
    let schema = serde_json::to_value(schemars::schema_for!(PostgresSourceConfig)).unwrap();
    assert_eq!(
        schema.pointer("/x-ui/capabilities"),
        Some(&serde_json::json!({
            "component": "source", "key": "postgres",
            "batch_stream_handoff": "exact_switchover",
            "delivery_modes": ["batch", "stream", "batch_and_stream"],
            "record_semantics": ["append_only", "changelog"]
        }))
    );
    assert_eq!(
        schema.pointer("/properties/replication/x-ui"),
        Some(&serde_json::json!({
            "widget": "inline_object", "section": "advanced",
            "delivery_types": ["stream", "batch_and_stream"]
        }))
    );
    assert!(schema
        .pointer("/$defs/PostgresReplicationConfig/properties/plugin/x-ui/section")
        .is_none());
    assert!(schema.pointer("/properties/replication/anyOf").is_none());
    assert!(schema
        .pointer("/$defs/PostgresReplicationConfig/x-ui/capabilities")
        .is_none());
}

#[tokio::test]
async fn batch_preparation_needs_no_replication_context_and_cannot_reuse_stream_caches() {
    let config: PostgresSourceConfig = serde_yaml::from_str(
        "host: 127.0.0.1\nport: 1\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  rules:\n    - include: public.events\n",
    ).unwrap();
    let context = |delivery_type| transferia_registry::SourceExecutionContext {
        request: transferia_core::delivery::DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: tokio_util::sync::CancellationToken::new(),
        delivery_type,
        replay_identity: None,
        durable: transferia_test_support::durable_context(),
    };
    let connector =
        PostgresSourceConnector::from_config(config.clone(), Arc::new(MetricsRegistry::default()))
            .unwrap();
    // No server is listening: batch preparation must not connect or validate a slot ID.
    assert!(connector
        .prepare_execution(context(DeliveryType::Batch))
        .await
        .unwrap()
        .is_none());
    let error = connector
        .prepare_execution(context(DeliveryType::Stream))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("different delivery type"));
    let stream =
        PostgresSourceConnector::from_config(config, Arc::new(MetricsRegistry::default())).unwrap();
    let error = stream
        .prepare_execution(context(DeliveryType::Stream))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("replay identity"));
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
