use std::sync::Arc;

use arrow::datatypes::DataType;

use super::config::PostgresSourceConfig;
use super::connector::{incoming_user_schema, PostgresSourceConnector};
use crate::connectors::postgres::PostgresCopyFormat;
use crate::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::{RecordSemantics, SourceBehavior};
use transferia_registry::SourceConnector;

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
        "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  - name: events\nreplication:\n  slot: transferia_slot\n  decoder:\n    type: pgoutput\n    publication: transferia_publication\n",
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
}
