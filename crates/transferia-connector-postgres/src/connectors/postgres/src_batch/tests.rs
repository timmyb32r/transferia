use std::sync::Arc;

use super::config::PostgresSourceConfig;
use super::connector::PostgresSourceConnector;
use super::reader::source_column_expression;
use arrow::datatypes::DataType;
use tokio_postgres::types::{Kind, Type};

use crate::connectors::postgres::common::postgres_to_arrow;
use crate::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::{RecordSemantics, SourceBehavior};
use transferia_registry::SourceConnector;

#[test]
fn source_config_requires_explicit_plaintext_trust_and_tables() {
    let config: PostgresSourceConfig =
        serde_yaml::from_str("host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: false\ntables: []\n").unwrap();
    assert!(config.validate().is_err());
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

    let snapshot = PostgresSourceConnector::from_config(
        snapshot,
        Arc::new(MetricsRegistry::default()),
    )
    .unwrap()
    .compatibility();
    let replication = PostgresSourceConnector::from_config(
        replication,
        Arc::new(MetricsRegistry::default()),
    )
    .unwrap()
    .compatibility();

    assert_eq!(snapshot.source_behavior(), Some(SourceBehavior::FiniteAppendOnlyRows));
    assert_eq!(snapshot.record_semantics(), Some(RecordSemantics::AppendOnly));
    assert_eq!(replication.source_behavior(), Some(SourceBehavior::ChangelogRows));
    assert_eq!(replication.record_semantics(), Some(RecordSemantics::Changelog));
}

#[test]
fn postgres_types_use_native_arrow_where_lossless_and_canonical_text_otherwise() {
    for (postgres, arrow) in [
        (Type::BOOL, DataType::Boolean),
        (Type::CHAR, DataType::Int8),
        (Type::INT2, DataType::Int16),
        (Type::INT4, DataType::Int32),
        (Type::INT8, DataType::Int64),
        (Type::OID, DataType::UInt32),
        (Type::FLOAT4, DataType::Float32),
        (Type::FLOAT8, DataType::Float64),
        (Type::BYTEA, DataType::Binary),
        (Type::TEXT, DataType::Utf8),
        (Type::VARCHAR, DataType::Utf8),
        (Type::BPCHAR, DataType::Utf8),
        (Type::NAME, DataType::Utf8),
    ] {
        assert_eq!(postgres_to_arrow(&postgres).unwrap(), arrow);
        assert_eq!(
            source_column_expression("mixed\"case", &postgres).unwrap(),
            "\"mixed\"\"case\""
        );
    }

    for postgres in [
        Type::NUMERIC,
        Type::MONEY,
        Type::DATE,
        Type::TIME,
        Type::TIMETZ,
        Type::TIMESTAMP,
        Type::TIMESTAMPTZ,
        Type::INTERVAL,
        Type::JSON,
        Type::JSONB,
        Type::XML,
        Type::UUID,
        Type::INET,
        Type::CIDR,
        Type::MACADDR,
        Type::MACADDR8,
        Type::BIT,
        Type::VARBIT,
        Type::POINT,
        Type::LINE,
        Type::LSEG,
        Type::BOX,
        Type::PATH,
        Type::POLYGON,
        Type::CIRCLE,
        Type::INT4_ARRAY,
        Type::INT4_RANGE,
        Type::INT4MULTI_RANGE,
    ] {
        assert_eq!(postgres_to_arrow(&postgres).unwrap(), DataType::Utf8);
        assert_eq!(
            source_column_expression("value", &postgres).unwrap(),
            "\"value\"::text AS \"value\""
        );
    }
}

#[test]
fn user_defined_postgres_types_are_lossless_text_and_pseudo_types_fail_closed() {
    for kind in [
        Kind::Simple,
        Kind::Enum(vec!["one".to_owned(), "two".to_owned()]),
        Kind::Array(Type::INT4),
        Kind::Range(Type::INT4),
        Kind::Multirange(Type::INT4_RANGE),
        Kind::Domain(Type::TEXT),
        Kind::Composite(Vec::new()),
    ] {
        let data_type = Type::new("custom".to_owned(), 80_000, kind, "public".to_owned());
        assert_eq!(postgres_to_arrow(&data_type).unwrap(), DataType::Utf8);
        assert_eq!(
            source_column_expression("value", &data_type).unwrap(),
            "\"value\"::text AS \"value\""
        );
    }

    let pseudo = Type::new(
        "custom_pseudo".to_owned(),
        80_001,
        Kind::Pseudo,
        "public".to_owned(),
    );
    assert!(postgres_to_arrow(&pseudo).is_err());
    assert!(source_column_expression("value", &pseudo).is_err());
}
