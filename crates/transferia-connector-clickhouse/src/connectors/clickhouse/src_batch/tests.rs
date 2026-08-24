use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::metrics::MetricsRegistry;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::SystemColumnKind;

use super::*;

#[test]
fn source_contract_has_no_shard_group() {
    let schema = serde_json::to_value(schemars::schema_for!(ClickHouseSourceConfig))
        .expect("ClickHouse source schema must serialize");
    assert!(schema.pointer("/properties/shard_group").is_none());

    let legacy = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\nshard_group: legacy\ntables: [{database: default, name: events}]\n";
    assert!(serde_yaml::from_str::<ClickHouseSourceConfig>(legacy).is_err());
}

#[test]
fn derives_output_name_from_the_source_table() {
    let yaml = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\ntables: [{database: default, name: events}]\n";
    assert!(ClickHouseSourceConnector::from_config(
        serde_yaml::from_str(yaml).unwrap(),
        Arc::new(MetricsRegistry::new())
    )
    .is_ok());

    let invalid = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\ntables: [{database: default, name: bad-name}]\n";
    assert!(ClickHouseSourceConnector::from_config(
        serde_yaml::from_str(invalid).unwrap(),
        Arc::new(MetricsRegistry::new())
    )
    .is_err());
}

#[test]
fn supports_verified_tls() {
    let value = serde_yaml::from_str("hosts: [localhost]\nport: 9440\ntrusted_plaintext: false\ntls_ca_file: /tmp/ca.pem\nusername: default\ntables: [{database: default, name: events}]\n").unwrap();
    assert!(
        ClickHouseSourceConnector::from_config(value, Arc::new(MetricsRegistry::new())).is_ok()
    );
}

fn round_trip_schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("value".into(), DataType::Int64, false),
        SchemaColumn::new("_system_topic".into(), DataType::Utf8, false),
        SchemaColumn::new("_system_partition".into(), DataType::Int64, false),
        SchemaColumn::new("_system_offset".into(), DataType::Int64, false),
        SchemaColumn::new("_system_message_index".into(), DataType::UInt64, false),
    ])
}

#[test]
fn recognizes_complete_round_trip_system_columns() -> anyhow::Result<()> {
    let columns = connector::classify_system_columns(&round_trip_schema())?;
    assert_eq!(columns.iter().len(), 4);
    assert_eq!(
        columns
            .get(SystemColumnKind::MessageIndex)
            .map(|column| column.index),
        Some(4)
    );
    Ok(())
}

#[test]
fn rejects_partial_round_trip_system_columns() {
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "_system_topic".into(),
        DataType::Utf8,
        false,
    )]);
    assert!(connector::classify_system_columns(&schema).is_err());
}

#[test]
fn converts_clickhouse_string_system_topic_to_utf8() -> anyhow::Result<()> {
    let physical_system_columns = connector::classify_system_columns(&round_trip_schema())?;
    let table = connector::DiscoveredTable {
        config: config::TableConfig {
            database: "db1".into(),
            name: "my_table".into(),
        },
        schema: round_trip_schema(),
        physical_system_columns,
    };
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("_system_topic", DataType::Binary, false),
            Field::new("_system_partition", DataType::Int64, false),
            Field::new("_system_offset", DataType::Int64, false),
            Field::new("_system_message_index", DataType::UInt64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(BinaryArray::from_vec(vec![b"topic"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            Arc::new(Int64Array::from(vec![42])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
        ],
    )?;

    let normalized = reader::normalize_snapshot_schema(&batch, &table)?;
    assert_eq!(normalized.schema().field(1).data_type(), &DataType::Utf8);
    Ok(())
}
