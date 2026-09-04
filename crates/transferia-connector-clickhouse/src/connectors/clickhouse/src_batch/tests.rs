use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int64Array, TimestampMillisecondArray, TimestampSecondArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::connectors::clickhouse::ClickHouseCompression;
use crate::metrics::MetricsRegistry;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::SystemColumnKind;

use super::connector::source_arrow_type;
use super::*;

#[test]
fn source_preserves_date_and_second_timestamp_types() -> anyhow::Result<()> {
    assert_eq!(
        source_arrow_type(&"Date".parse()?, "Date")?,
        DataType::Date32
    );
    assert_eq!(
        source_arrow_type(&"Date32".parse()?, "Date32")?,
        DataType::Date32
    );
    assert_eq!(
        source_arrow_type(&"DateTime64(0)".parse()?, "DateTime64(0)")?,
        DataType::Timestamp(TimeUnit::Second, None),
    );
    assert_eq!(
        source_arrow_type(&"DateTime64(0, 'UTC')".parse()?, "DateTime64(0, 'UTC')",)?,
        DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
    );
    assert_eq!(
        source_arrow_type(
            &"Nullable(DateTime64(0))".parse()?,
            "Nullable(DateTime64(0))",
        )?,
        DataType::Timestamp(TimeUnit::Second, None),
    );
    assert_eq!(
        source_arrow_type(
            &"Nullable(DateTime('Europe/Moscow'))".parse()?,
            "Nullable(DateTime('Europe/Moscow'))",
        )?,
        DataType::Timestamp(TimeUnit::Second, Some(Arc::from("Europe/Moscow")),),
    );
    Ok(())
}

#[test]
fn explicit_primary_key_is_validated_and_never_inferred_from_clickhouse_sorting() {
    let base = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\n";
    let duplicate =
        format!("{base}tables: [{{database: default, name: events, primary_key: [id, id]}}]\n");
    assert!(ClickHouseSourceConnector::from_config(
        serde_yaml::from_str(&duplicate).unwrap(),
        Arc::new(MetricsRegistry::new()),
    )
    .is_err());

    let mut schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("nullable".into(), DataType::Int64, true),
        SchemaColumn::new("_system_topic".into(), DataType::Utf8, false),
    ]);
    let table = config::TableConfig {
        database: "default".into(),
        name: "events".into(),
        primary_key: vec!["id".into()],
    };
    connector::apply_declared_primary_key(&mut schema, &table).unwrap();
    assert!(schema.columns[0].primary_key);
    assert!(!schema.columns[1].primary_key);

    for (key, expected) in [
        ("missing", "does not exist"),
        ("nullable", "must be non-nullable"),
        ("_system_topic", "cannot be a primary key"),
    ] {
        let mut candidate = schema.clone();
        let table = config::TableConfig {
            database: "default".into(),
            name: "events".into(),
            primary_key: vec![key.into()],
        };
        let error = connector::apply_declared_primary_key(&mut candidate, &table)
            .expect_err("invalid primary key must fail discovery");
        assert!(error.to_string().contains(expected), "{error:#}");
    }
}

#[test]
fn source_contract_has_no_shard_group() {
    let schema = serde_json::to_value(schemars::schema_for!(ClickHouseSourceConfig))
        .expect("ClickHouse source schema must serialize");
    assert!(schema.pointer("/properties/shard_group").is_none());

    let legacy = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\nshard_group: legacy\ntables: [{database: default, name: events}]\n";
    assert!(serde_yaml::from_str::<ClickHouseSourceConfig>(legacy).is_err());
}

#[test]
fn source_defaults_use_bounded_high_throughput_parquet_settings() {
    let config: ClickHouseSourceConfig = serde_yaml::from_str(
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\ntables: [{database: default, name: events}]\n",
    )
    .unwrap();

    assert_eq!(config.batch_rows, 65_409);
    assert_eq!(config.http_port, 8123);
    assert!(matches!(
        config.snapshot_reader,
        ClickHouseSnapshotReader::Parquet {
            compression: ClickHouseParquetCompression::Zstd,
            max_threads: 32,
            row_group_rows: 250_000,
            decode_threads: 16,
            max_response_bytes: 2_147_483_648,
        }
    ));
}

#[test]
fn source_accepts_zstd_parquet_and_native_reader_profiles() {
    let base = "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\nusername: default\ntables: [{database: default, name: events}]\n";
    let parquet: ClickHouseSourceConfig = serde_yaml::from_str(&format!(
        "{base}snapshot_reader: {{ type: parquet, compression: zstd, max_threads: 32, row_group_rows: 250000, decode_threads: 16, max_response_bytes: 1073741824 }}\n"
    ))
    .unwrap();
    assert!(matches!(
        parquet.snapshot_reader,
        ClickHouseSnapshotReader::Parquet {
            compression: ClickHouseParquetCompression::Zstd,
            row_group_rows: 250_000,
            ..
        }
    ));

    let native: ClickHouseSourceConfig = serde_yaml::from_str(&format!(
        "{base}snapshot_reader: {{ type: native, max_threads: 16, compression: zstd }}\n"
    ))
    .unwrap();
    assert!(matches!(
        native.snapshot_reader,
        ClickHouseSnapshotReader::Native {
            max_threads: 16,
            compression: ClickHouseCompression::Zstd,
        }
    ));
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
    let ca = format!(
        "{}/src/connectors/clickhouse/sink/tests/fixtures/localhost-ca.pem",
        env!("CARGO_MANIFEST_DIR")
    );
    let value = serde_yaml::from_str(&format!("hosts: [localhost]\nport: 9440\ntrusted_plaintext: false\ntls_ca_file: {ca}\nusername: default\ntables: [{{database: default, name: events}}]\n")).unwrap();
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
            primary_key: Vec::new(),
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

#[test]
fn converts_parquet_milliseconds_to_discovered_seconds_only_when_exact() -> anyhow::Result<()> {
    let expected_type = DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC")));
    let table = connector::DiscoveredTable {
        config: config::TableConfig {
            database: "db1".into(),
            name: "events".into(),
            primary_key: Vec::new(),
        },
        schema: DatasetSchema::new(vec![SchemaColumn::new(
            "event_time".into(),
            expected_type.clone(),
            true,
        )]),
        physical_system_columns: transferia_core::SystemColumns::default(),
    };
    let input = RecordBatch::try_from_iter([(
        "event_time",
        Arc::new(
            TimestampMillisecondArray::from(vec![Some(1_000), Some(-2_000), None])
                .with_timezone("UTC"),
        ) as ArrayRef,
    )])?;

    let normalized = reader::normalize_snapshot_schema(&input, &table)?;
    assert_eq!(normalized.schema().field(0).data_type(), &expected_type);
    assert_eq!(
        normalized
            .column(0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .expect("normalized second timestamp")
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(-2), None]
    );

    let inexact = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            true,
        )])),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1_500]).with_timezone("UTC")) as ArrayRef,
        ],
    )?;
    let error = reader::normalize_snapshot_schema(&inexact, &table)
        .expect_err("sub-second value must not be truncated");
    assert!(error
        .to_string()
        .contains("cannot be represented losslessly"));
    Ok(())
}

#[test]
fn timestamp_normalization_rejects_schema_drift_before_relabeling() -> anyhow::Result<()> {
    let table = connector::DiscoveredTable {
        config: config::TableConfig {
            database: "db1".into(),
            name: "events".into(),
            primary_key: Vec::new(),
        },
        schema: DatasetSchema::new(vec![
            SchemaColumn::new("first".into(), DataType::Int64, false),
            SchemaColumn::new("second".into(), DataType::Int64, false),
        ]),
        physical_system_columns: transferia_core::SystemColumns::default(),
    };

    let reordered = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("second", DataType::Int64, false),
            Field::new("first", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ],
    )?;
    assert!(reader::normalize_snapshot_schema(&reordered, &table).is_err());

    let nullable = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("first", DataType::Int64, true),
            Field::new("second", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ],
    )?;
    assert!(reader::normalize_snapshot_schema(&nullable, &table).is_err());

    let wrong_type = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("first", DataType::UInt64, false),
            Field::new("second", DataType::Int64, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ],
    )?;
    assert!(reader::normalize_snapshot_schema(&wrong_type, &table).is_err());
    Ok(())
}

#[test]
fn snapshot_normalization_preserves_discovered_column_metadata() -> anyhow::Result<()> {
    let discovered = SchemaColumn::new("id".into(), DataType::Int64, false).with_constraints(
        true,
        true,
        Some(32),
    );
    let table = connector::DiscoveredTable {
        config: config::TableConfig {
            database: "db1".into(),
            name: "events".into(),
            primary_key: vec!["id".into()],
        },
        schema: DatasetSchema::new(vec![discovered.clone()]),
        physical_system_columns: transferia_core::SystemColumns::default(),
    };
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;

    let normalized = reader::normalize_snapshot_schema(&input, &table)?;
    assert_eq!(
        normalized.schema().field(0).metadata(),
        &discovered.arrow_metadata()
    );
    Ok(())
}
