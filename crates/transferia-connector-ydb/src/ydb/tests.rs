use super::config::{YdbAuth, YdbConnectionConfig, YdbSinkConfig, YdbTableConfig};
use super::sink::{create_table_query, encode_arrow_batch};
use super::types::{column_plans, dataset_schema, result_set_to_batch, ColumnKind};
use arrow::array::{Array as _, Decimal128Array, FixedSizeBinaryArray, StringArray, UInt64Array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use ydb_grpc::ydb_proto::r#type::{PrimitiveTypeId, Type as TypeKind};
use ydb_grpc::ydb_proto::table::ColumnMeta;
use ydb_grpc::ydb_proto::{
    result_set, value, Column, DecimalType, ListType, OptionalType, ResultSet, Type, Value,
};

#[test]
fn plaintext_endpoint_requires_explicit_trust() {
    let config = YdbConnectionConfig {
        endpoint: "grpc://localhost:2136".to_owned(),
        database: "/local".to_owned(),
        trusted_plaintext: false,
        auth: YdbAuth::Anonymous,
        request_timeout_ms: 30_000,
    };
    assert!(config.validate().is_err());
}

#[test]
fn discovery_preserves_optional_decimal_and_uuid_types() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![
            column("key", primitive(PrimitiveTypeId::Uint64), None),
            column(
                "amount",
                optional(Type {
                    r#type: Some(TypeKind::DecimalType(DecimalType {
                        precision: 22,
                        scale: 7,
                    })),
                }),
                None,
            ),
            column("event_id", primitive(PrimitiveTypeId::Uuid), Some(true)),
        ],
        &["key".to_owned()],
    )?;
    assert_eq!(columns[0].kind, ColumnKind::UInt64);
    assert!(!columns[0].nullable);
    assert!(columns[0].primary_key);
    assert_eq!(
        columns[1].kind,
        ColumnKind::Decimal {
            precision: 22,
            scale: 7
        }
    );
    assert!(columns[1].nullable);
    let schema = dataset_schema(&columns);
    assert_eq!(schema.columns[1].data_type, DataType::Decimal128(22, 7));
    assert_eq!(schema.columns[2].arrow_extension_name, Some("arrow.uuid"));
    Ok(())
}

#[test]
fn source_decodes_values_nulls_decimal_and_uuid_losslessly() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![
            column("name", primitive(PrimitiveTypeId::Utf8), Some(true)),
            column(
                "amount",
                optional(Type {
                    r#type: Some(TypeKind::DecimalType(DecimalType {
                        precision: 22,
                        scale: 7,
                    })),
                }),
                None,
            ),
            column("event_id", primitive(PrimitiveTypeId::Uuid), Some(true)),
        ],
        &[],
    )?;
    let uuid = uuid::Uuid::parse_str("12345678-1234-4abc-89ab-1234567890ab")?;
    let little_endian = uuid.to_bytes_le();
    let low = u64::from_le_bytes(little_endian[..8].try_into()?);
    let high = u64::from_le_bytes(little_endian[8..].try_into()?);
    let decimal = -12_345_678_901_i128;
    let decimal_bits = decimal as u128;
    let result = ResultSet {
        columns: vec![
            result_column("name", primitive(PrimitiveTypeId::Utf8)),
            result_column(
                "amount",
                optional(Type {
                    r#type: Some(TypeKind::DecimalType(DecimalType {
                        precision: 22,
                        scale: 7,
                    })),
                }),
            ),
            result_column("event_id", primitive(PrimitiveTypeId::Uuid)),
        ],
        rows: vec![
            Value {
                items: vec![
                    scalar(value::Value::TextValue("alpha".to_owned())),
                    high_low(decimal_bits as u64, (decimal_bits >> 64) as u64),
                    high_low(low, high),
                ],
                ..Value::default()
            },
            Value {
                items: vec![
                    scalar(value::Value::TextValue("beta".to_owned())),
                    scalar(value::Value::NullFlagValue(0)),
                    high_low(low, high),
                ],
                ..Value::default()
            },
        ],
        truncated: false,
        format: result_set::Format::Value as i32,
        arrow_format_meta: None,
        data: Vec::new(),
    };
    let batch = result_set_to_batch(result, &columns)?;
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "alpha"
    );
    let amounts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amounts.value(0), decimal);
    assert!(amounts.is_null(1));
    let ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(ids.value(0), uuid.as_bytes());
    Ok(())
}

#[test]
fn unsupported_complex_type_fails_before_reading() {
    let complex = Type {
        r#type: Some(TypeKind::ListType(Box::new(ListType {
            item: Some(Box::new(primitive(PrimitiveTypeId::Utf8))),
        }))),
    };
    let error = column_plans(vec![column("items", complex, None)], &[]).unwrap_err();
    assert!(error.to_string().contains("unsupported YDB column type"));
}

#[test]
fn schema_drift_is_rejected_before_emitting_rows() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![column("value", primitive(PrimitiveTypeId::Utf8), None)],
        &[],
    )?;
    let result = ResultSet {
        columns: vec![result_column("value", primitive(PrimitiveTypeId::String))],
        rows: Vec::new(),
        truncated: false,
        format: result_set::Format::Value as i32,
        arrow_format_meta: None,
        data: Vec::new(),
    };
    assert!(result_set_to_batch(result, &columns).is_err());
    Ok(())
}

#[test]
fn streamed_result_chunk_may_be_marked_truncated_without_losing_rows() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![column("value", primitive(PrimitiveTypeId::Utf8), None)],
        &[],
    )?;
    let result = ResultSet {
        columns: vec![result_column("value", primitive(PrimitiveTypeId::Utf8))],
        rows: vec![Value {
            items: vec![scalar(value::Value::TextValue("kept".to_owned()))],
            ..Value::default()
        }],
        truncated: true,
        format: result_set::Format::Value as i32,
        arrow_format_meta: None,
        data: Vec::new(),
    };
    let batch = result_set_to_batch(result, &columns)?;
    assert_eq!(batch.num_rows(), 1);
    Ok(())
}

#[test]
fn sink_requires_exact_table_mappings_and_primary_key() -> anyhow::Result<()> {
    let config = YdbSinkConfig {
        connection: YdbConnectionConfig {
            endpoint: "grpc://localhost:2136".to_owned(),
            database: "/local".to_owned(),
            trusted_plaintext: true,
            auth: YdbAuth::Anonymous,
            request_timeout_ms: 30_000,
        },
        tables: vec![YdbTableConfig {
            name: "events".to_owned(),
            path: "/local/events".to_owned(),
        }],
        create_tables: true,
        retry_max_ms: 30_000,
    };
    config.validate()?;
    assert_eq!(config.table_path("events")?, "/local/events");
    assert!(config.table_path("missing").is_err());
    Ok(())
}

#[test]
fn sink_arrow_payload_round_trips_without_semantic_metadata() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["one", "two"])),
        ],
    )?;
    let (schema, data) = encode_arrow_batch(&batch)?;
    let mut decoder = StreamDecoder::new();
    let mut schema = Buffer::from_vec(schema);
    assert!(decoder.decode(&mut schema)?.is_none());
    let mut data = Buffer::from_vec(data);
    let decoded = decoder.decode(&mut data)?.expect("record batch");
    assert_eq!(decoded, batch);
    Ok(())
}

#[test]
fn sink_create_table_query_preserves_composite_primary_key() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("partition".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("offset".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, true),
    ]);
    let query = create_table_query("/local/events", &schema)?;
    assert!(query.contains("CREATE TABLE IF NOT EXISTS `/local/events`"));
    assert!(query.contains("PRIMARY KEY (`partition`, `offset`)"));
    assert!(query.contains("`payload` Utf8"));
    Ok(())
}

fn primitive(id: PrimitiveTypeId) -> Type {
    Type {
        r#type: Some(TypeKind::TypeId(id as i32)),
    }
}

fn optional(item: Type) -> Type {
    Type {
        r#type: Some(TypeKind::OptionalType(Box::new(OptionalType {
            item: Some(Box::new(item)),
        }))),
    }
}

fn column(name: &str, r#type: Type, not_null: Option<bool>) -> ColumnMeta {
    ColumnMeta {
        name: name.to_owned(),
        r#type: Some(r#type),
        family: String::new(),
        not_null,
        default_value: None,
    }
}

fn result_column(name: &str, r#type: Type) -> Column {
    Column {
        name: name.to_owned(),
        r#type: Some(r#type),
    }
}

fn scalar(value: value::Value) -> Value {
    Value {
        value: Some(value),
        ..Value::default()
    }
}

fn high_low(low: u64, high: u64) -> Value {
    Value {
        high_128: high,
        value: Some(value::Value::Low128(low)),
        ..Value::default()
    }
}
