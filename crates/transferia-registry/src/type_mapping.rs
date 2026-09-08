//! Offline examples evaluated with the very same resolvers used by discovery/DDL.
//! Inputs are examples, never an independently maintained conversion table.
use arrow::datatypes::{DataType, Field, TimeUnit};
use schemars::JsonSchema;
use serde::Serialize;
use std::sync::Arc;
use transferia_core::data::schema::SchemaColumn;

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypeMapping {
    pub context: String,
    pub rows: Vec<TypeMappingRow>,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypeMappingRow {
    pub input: String,
    pub output: Option<String>,
    pub error: Option<String>,
}

impl TypeMappingRow {
    pub fn evaluate(input: impl Into<String>, result: anyhow::Result<String>) -> Self {
        let (output, error) = match result {
            Ok(output) => (Some(output), None),
            Err(error) => (None, Some(format!("{error:#}"))),
        };
        Self { input: input.into(), output, error }
    }
}

/// Representative parameter values are deliberately explicit in each row.
/// Rejections remain visible; absence from this list does not mean unsupported.
pub fn destination_mapping(
    context: &str,
    resolve: impl Fn(&SchemaColumn) -> anyhow::Result<String>,
) -> TypeMapping {
    let mut types = vec![
        DataType::Null, DataType::Boolean,
        DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64,
        DataType::UInt8, DataType::UInt16, DataType::UInt32, DataType::UInt64,
        DataType::Float16, DataType::Float32, DataType::Float64,
        DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View,
        DataType::Binary, DataType::LargeBinary, DataType::BinaryView,
        DataType::FixedSizeBinary(16), DataType::Date32, DataType::Date64,
        DataType::Decimal128(18, 4), DataType::Decimal128(38, 9), DataType::Decimal256(76, 18),
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        DataType::Struct(vec![Field::new("value", DataType::Utf8, true)].into()),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ];
    for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        types.push(DataType::Timestamp(unit.clone(), None));
        types.push(DataType::Timestamp(unit.clone(), Some("UTC".into())));
        types.push(DataType::Duration(unit));
    }
    TypeMapping {
        context: context.to_owned(),
        rows: types.into_iter().map(|data_type| {
            let input = format!("{data_type:?}");
            TypeMappingRow::evaluate(input, resolve(&SchemaColumn::new("value".to_owned(), data_type, false)))
        }).collect(),
    }
}
