use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    DurationMicrosecondArray, FixedSizeBinaryBuilder, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringBuilder, TimestampMicrosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use ydb_grpc::ydb_proto::r#type::PrimitiveTypeId;
use ydb_grpc::ydb_proto::{r#type, result_set, value, ResultSet, Type, Value};
use ydb_grpc::ydb_proto::table::ColumnMeta;

use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};

pub(super) const YDB_YSON_EXTENSION: &str = "transferia.ydb.yson";
pub(super) const YDB_TZ_DATE_EXTENSION: &str = "transferia.ydb.tz_date";
pub(super) const YDB_TZ_DATETIME_EXTENSION: &str = "transferia.ydb.tz_datetime";
pub(super) const YDB_TZ_TIMESTAMP_EXTENSION: &str = "transferia.ydb.tz_timestamp";
pub(super) const YDB_DYNUMBER_EXTENSION: &str = "transferia.ydb.dynumber";
pub(super) const ARROW_UUID_EXTENSION: &str = "arrow.uuid";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ColumnKind {
    Bool,
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Date32,
    TimestampSecond,
    TimestampMicrosecond,
    DurationMicrosecond,
    Binary(Option<&'static str>),
    Utf8(Option<&'static str>),
    Decimal { precision: u8, scale: i8 },
    Uuid,
}

impl ColumnKind {
    pub fn arrow_type(&self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Int8 => DataType::Int8,
            Self::UInt8 => DataType::UInt8,
            Self::Int16 => DataType::Int16,
            Self::UInt16 => DataType::UInt16,
            Self::Int32 => DataType::Int32,
            Self::UInt32 => DataType::UInt32,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Date32 => DataType::Date32,
            Self::TimestampSecond => DataType::Timestamp(TimeUnit::Second, None),
            Self::TimestampMicrosecond => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::DurationMicrosecond => DataType::Duration(TimeUnit::Microsecond),
            Self::Binary(_) => DataType::Binary,
            Self::Utf8(_) => DataType::Utf8,
            Self::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
            Self::Uuid => DataType::FixedSizeBinary(16),
        }
    }

    pub const fn extension(&self) -> Option<&'static str> {
        match self {
            Self::Binary(extension) | Self::Utf8(extension) => *extension,
            Self::Uuid => Some(ARROW_UUID_EXTENSION),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ColumnPlan {
    pub name: String,
    pub kind: ColumnKind,
    pub nullable: bool,
    pub primary_key: bool,
}

pub(super) fn column_plans(
    columns: Vec<ColumnMeta>,
    primary_key: &[String],
) -> anyhow::Result<Vec<ColumnPlan>> {
    let primary_key = primary_key.iter().map(String::as_str).collect::<HashSet<_>>();
    columns
        .into_iter()
        .map(|column| {
            let declared = column.r#type.ok_or_else(|| {
                anyhow::anyhow!("YDB column '{}' has no declared type", column.name)
            })?;
            let (kind, optional) = column_kind(&declared)?;
            anyhow::ensure!(
                !(optional && column.not_null == Some(true)),
                "YDB column '{}' declares both Optional and NOT NULL",
                column.name
            );
            // Older YDB servers encode nullability exclusively in Type::Optional
            // and leave ColumnMeta.not_null unset. Treating an absent flag as
            // nullable would incorrectly reject every primary key discovered from
            // those servers.
            let nullable = optional || column.not_null == Some(false);
            let is_key = primary_key.contains(column.name.as_str());
            anyhow::ensure!(
                !is_key || !nullable,
                "YDB primary-key column '{}' is nullable",
                column.name
            );
            Ok(ColumnPlan {
                name: column.name,
                kind,
                nullable,
                primary_key: is_key,
            })
        })
        .collect()
}

pub(super) fn dataset_schema(columns: &[ColumnPlan]) -> DatasetSchema {
    DatasetSchema::new(
        columns
            .iter()
            .map(|column| {
                let mut schema = SchemaColumn::new(
                    column.name.clone(),
                    column.kind.arrow_type(),
                    column.nullable,
                )
                .with_constraints(column.primary_key, false, None);
                if let Some(extension) = column.kind.extension() {
                    schema = schema.with_arrow_extension(extension);
                }
                schema
            })
            .collect(),
    )
}

fn column_kind(value: &Type) -> anyhow::Result<(ColumnKind, bool)> {
    match value.r#type.as_ref() {
        Some(r#type::Type::OptionalType(optional)) => {
            let item = optional
                .item
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("YDB Optional type has no item"))?;
            let (kind, nested_optional) = column_kind(item)?;
            anyhow::ensure!(!nested_optional, "nested YDB Optional columns are not supported losslessly");
            Ok((kind, true))
        }
        Some(r#type::Type::TypeId(type_id)) => {
            let primitive = PrimitiveTypeId::try_from(*type_id)
                .map_err(|_| anyhow::anyhow!("unknown YDB primitive type id {type_id}"))?;
            Ok((primitive_kind(primitive)?, false))
        }
        Some(r#type::Type::DecimalType(decimal)) => {
            let precision = u8::try_from(decimal.precision)?;
            let scale = i8::try_from(decimal.scale)?;
            anyhow::ensure!(precision <= 38, "YDB Decimal({precision},{scale}) exceeds Arrow Decimal128 precision 38");
            Ok((ColumnKind::Decimal { precision, scale }, false))
        }
        Some(other) => anyhow::bail!("unsupported YDB column type {other:?}"),
        None => anyhow::bail!("YDB column type is empty"),
    }
}

fn same_declared_type(actual: &Type, expected: &ColumnKind) -> anyhow::Result<bool> {
    let (actual, _optional) = column_kind(actual)?;
    Ok(&actual == expected)
}

fn primitive_kind(value: PrimitiveTypeId) -> anyhow::Result<ColumnKind> {
    Ok(match value {
        PrimitiveTypeId::Bool => ColumnKind::Bool,
        PrimitiveTypeId::Int8 => ColumnKind::Int8,
        PrimitiveTypeId::Uint8 => ColumnKind::UInt8,
        PrimitiveTypeId::Int16 => ColumnKind::Int16,
        PrimitiveTypeId::Uint16 => ColumnKind::UInt16,
        PrimitiveTypeId::Int32 => ColumnKind::Int32,
        PrimitiveTypeId::Uint32 => ColumnKind::UInt32,
        PrimitiveTypeId::Int64 => ColumnKind::Int64,
        PrimitiveTypeId::Uint64 => ColumnKind::UInt64,
        PrimitiveTypeId::Float => ColumnKind::Float32,
        PrimitiveTypeId::Double => ColumnKind::Float64,
        PrimitiveTypeId::Date | PrimitiveTypeId::Date32 => ColumnKind::Date32,
        PrimitiveTypeId::Datetime | PrimitiveTypeId::Datetime64 => ColumnKind::TimestampSecond,
        PrimitiveTypeId::Timestamp | PrimitiveTypeId::Timestamp64 => {
            ColumnKind::TimestampMicrosecond
        }
        PrimitiveTypeId::Interval | PrimitiveTypeId::Interval64 => {
            ColumnKind::DurationMicrosecond
        }
        PrimitiveTypeId::String => ColumnKind::Binary(None),
        PrimitiveTypeId::Utf8 => ColumnKind::Utf8(None),
        PrimitiveTypeId::Yson => ColumnKind::Binary(Some(YDB_YSON_EXTENSION)),
        PrimitiveTypeId::Json | PrimitiveTypeId::JsonDocument => {
            ColumnKind::Utf8(Some(ARROW_JSON_EXTENSION_NAME))
        }
        PrimitiveTypeId::Uuid => ColumnKind::Uuid,
        PrimitiveTypeId::TzDate => ColumnKind::Utf8(Some(YDB_TZ_DATE_EXTENSION)),
        PrimitiveTypeId::TzDatetime => ColumnKind::Utf8(Some(YDB_TZ_DATETIME_EXTENSION)),
        PrimitiveTypeId::TzTimestamp => ColumnKind::Utf8(Some(YDB_TZ_TIMESTAMP_EXTENSION)),
        PrimitiveTypeId::Dynumber => ColumnKind::Utf8(Some(YDB_DYNUMBER_EXTENSION)),
        PrimitiveTypeId::Unspecified => anyhow::bail!("YDB primitive type is unspecified"),
    })
}

pub(super) fn result_set_to_batch(
    result: ResultSet,
    columns: &[ColumnPlan],
) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(!result.truncated, "YDB returned a truncated table result");
    let format = result_set::Format::try_from(result.format)
        .map_err(|_| anyhow::anyhow!("YDB returned unknown result format {}", result.format))?;
    anyhow::ensure!(
        matches!(format, result_set::Format::Unspecified | result_set::Format::Value)
            && result.data.is_empty()
            && result.arrow_format_meta.is_none(),
        "YDB returned unsupported result format {format:?}"
    );
    if !result.columns.is_empty() {
        anyhow::ensure!(
            result.columns.len() == columns.len()
                && result
                    .columns
                    .iter()
                    .zip(columns)
                    .all(|(actual, expected)| {
                        actual.name == expected.name
                            && actual
                                .r#type
                                .as_ref()
                                .is_some_and(|actual| {
                                    same_declared_type(actual, &expected.kind).unwrap_or(false)
                                })
                    }),
            "YDB result schema changed after discovery"
        );
    }
    for row in &result.rows {
        anyhow::ensure!(
            row.items.len() == columns.len(),
            "YDB row has {} values, schema declares {} columns",
            row.items.len(),
            columns.len()
        );
    }
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let mut schema = SchemaColumn::new(
            column.name.clone(),
            column.kind.arrow_type(),
            column.nullable,
        )
        .with_constraints(column.primary_key, false, None);
        if let Some(extension) = column.kind.extension() {
            schema = schema.with_arrow_extension(extension);
        }
        fields.push(
            Field::new(&column.name, column.kind.arrow_type(), column.nullable)
                .with_metadata(schema.arrow_metadata()),
        );
        arrays.push(column_array(&result.rows, index, column)?);
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

fn cell<'a>(
    row: &'a Value,
    index: usize,
    column: &ColumnPlan,
) -> anyhow::Result<Option<&'a value::Value>> {
    let item = row.items.get(index).ok_or_else(|| {
        anyhow::anyhow!(
            "YDB row has {} values, missing column '{}' at index {index}",
            row.items.len(),
            column.name
        )
    })?;
    let value = item.value.as_ref();
    match value {
        Some(value::Value::NullFlagValue(_)) | None if column.nullable => Ok(None),
        Some(value::Value::NullFlagValue(_)) | None => {
            anyhow::bail!("YDB returned NULL for non-null column '{}'", column.name)
        }
        Some(value::Value::NestedValue(_)) => {
            anyhow::bail!("YDB returned nested Optional for column '{}'", column.name)
        }
        Some(value) => Ok(Some(value)),
    }
}

fn column_array(rows: &[Value], index: usize, column: &ColumnPlan) -> anyhow::Result<ArrayRef> {
    macro_rules! primitive_array {
        ($variant:ident, $native:ty, $array:ty) => {{
            let values = rows
                .iter()
                .map(|row| match cell(row, index, column)? {
                    None => Ok(None),
                    Some(value::Value::$variant(value)) => Ok(Some(<$native>::try_from(*value)?)),
                    Some(other) => anyhow::bail!("YDB column '{}' returned {other:?}", column.name),
                })
                .collect::<anyhow::Result<Vec<Option<$native>>>>()?;
            Arc::new(<$array>::from(values)) as ArrayRef
        }};
    }
    Ok(match &column.kind {
        ColumnKind::Bool => primitive_array!(BoolValue, bool, BooleanArray),
        ColumnKind::Int8 => primitive_array!(Int32Value, i8, Int8Array),
        ColumnKind::UInt8 => primitive_array!(Uint32Value, u8, UInt8Array),
        ColumnKind::Int16 => primitive_array!(Int32Value, i16, Int16Array),
        ColumnKind::UInt16 => primitive_array!(Uint32Value, u16, UInt16Array),
        ColumnKind::Int32 | ColumnKind::Date32 => {
            let values = rows
                .iter()
                .map(|row| match cell(row, index, column)? {
                    None => Ok(None),
                    Some(value::Value::Int32Value(value)) => Ok(Some(*value)),
                    Some(value::Value::Uint32Value(value)) => Ok(Some(i32::try_from(*value)?)),
                    Some(other) => anyhow::bail!("YDB column '{}' returned {other:?}", column.name),
                })
                .collect::<anyhow::Result<Vec<Option<i32>>>>()?;
            if column.kind == ColumnKind::Date32 {
                Arc::new(Date32Array::from(values))
            } else {
                Arc::new(Int32Array::from(values))
            }
        }
        ColumnKind::UInt32 => primitive_array!(Uint32Value, u32, UInt32Array),
        ColumnKind::Int64
        | ColumnKind::TimestampSecond
        | ColumnKind::TimestampMicrosecond
        | ColumnKind::DurationMicrosecond => {
            let values = rows
                .iter()
                .map(|row| match cell(row, index, column)? {
                    None => Ok(None),
                    Some(value::Value::Uint32Value(value)) => Ok(Some(i64::from(*value))),
                    Some(value::Value::Int64Value(value)) => Ok(Some(*value)),
                    Some(value::Value::Uint64Value(value)) => Ok(Some(i64::try_from(*value)?)),
                    Some(other) => anyhow::bail!("YDB column '{}' returned {other:?}", column.name),
                })
                .collect::<anyhow::Result<Vec<Option<i64>>>>()?;
            match &column.kind {
                ColumnKind::TimestampSecond => Arc::new(TimestampSecondArray::from(values)),
                ColumnKind::TimestampMicrosecond => {
                    Arc::new(TimestampMicrosecondArray::from(values))
                }
                ColumnKind::DurationMicrosecond => Arc::new(DurationMicrosecondArray::from(values)),
                _ => Arc::new(Int64Array::from(values)),
            }
        }
        ColumnKind::UInt64 => primitive_array!(Uint64Value, u64, UInt64Array),
        ColumnKind::Float32 => primitive_array!(FloatValue, f32, Float32Array),
        ColumnKind::Float64 => primitive_array!(DoubleValue, f64, Float64Array),
        ColumnKind::Binary(_) => {
            let mut builder = BinaryBuilder::new();
            for row in rows {
                match cell(row, index, column)? {
                    None => builder.append_null(),
                    Some(value::Value::BytesValue(value)) => builder.append_value(value),
                    Some(other) => anyhow::bail!("YDB column '{}' returned {other:?}", column.name),
                }
            }
            Arc::new(builder.finish())
        }
        ColumnKind::Utf8(_) => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match cell(row, index, column)? {
                    None => builder.append_null(),
                    Some(value::Value::TextValue(value)) => builder.append_value(value),
                    Some(other) => anyhow::bail!("YDB column '{}' returned {other:?}", column.name),
                }
            }
            Arc::new(builder.finish())
        }
        ColumnKind::Decimal { precision, scale } => {
            let values = rows
                .iter()
                .map(|row| match cell(row, index, column)? {
                    None => Ok(None),
                    Some(value::Value::Low128(low)) => {
                        let high = row.items[index].high_128;
                        Ok(Some((((u128::from(high)) << 64) | u128::from(*low)) as i128))
                    }
                    Some(other) => anyhow::bail!("YDB column '{}' returned {other:?}", column.name),
                })
                .collect::<anyhow::Result<Vec<Option<i128>>>>()?;
            Arc::new(
                Decimal128Array::from(values).with_precision_and_scale(*precision, *scale)?,
            )
        }
        ColumnKind::Uuid => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
            for row in rows {
                match cell(row, index, column)? {
                    None => builder.append_null(),
                    Some(value::Value::Low128(low)) => {
                        let mut little_endian = [0_u8; 16];
                        little_endian[..8].copy_from_slice(&low.to_le_bytes());
                        little_endian[8..]
                            .copy_from_slice(&row.items[index].high_128.to_le_bytes());
                        // YDB transmits UUID numeric fields in little-endian form;
                        // Arrow's uuid extension stores the canonical RFC byte order.
                        let canonical = uuid::Uuid::from_bytes_le(little_endian).into_bytes();
                        builder.append_value(&canonical)?;
                    }
                    Some(other) => anyhow::bail!("YDB column '{}' returned {other:?}", column.name),
                }
            }
            Arc::new(builder.finish())
        }
    })
}
