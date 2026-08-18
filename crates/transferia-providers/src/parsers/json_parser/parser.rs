use alloc::sync::Arc;
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, LargeStringBuilder,
    StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde::Deserializer as _;
use serde_json::Value;
use std::collections::HashSet;
use std::str::FromStr;

use bigdecimal::BigDecimal;

use super::dlq::{append_base64, dlq_record, subslice_range, DlqReason, DlqRecord};
use super::extraction::{
    parse_root_fields_typed, ColumnIndex, DuplicateMappedRootVisitor, RootFieldInfo,
};
use super::framing::frame_json_arrays;
use super::system_columns::{
    append_system_columns, make_exact_system_builder, make_system_builder, SystemColumnValues,
};
use super::typed::{str_val, TypedScratch};
use super::workspace::ParserWorkspace;
use crate::parsers::json_parser::config::{
    ConversionErrorPolicy, EpochUnit, JsonDataType, JsonFramingMode, JsonParserConfig,
    TimeConversion, UnknownFieldPolicy,
};
use crate::parsers::{ParserFactory, ParserSession, SystemColumnsConfig};
use transferia_core::data::message::{Message, MessageMeta};
use transferia_core::data::schema::SchemaColumn;
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::{dlq_name, TableData};

// ---------------------------------------------------------------------------
// Compiled JSONPath
// ---------------------------------------------------------------------------

enum CompiledPath {
    RootField(String),
    Complex(jsonpath_lib::Compiled),
    Rest,
}

fn compile_path(raw: &str) -> anyhow::Result<CompiledPath> {
    let compiled = jsonpath_lib::Compiled::compile(raw)
        .map_err(|error| anyhow::anyhow!("invalid JSONPath '{raw}': {error}"))?;
    if let Some(field) = raw.strip_prefix("$.") {
        if !field.is_empty()
            && field
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Ok(CompiledPath::RootField(field.to_string()));
        }
    }
    Ok(CompiledPath::Complex(compiled))
}

fn mapped_top_level_field(raw: &str) -> Option<&str> {
    let path = raw.strip_prefix("$.")?;
    let end = path.find(['.', '[']).unwrap_or(path.len());
    (end > 0).then(|| &path[..end])
}

// ---------------------------------------------------------------------------
// Parse mode
// ---------------------------------------------------------------------------

enum ParseMode {
    AllRootField(RootFieldInfo),
    Mixed,
}

// ---------------------------------------------------------------------------
// Arrow builder enum
// ---------------------------------------------------------------------------

pub(super) enum AnyBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Json(StringBuilder),
    Decimal128(Decimal128Builder, u8, i8),
    Date32(Date32Builder),
    Date64(Date64Builder),
    TimestampSecond(TimestampSecondBuilder),
    TimestampMillisecond(TimestampMillisecondBuilder),
    TimestampMicrosecond(TimestampMicrosecondBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnKind {
    Utf8,
    LargeUtf8,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    Json,
    Decimal128(u8, i8),
    Date32,
    Date64,
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
}

impl ColumnKind {
    const fn from_data_type(dt: &DataType) -> Option<Self> {
        Some(match dt {
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Boolean => Self::Boolean,
            DataType::Decimal128(precision, scale) => Self::Decimal128(*precision, *scale),
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Timestamp(TimeUnit::Second, _) => Self::TimestampSecond,
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::TimestampMillisecond,
            DataType::Timestamp(TimeUnit::Microsecond, _) => Self::TimestampMicrosecond,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Self::TimestampNanosecond,
            DataType::Null
            | DataType::Float16
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8View
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::FixedSizeList(..)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_)
            | DataType::Union(..)
            | DataType::Dictionary(..)
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal256(..)
            | DataType::Map(..)
            | DataType::RunEndEncoded(..) => return None,
        })
    }

    pub(super) const fn fixed_width_bytes(self) -> Option<usize> {
        match self {
            Self::Utf8 | Self::LargeUtf8 | Self::Json => None,
            Self::Int8 | Self::UInt8 => Some(1),
            Self::Int16 | Self::UInt16 => Some(2),
            Self::Int32 | Self::UInt32 | Self::Float32 | Self::Date32 => Some(4),
            Self::Int64
            | Self::UInt64
            | Self::Float64
            | Self::Date64
            | Self::TimestampSecond
            | Self::TimestampMillisecond
            | Self::TimestampMicrosecond
            | Self::TimestampNanosecond => Some(8),
            Self::Boolean => Some(0),
            Self::Decimal128(..) => Some(16),
        }
    }
}

#[inline]
fn make_builder(
    kind: ColumnKind,
    data_type: &DataType,
    n: usize,
    string_bytes: usize,
) -> AnyBuilder {
    // Capacity is only a throughput hint. Bound eager allocations so one
    // source delivery cannot reserve its full worst-case output before any
    // value has been validated; builders grow on demand under the active
    // transform reservation.
    const MAX_INITIAL_ROWS: usize = 65_536;
    const MAX_INITIAL_STRING_BYTES: usize = 1024 * 1024;
    let n = n.min(MAX_INITIAL_ROWS);
    let string_bytes = string_bytes.min(MAX_INITIAL_STRING_BYTES);
    match kind {
        ColumnKind::Utf8 => AnyBuilder::Utf8(StringBuilder::with_capacity(n, string_bytes)),
        ColumnKind::LargeUtf8 => {
            AnyBuilder::LargeUtf8(LargeStringBuilder::with_capacity(n, string_bytes))
        }
        ColumnKind::Int64 => AnyBuilder::Int64(Int64Builder::with_capacity(n)),
        ColumnKind::Int32 => AnyBuilder::Int32(Int32Builder::with_capacity(n)),
        ColumnKind::Int16 => AnyBuilder::Int16(Int16Builder::with_capacity(n)),
        ColumnKind::Int8 => AnyBuilder::Int8(Int8Builder::with_capacity(n)),
        ColumnKind::UInt64 => AnyBuilder::UInt64(UInt64Builder::with_capacity(n)),
        ColumnKind::UInt32 => AnyBuilder::UInt32(UInt32Builder::with_capacity(n)),
        ColumnKind::UInt16 => AnyBuilder::UInt16(UInt16Builder::with_capacity(n)),
        ColumnKind::UInt8 => AnyBuilder::UInt8(UInt8Builder::with_capacity(n)),
        ColumnKind::Float64 => AnyBuilder::Float64(Float64Builder::with_capacity(n)),
        ColumnKind::Float32 => AnyBuilder::Float32(Float32Builder::with_capacity(n)),
        ColumnKind::Boolean => AnyBuilder::Boolean(BooleanBuilder::with_capacity(n)),
        ColumnKind::Json => AnyBuilder::Json(StringBuilder::with_capacity(n, string_bytes)),
        ColumnKind::Decimal128(precision, scale) => AnyBuilder::Decimal128(
            Decimal128Builder::with_capacity(n)
                .with_data_type(DataType::Decimal128(precision, scale)),
            precision,
            scale,
        ),
        ColumnKind::Date32 => AnyBuilder::Date32(Date32Builder::with_capacity(n)),
        ColumnKind::Date64 => AnyBuilder::Date64(Date64Builder::with_capacity(n)),
        ColumnKind::TimestampMillisecond => AnyBuilder::TimestampMillisecond(
            TimestampMillisecondBuilder::with_capacity(n)
                .with_timezone_opt(timestamp_timezone(data_type)),
        ),
        ColumnKind::TimestampMicrosecond => AnyBuilder::TimestampMicrosecond(
            TimestampMicrosecondBuilder::with_capacity(n)
                .with_timezone_opt(timestamp_timezone(data_type)),
        ),
        ColumnKind::TimestampNanosecond => AnyBuilder::TimestampNanosecond(
            TimestampNanosecondBuilder::with_capacity(n)
                .with_timezone_opt(timestamp_timezone(data_type)),
        ),
        ColumnKind::TimestampSecond => AnyBuilder::TimestampSecond(
            TimestampSecondBuilder::with_capacity(n)
                .with_timezone_opt(timestamp_timezone(data_type)),
        ),
    }
}

fn timestamp_timezone(data_type: &DataType) -> Option<Arc<str>> {
    match data_type {
        DataType::Timestamp(_, timezone) => timezone.clone(),
        _ => None,
    }
}

impl AnyBuilder {
    #[inline]
    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Utf8(b) | Self::Json(b) => Arc::new(b.finish()),
            Self::LargeUtf8(b) => Arc::new(b.finish()),
            Self::Int8(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::UInt8(b) => Arc::new(b.finish()),
            Self::UInt16(b) => Arc::new(b.finish()),
            Self::UInt32(b) => Arc::new(b.finish()),
            Self::UInt64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Decimal128(b, ..) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::Date64(b) => Arc::new(b.finish()),
            Self::TimestampSecond(b) => Arc::new(b.finish()),
            Self::TimestampMillisecond(b) => Arc::new(b.finish()),
            Self::TimestampMicrosecond(b) => Arc::new(b.finish()),
            Self::TimestampNanosecond(b) => Arc::new(b.finish()),
        }
    }
}

// ---------------------------------------------------------------------------
// Value-based append (fallback for Mixed mode)
// ---------------------------------------------------------------------------

#[inline]
fn value_matches_kind(kind: ColumnKind, val: &Value) -> bool {
    if val.is_null() {
        return true;
    }
    match kind {
        ColumnKind::Utf8 | ColumnKind::LargeUtf8 | ColumnKind::Json => val.as_str().is_some(),
        ColumnKind::Int64
        | ColumnKind::Date64
        | ColumnKind::TimestampSecond
        | ColumnKind::TimestampMillisecond
        | ColumnKind::TimestampMicrosecond
        | ColumnKind::TimestampNanosecond => val.as_i64().is_some(),
        ColumnKind::Int32 | ColumnKind::Date32 => val
            .as_i64()
            .is_some_and(|value| i32::try_from(value).is_ok()),
        ColumnKind::Int16 => val
            .as_i64()
            .is_some_and(|value| i16::try_from(value).is_ok()),
        ColumnKind::Int8 => val
            .as_i64()
            .is_some_and(|value| i8::try_from(value).is_ok()),
        ColumnKind::UInt64 => val.as_u64().is_some(),
        ColumnKind::UInt32 => val
            .as_u64()
            .is_some_and(|value| u32::try_from(value).is_ok()),
        ColumnKind::UInt16 => val
            .as_u64()
            .is_some_and(|value| u16::try_from(value).is_ok()),
        ColumnKind::UInt8 => val
            .as_u64()
            .is_some_and(|value| u8::try_from(value).is_ok()),
        ColumnKind::Float64 => val.as_f64().is_some_and(f64::is_finite),
        ColumnKind::Float32 => val.as_f64().is_some_and(|value| {
            value.is_finite() && value >= f64::from(f32::MIN) && value <= f64::from(f32::MAX)
        }),
        ColumnKind::Boolean => val.as_bool().is_some(),
        ColumnKind::Decimal128(precision, scale) => {
            decimal_unscaled(val, precision, scale).is_some()
        }
    }
}

fn json_value_matches(kind: JsonDataType, value: &Value) -> bool {
    value.is_null()
        || match kind {
            JsonDataType::String | JsonDataType::Decimal => value.is_string(),
            JsonDataType::Number => value.as_f64().is_some_and(f64::is_finite),
            JsonDataType::Boolean => value.is_boolean(),
            JsonDataType::Json => true,
        }
}

fn decimal_unscaled(value: &Value, precision: u8, scale: i8) -> Option<i128> {
    let decimal = BigDecimal::from_str(value.as_str()?).ok()?;
    let scaled = decimal.with_scale(i64::from(scale));
    if scaled != decimal {
        return None;
    }
    let (integer, actual_scale) = scaled.into_bigint_and_scale();
    if actual_scale != i64::from(scale) {
        return None;
    }
    let unscaled = i128::try_from(integer).ok()?;
    let digits = unscaled.unsigned_abs().to_string().len();
    (digits <= usize::from(precision)).then_some(unscaled)
}

fn convert_time_value(
    value: &Value,
    conversion: &TimeConversion,
    target: ColumnKind,
) -> Result<Value, RowConversionError> {
    let source_ns = match conversion {
        TimeConversion::Epoch { unit } => {
            let raw = value
                .as_i64()
                .ok_or_else(|| RowConversionError("epoch value is not a signed integer".into()))?;
            raw.checked_mul(match unit {
                EpochUnit::Seconds => 1_000_000_000,
                EpochUnit::Milliseconds => 1_000_000,
                EpochUnit::Microseconds => 1_000,
                EpochUnit::Nanoseconds => 1,
            })
            .ok_or_else(|| RowConversionError("epoch conversion overflow".into()))?
        }
        TimeConversion::String { format } => {
            let raw = value
                .as_str()
                .ok_or_else(|| RowConversionError("time value is not a string".into()))?;
            let description = time::format_description::parse_borrowed::<2>(format)
                .map_err(|error| RowConversionError(error.to_string()))?;
            let parsed = time::OffsetDateTime::parse(raw, &description)
                .or_else(|_| {
                    time::PrimitiveDateTime::parse(raw, &description)
                        .map(time::PrimitiveDateTime::assume_utc)
                })
                .map_err(|error| RowConversionError(error.to_string()))?;
            i64::try_from(parsed.unix_timestamp_nanos())
                .map_err(|_| RowConversionError("parsed timestamp is outside Arrow range".into()))?
        }
    };
    let converted = match target {
        ColumnKind::TimestampSecond => source_ns.div_euclid(1_000_000_000),
        ColumnKind::TimestampMillisecond | ColumnKind::Date64 => source_ns.div_euclid(1_000_000),
        ColumnKind::TimestampMicrosecond => source_ns.div_euclid(1_000),
        ColumnKind::TimestampNanosecond => source_ns,
        ColumnKind::Date32 => source_ns.div_euclid(86_400_000_000_000),
        _ => {
            return Err(RowConversionError(
                "time conversion target is not temporal".into(),
            ))
        }
    };
    Ok(Value::from(converted))
}

/// Append a value that has already passed [`value_matches_kind`].
///
/// Returning `false` keeps the validation and materialization contracts coupled:
/// a future type addition cannot silently fall back to zero, NULL, or a narrowing
/// cast when the two functions drift apart.
#[inline]
fn append_value(builder: &mut AnyBuilder, val: &Value) -> bool {
    if val.is_null() {
        append_null(builder);
        return true;
    }
    match builder {
        AnyBuilder::Utf8(b) => append_if_some(val.as_str(), |value| b.append_value(value)),
        AnyBuilder::LargeUtf8(b) => append_if_some(val.as_str(), |value| b.append_value(value)),
        AnyBuilder::Int64(b) => append_if_some(val.as_i64(), |value| b.append_value(value)),
        AnyBuilder::Int32(b) => append_if_some(
            val.as_i64().and_then(|value| i32::try_from(value).ok()),
            |value| b.append_value(value),
        ),
        AnyBuilder::Int16(b) => append_if_some(
            val.as_i64().and_then(|value| i16::try_from(value).ok()),
            |value| b.append_value(value),
        ),
        AnyBuilder::Int8(b) => append_if_some(
            val.as_i64().and_then(|value| i8::try_from(value).ok()),
            |value| b.append_value(value),
        ),
        AnyBuilder::UInt64(b) => append_if_some(val.as_u64(), |value| b.append_value(value)),
        AnyBuilder::UInt32(b) => append_if_some(
            val.as_u64().and_then(|value| u32::try_from(value).ok()),
            |value| b.append_value(value),
        ),
        AnyBuilder::UInt16(b) => append_if_some(
            val.as_u64().and_then(|value| u16::try_from(value).ok()),
            |value| b.append_value(value),
        ),
        AnyBuilder::UInt8(b) => append_if_some(
            val.as_u64().and_then(|value| u8::try_from(value).ok()),
            |value| b.append_value(value),
        ),
        AnyBuilder::Float64(b) => {
            append_if_some(val.as_f64().filter(|value| value.is_finite()), |value| {
                b.append_value(value);
            })
        }
        AnyBuilder::Float32(b) => append_if_some(
            val.as_f64()
                .filter(|value| {
                    value.is_finite()
                        && *value >= f64::from(f32::MIN)
                        && *value <= f64::from(f32::MAX)
                })
                .map(|value| value as f32),
            |value| b.append_value(value),
        ),
        AnyBuilder::Boolean(b) => append_if_some(val.as_bool(), |value| b.append_value(value)),
        AnyBuilder::Json(b) => append_if_some(val.as_str(), |value| b.append_value(value)),
        AnyBuilder::Decimal128(b, precision, scale) => {
            append_if_some(decimal_unscaled(val, *precision, *scale), |value| {
                b.append_value(value);
            })
        }
        AnyBuilder::TimestampMillisecond(b) => {
            append_if_some(val.as_i64(), |value| b.append_value(value))
        }
        AnyBuilder::TimestampMicrosecond(b) => {
            append_if_some(val.as_i64(), |value| b.append_value(value))
        }
        AnyBuilder::TimestampNanosecond(b) => {
            append_if_some(val.as_i64(), |value| b.append_value(value))
        }
        AnyBuilder::TimestampSecond(b) => {
            append_if_some(val.as_i64(), |value| b.append_value(value))
        }
        AnyBuilder::Date32(b) => append_if_some(
            val.as_i64().and_then(|value| i32::try_from(value).ok()),
            |value| b.append_value(value),
        ),
        AnyBuilder::Date64(b) => append_if_some(val.as_i64(), |value| b.append_value(value)),
    }
}

#[inline]
fn append_if_some<T>(value: Option<T>, append: impl FnOnce(T)) -> bool {
    let Some(value) = value else {
        return false;
    };
    append(value);
    true
}

/// Appends a typed scratch value into the corresponding Arrow builder.
/// Strings are reconstructed from `json_buf` byte ranges — zero-copy.
/// Appends a NULL to any builder variant.
#[inline]
fn append_null(b: &mut AnyBuilder) {
    match b {
        AnyBuilder::Utf8(x) | AnyBuilder::Json(x) => x.append_null(),
        AnyBuilder::LargeUtf8(x) => x.append_null(),
        AnyBuilder::Int64(x) => x.append_null(),
        AnyBuilder::Int32(x) => x.append_null(),
        AnyBuilder::Int16(x) => x.append_null(),
        AnyBuilder::Int8(x) => x.append_null(),
        AnyBuilder::UInt64(x) => x.append_null(),
        AnyBuilder::UInt32(x) => x.append_null(),
        AnyBuilder::UInt16(x) => x.append_null(),
        AnyBuilder::UInt8(x) => x.append_null(),
        AnyBuilder::Float64(x) => x.append_null(),
        AnyBuilder::Float32(x) => x.append_null(),
        AnyBuilder::Boolean(x) => x.append_null(),
        AnyBuilder::Decimal128(x, ..) => x.append_null(),
        AnyBuilder::Date32(x) => x.append_null(),
        AnyBuilder::Date64(x) => x.append_null(),
        AnyBuilder::TimestampSecond(x) => x.append_null(),
        AnyBuilder::TimestampMillisecond(x) => x.append_null(),
        AnyBuilder::TimestampMicrosecond(x) => x.append_null(),
        AnyBuilder::TimestampNanosecond(x) => x.append_null(),
    }
}

#[inline]
fn append_typed(
    builder: &mut AnyBuilder,
    scratch: &TypedScratch,
    json_buf: &[u8],
) -> anyhow::Result<()> {
    match scratch {
        TypedScratch::Str(range) => {
            let s = str_val(json_buf, *range)?;
            match builder {
                AnyBuilder::Utf8(b) => b.append_value(s),
                AnyBuilder::LargeUtf8(b) => b.append_value(s),
                AnyBuilder::Int8(_)
                | AnyBuilder::Int16(_)
                | AnyBuilder::Int32(_)
                | AnyBuilder::Int64(_)
                | AnyBuilder::UInt8(_)
                | AnyBuilder::UInt16(_)
                | AnyBuilder::UInt32(_)
                | AnyBuilder::UInt64(_)
                | AnyBuilder::Float32(_)
                | AnyBuilder::Float64(_)
                | AnyBuilder::Boolean(_)
                | AnyBuilder::Json(_)
                | AnyBuilder::Decimal128(..)
                | AnyBuilder::Date32(_)
                | AnyBuilder::Date64(_)
                | AnyBuilder::TimestampSecond(_)
                | AnyBuilder::TimestampMillisecond(_)
                | AnyBuilder::TimestampMicrosecond(_)
                | AnyBuilder::TimestampNanosecond(_) => append_null(builder),
            }
        }
        TypedScratch::I64(n) => match builder {
            AnyBuilder::Int64(b) => b.append_value(*n),
            AnyBuilder::Int32(b) => b.append_value(*n as i32),
            AnyBuilder::Int16(b) => b.append_value(*n as i16),
            AnyBuilder::Int8(b) => b.append_value(*n as i8),
            AnyBuilder::Date32(b) => b.append_value(*n as i32),
            AnyBuilder::Date64(b) => b.append_value(*n),
            AnyBuilder::TimestampSecond(b) => b.append_value(*n),
            AnyBuilder::TimestampMillisecond(b) => b.append_value(*n),
            AnyBuilder::TimestampMicrosecond(b) => b.append_value(*n),
            AnyBuilder::TimestampNanosecond(b) => b.append_value(*n),
            AnyBuilder::Utf8(_)
            | AnyBuilder::LargeUtf8(_)
            | AnyBuilder::UInt8(_)
            | AnyBuilder::UInt16(_)
            | AnyBuilder::UInt32(_)
            | AnyBuilder::UInt64(_)
            | AnyBuilder::Float32(_)
            | AnyBuilder::Float64(_)
            | AnyBuilder::Boolean(_)
            | AnyBuilder::Json(_)
            | AnyBuilder::Decimal128(..) => append_null(builder),
        },
        TypedScratch::U64(n) => match builder {
            AnyBuilder::UInt64(b) => b.append_value(*n),
            AnyBuilder::UInt32(b) => b.append_value(*n as u32),
            AnyBuilder::UInt16(b) => b.append_value(*n as u16),
            AnyBuilder::UInt8(b) => b.append_value(*n as u8),
            AnyBuilder::Utf8(_)
            | AnyBuilder::LargeUtf8(_)
            | AnyBuilder::Int8(_)
            | AnyBuilder::Int16(_)
            | AnyBuilder::Int32(_)
            | AnyBuilder::Int64(_)
            | AnyBuilder::Float32(_)
            | AnyBuilder::Float64(_)
            | AnyBuilder::Boolean(_)
            | AnyBuilder::Json(_)
            | AnyBuilder::Decimal128(..)
            | AnyBuilder::Date32(_)
            | AnyBuilder::Date64(_)
            | AnyBuilder::TimestampSecond(_)
            | AnyBuilder::TimestampMillisecond(_)
            | AnyBuilder::TimestampMicrosecond(_)
            | AnyBuilder::TimestampNanosecond(_) => append_null(builder),
        },
        TypedScratch::F64(n) => match builder {
            AnyBuilder::Float64(b) => b.append_value(*n),
            AnyBuilder::Float32(b) => b.append_value(*n as f32),
            AnyBuilder::Utf8(_)
            | AnyBuilder::LargeUtf8(_)
            | AnyBuilder::Int8(_)
            | AnyBuilder::Int16(_)
            | AnyBuilder::Int32(_)
            | AnyBuilder::Int64(_)
            | AnyBuilder::UInt8(_)
            | AnyBuilder::UInt16(_)
            | AnyBuilder::UInt32(_)
            | AnyBuilder::UInt64(_)
            | AnyBuilder::Boolean(_)
            | AnyBuilder::Json(_)
            | AnyBuilder::Decimal128(..)
            | AnyBuilder::Date32(_)
            | AnyBuilder::Date64(_)
            | AnyBuilder::TimestampSecond(_)
            | AnyBuilder::TimestampMillisecond(_)
            | AnyBuilder::TimestampMicrosecond(_)
            | AnyBuilder::TimestampNanosecond(_) => append_null(builder),
        },
        TypedScratch::Bool(v) => match builder {
            AnyBuilder::Boolean(b) => b.append_value(*v),
            AnyBuilder::Utf8(_)
            | AnyBuilder::LargeUtf8(_)
            | AnyBuilder::Int8(_)
            | AnyBuilder::Int16(_)
            | AnyBuilder::Int32(_)
            | AnyBuilder::Int64(_)
            | AnyBuilder::UInt8(_)
            | AnyBuilder::UInt16(_)
            | AnyBuilder::UInt32(_)
            | AnyBuilder::UInt64(_)
            | AnyBuilder::Float32(_)
            | AnyBuilder::Float64(_)
            | AnyBuilder::Json(_)
            | AnyBuilder::Decimal128(..)
            | AnyBuilder::Date32(_)
            | AnyBuilder::Date64(_)
            | AnyBuilder::TimestampSecond(_)
            | AnyBuilder::TimestampMillisecond(_)
            | AnyBuilder::TimestampMicrosecond(_)
            | AnyBuilder::TimestampNanosecond(_) => append_null(builder),
        },
        TypedScratch::Empty => append_null(builder),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// DLQ — source metadata uses the same typed system-column contract as main data
// ---------------------------------------------------------------------------
fn dlq_schema(system_columns: &SystemColumns) -> Schema {
    let mut fields = vec![
        Field::new("raw_base64", DataType::Utf8, false),
        Field::new("error_message", DataType::Utf8, false),
        Field::new("source_write_timestamp_ms", DataType::Int64, true),
    ];
    for column in system_columns.iter() {
        fields.push(Field::new(
            column.name.as_ref(),
            column.kind.data_type(),
            false,
        ));
    }
    Schema::new(fields)
}

#[derive(Debug)]
struct RowConversionError(String);

impl core::fmt::Display for RowConversionError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// JsonParser
// ---------------------------------------------------------------------------

pub struct JsonParser {
    mappings: Vec<ColumnMappingExt>,
    kinds: Vec<ColumnKind>,
    arrow_schema: Arc<Schema>,
    mode: ParseMode,
    /// Base destination table name, stamped into every produced batch's meta.
    table: Arc<str>,
    /// Pre-resolved DLQ table name (`<table>_dlq`).
    dlq_table: Arc<str>,
    /// Cached per-column `DataType` (avoids double `parse_arrow_type`).
    data_types: Vec<DataType>,
    /// How to split incoming message bytes into individual JSON objects.
    json_framing: JsonFramingMode,
    system_kinds: Vec<SystemColumnKind>,
    system_columns: SystemColumns,
    dlq_system_columns: SystemColumns,
    /// Unique top-level mapped fields. The mixed `JSONPath` path uses this to
    /// reject the same duplicate keys as the root-field fast path.
    mapped_root_fields: Vec<String>,
    conversion_error: ConversionErrorPolicy,
    unknown_fields: UnknownFieldPolicy,
    mapped_top_level_fields: HashSet<String>,
}

struct ColumnMappingExt {
    column_name: Arc<str>,
    path: CompiledPath,
    /// `true` when the column is non-nullable (a missing value routes the row to DLQ).
    required: bool,
    json_data_type: JsonDataType,
    max_length: Option<usize>,
    time_conversion: Option<TimeConversion>,
}

impl JsonParser {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        super::memory::output_memory_bound(
            &self.kinds,
            &self.system_kinds,
            &self.dlq_system_columns,
            self.json_framing,
            messages,
        )
    }

    pub fn new(
        config: &JsonParserConfig,
        system_config: &SystemColumnsConfig,
        table: Arc<str>,
    ) -> anyhow::Result<Self> {
        if config.columns.is_empty() {
            anyhow::bail!("columns must not be empty");
        }
        let mut n = config.columns.len();
        let mut mappings = Vec::with_capacity(n);
        let mut kinds = Vec::with_capacity(n);
        let mut data_types = Vec::with_capacity(n);
        let mut all_root = true;
        let mut column_names = HashSet::with_capacity(n);
        let mut mapped_root_fields = Vec::new();
        let mut mapped_top_level_fields = HashSet::new();

        for col in &config.columns {
            anyhow::ensure!(
                column_names.insert(col.column_name.as_str()),
                "duplicate parser column name '{}'",
                col.column_name
            );
            let arrow_type = col.data_type()?;
            let kind = if col.arrow_type == "Json" {
                ColumnKind::Json
            } else {
                ColumnKind::from_data_type(&arrow_type).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Column '{}': unsupported Arrow type {:?}",
                        col.column_name,
                        arrow_type
                    )
                })?
            };
            let path = compile_path(&col.jsonpath).map_err(|error| {
                error.context(format!("column '{}': invalid JSONPath", col.column_name))
            })?;
            if matches!(&path, CompiledPath::Complex(_)) {
                all_root = false;
            }
            if let CompiledPath::RootField(field) = &path {
                mapped_top_level_fields.insert(field.clone());
                if !mapped_root_fields.contains(field) {
                    mapped_root_fields.push(field.clone());
                }
            }
            if let Some(field) = mapped_top_level_field(&col.jsonpath) {
                mapped_top_level_fields.insert(field.to_owned());
            } else {
                anyhow::bail!(
                    "column '{}': JSONPath must begin with a named top-level field because unknown_fields is explicit",
                    col.column_name
                );
            }
            kinds.push(kind);
            data_types.push(arrow_type);
            mappings.push(ColumnMappingExt {
                column_name: Arc::from(col.column_name.as_str()),
                path,
                required: !col.nullable,
                json_data_type: col.json_data_type,
                max_length: col.max_length,
                time_conversion: col.time_conversion.clone(),
            });
        }

        let required: Vec<bool> = config.columns.iter().map(|c| !c.nullable).collect();
        let required_total = required.iter().filter(|r| **r).count();

        let mode = if all_root
            && !matches!(
                config.unknown_fields,
                UnknownFieldPolicy::SendToColumn { .. }
            )
            && config.conversion_error == ConversionErrorPolicy::Dlq
            && config
                .columns
                .iter()
                .all(|column| column.time_conversion.is_none() && column.max_length.is_none())
            && kinds
                .iter()
                .all(|kind| !matches!(kind, ColumnKind::Json | ColumnKind::Decimal128(..)))
        {
            let pairs: Vec<(String, usize)> = mappings
                .iter()
                .enumerate()
                .filter_map(|(i, m)| match &m.path {
                    CompiledPath::RootField(f) => Some((f.clone(), i)),
                    CompiledPath::Complex(_) | CompiledPath::Rest => None,
                })
                .collect();
            let unique_fields = pairs
                .iter()
                .map(|(field, _)| field.as_str())
                .collect::<HashSet<_>>()
                .len()
                == pairs.len();
            if unique_fields {
                // Adaptive: linear scan for ≤12 cols (no hash overhead), HashMap for more.
                let index = if n <= 12 {
                    ColumnIndex::Small(pairs)
                } else {
                    ColumnIndex::Large(pairs.into_iter().collect())
                };
                ParseMode::AllRootField(RootFieldInfo {
                    index,
                    required,
                    required_total,
                    reject_unknown: matches!(config.unknown_fields, UnknownFieldPolicy::Fail),
                })
            } else {
                // One JSON field may feed multiple output columns. The single-index
                // extractor cannot represent that, so use the general compiled path.
                ParseMode::Mixed
            }
        } else {
            ParseMode::Mixed
        };

        let mut fields: Vec<Field> = config
            .columns
            .iter()
            .zip(config.to_dataset_schema()?.columns.iter())
            .map(|(col, schema)| {
                Field::new(&col.column_name, schema.data_type.clone(), col.nullable)
                    .with_metadata(schema.arrow_metadata())
            })
            .collect();
        if let UnknownFieldPolicy::SendToColumn { column_name } = &config.unknown_fields {
            anyhow::ensure!(
                all_root,
                "unknown_fields.action=send_to_column currently requires only simple top-level JSONPaths"
            );
            fields.push(
                Field::new(column_name, DataType::Utf8, false).with_metadata(
                    SchemaColumn::new(column_name.clone(), DataType::Utf8, false)
                        .with_arrow_extension(
                            transferia_core::data::schema::ARROW_JSON_EXTENSION_NAME,
                        )
                        .arrow_metadata(),
                ),
            );
            data_types.push(DataType::Utf8);
            kinds.push(ColumnKind::Utf8);
            mappings.push(ColumnMappingExt {
                column_name: Arc::from(column_name.as_str()),
                path: CompiledPath::Rest,
                required: true,
                json_data_type: JsonDataType::String,
                max_length: None,
                time_conversion: None,
            });
            n += 1;
        }
        let mut schema_fields = fields;
        let system_kinds: Vec<_> = system_config.enabled().collect();
        for kind in [
            SystemColumnKind::Topic,
            SystemColumnKind::Partition,
            SystemColumnKind::Offset,
            SystemColumnKind::MessageIndex,
            SystemColumnKind::WriteTimestampMs,
        ] {
            let name = system_config.name(kind);
            if column_names.contains(name) {
                anyhow::bail!("user column '{name}' conflicts with reserved system column");
            }
        }
        for kind in &system_kinds {
            let name = system_config.name(*kind);
            let field = Field::new(name, kind.data_type(), false);
            schema_fields.push(if config.keys.iter().any(|key| key == name) {
                field.with_metadata(
                    SchemaColumn::new(name.to_owned(), kind.data_type(), false)
                        .with_constraints(true, false, None)
                        .arrow_metadata(),
                )
            } else {
                field
            });
        }
        let arrow_schema = Arc::new(Schema::new(schema_fields));
        let dlq_table: Arc<str> = dlq_name(&table).into();
        let system_columns = SystemColumns::new(
            system_kinds
                .iter()
                .enumerate()
                .map(|(offset, kind)| SystemColumn {
                    kind: *kind,
                    name: Arc::from(system_config.name(*kind)),
                    index: n + offset,
                })
                .collect::<Vec<_>>(),
        );
        let dlq_system_columns = SystemColumns::new(
            system_kinds
                .iter()
                .enumerate()
                .map(|(offset, kind)| SystemColumn {
                    kind: *kind,
                    name: Arc::from(system_config.name(*kind)),
                    index: 3 + offset,
                })
                .collect::<Vec<_>>(),
        );

        Ok(Self {
            mappings,
            kinds,
            arrow_schema,
            mode,
            table,
            dlq_table,
            data_types,
            json_framing: config.json_framing,
            system_kinds,
            system_columns,
            dlq_system_columns,
            mapped_root_fields,
            conversion_error: config.conversion_error,
            unknown_fields: config.unknown_fields.clone(),
            mapped_top_level_fields,
        })
    }

    #[inline]
    fn extract_value(&self, json: &Value, mapping: &ColumnMappingExt) -> Option<Value> {
        match &mapping.path {
            CompiledPath::RootField(field) => json.get(field).cloned(),
            CompiledPath::Complex(path) => path
                .select(json)
                .ok()
                .and_then(|r| r.first().map(|v| (*v).clone())),
            CompiledPath::Rest => json.as_object().map(|object| {
                Value::Object(
                    object
                        .iter()
                        .filter(|(key, _)| !self.mapped_top_level_fields.contains(*key))
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                )
            }),
        }
    }

    fn has_duplicate_mapped_root_field(&self, bytes: &[u8]) -> bool {
        if self.mapped_root_fields.is_empty() {
            return false;
        }

        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        deserializer
            .deserialize_map(DuplicateMappedRootVisitor {
                fields: &self.mapped_root_fields,
            })
            .and_then(|duplicate| {
                deserializer.end()?;
                Ok(duplicate)
            })
            .unwrap_or(false)
    }

    fn root_extraction_failure(&self, scratch: &[TypedScratch]) -> DlqReason {
        let failed = self
            .mappings
            .iter()
            .zip(scratch)
            .filter(|(mapping, value)| mapping.required && matches!(value, TypedScratch::Empty))
            .map(|(mapping, _)| format!("'{}'", mapping.column_name))
            .collect::<Vec<_>>();
        let columns = if failed.is_empty() {
            self.mappings
                .iter()
                .map(|mapping| format!("'{}'", mapping.column_name))
                .collect::<Vec<_>>()
        } else {
            failed
        };
        DlqReason::ExtractionFailed(format!(
            "JSONPath extraction failed for columns: {}",
            columns.join(", ")
        ))
    }

    fn duplicate_mapped_field_failure(&self) -> DlqReason {
        DlqReason::ExtractionFailed(format!(
            "JSONPath extraction failed for mapped columns: {}; JSON object repeats a mapped field",
            self.mappings
                .iter()
                .map(|mapping| format!("'{}'", mapping.column_name))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }

    fn build_dlq_batch(
        &self,
        messages: &[Message],
        dlq_records: Vec<DlqRecord>,
    ) -> anyhow::Result<TableData> {
        let n = dlq_records.len();
        let encoded_bytes = dlq_records.iter().fold(0_usize, |total, record| {
            total.saturating_add(
                ((record.byte_end - record.byte_start) as usize)
                    .div_ceil(3)
                    .saturating_mul(4),
            )
        });
        let error_bytes = dlq_records.iter().fold(0_usize, |total, record| {
            total.saturating_add(record.reason.as_str().len())
        });
        let topic_bytes = dlq_records.iter().fold(0_usize, |total, record| {
            total.saturating_add(
                messages[record.source_message as usize]
                    .meta
                    .topic
                    .as_ref()
                    .map_or(0, |topic| topic.len()),
            )
        });
        // The encoded size is exact and bounded by the parser-delivery cap.
        // Reserve it once: geometric Vec growth would briefly retain both the
        // old and new base64 buffers at the largest allocation.
        let mut raw_builder = StringBuilder::with_capacity(n, encoded_bytes);
        let mut err_builder = StringBuilder::with_capacity(n, error_bytes);
        let mut source_ts_builder = Int64Builder::with_capacity(n);

        let mut system_builders: Vec<_> = self
            .system_kinds
            .iter()
            .map(|kind| make_exact_system_builder(*kind, n, topic_bytes))
            .collect();

        for record in dlq_records {
            let message = &messages[record.source_message as usize];
            let system_values = self.system_column_values(&message.meta)?;
            append_base64(
                &mut raw_builder,
                &message.value[record.byte_start as usize..record.byte_end as usize],
            )?;
            err_builder.append_value(record.reason.as_str());
            source_ts_builder.append_option(message.meta.write_timestamp_ms);
            append_system_columns(
                &mut system_builders,
                0,
                &self.system_kinds,
                &system_values,
                u64::from(record.record_index),
            );
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(3 + system_builders.len());
        arrays.push(Arc::new(raw_builder.finish()));
        arrays.push(Arc::new(err_builder.finish()));
        arrays.push(Arc::new(source_ts_builder.finish()));
        arrays.extend(system_builders.iter_mut().map(AnyBuilder::finish));
        let batch = RecordBatch::try_new(Arc::new(dlq_schema(&self.dlq_system_columns)), arrays)?;

        Ok(TableData {
            batch,
            table: Arc::clone(&self.dlq_table),
            is_dlq: true,
            system_columns: self.dlq_system_columns.clone(),
        })
    }

    fn system_column_values<'a>(
        &self,
        meta: &'a MessageMeta,
    ) -> anyhow::Result<SystemColumnValues<'a>> {
        let missing = |kind: SystemColumnKind| {
            anyhow::anyhow!(
                "source message is missing metadata required for system column '{}'",
                self.system_columns.get(kind).map_or_else(
                    || kind.default_name().to_owned(),
                    |column| column.name.to_string()
                )
            )
        };
        let topic = if self.system_kinds.contains(&SystemColumnKind::Topic) {
            meta.topic
                .as_deref()
                .ok_or_else(|| missing(SystemColumnKind::Topic))?
        } else {
            ""
        };
        let partition = if self.system_kinds.contains(&SystemColumnKind::Partition) {
            meta.partition
                .ok_or_else(|| missing(SystemColumnKind::Partition))?
        } else {
            0
        };
        let offset = if self.system_kinds.contains(&SystemColumnKind::Offset) {
            meta.offset
                .ok_or_else(|| missing(SystemColumnKind::Offset))?
        } else {
            0
        };
        let write_timestamp_ms = if self
            .system_kinds
            .contains(&SystemColumnKind::WriteTimestampMs)
        {
            meta.write_timestamp_ms
                .ok_or_else(|| missing(SystemColumnKind::WriteTimestampMs))?
        } else {
            0
        };
        Ok(SystemColumnValues {
            topic,
            partition,
            offset,
            write_timestamp_ms,
        })
    }

    /// Appends one successfully parsed typed row and its configured system columns.
    fn append_root_line(
        builders: &mut [AnyBuilder],
        typed_scratch: &[TypedScratch],
        json_buf: &[u8],
        system_values: &SystemColumnValues<'_>,
        message_index: u64,
        system_kinds: &[SystemColumnKind],
    ) -> anyhow::Result<()> {
        for (builder, s) in builders.iter_mut().zip(typed_scratch.iter()) {
            append_typed(builder, s, json_buf)?;
        }
        append_system_columns(
            builders,
            typed_scratch.len(),
            system_kinds,
            system_values,
            message_index,
        );
        Ok(())
    }

    /// Parses `JsonLines` records directly from their source buffers. Keeping
    /// only compact DLQ descriptors avoids a second per-record allocation for
    /// dense invalid input.
    fn parse_all_root_newline(
        &self,
        messages: &[Message],
        info: &RootFieldInfo,
        builders: &mut [AnyBuilder],
        typed_scratch: &mut [TypedScratch],
        typed_seen: &mut [bool],
        json_buf: &mut Vec<u8>,
        dlq_records: &mut Vec<DlqRecord>,
    ) -> anyhow::Result<()> {
        for (source_message, msg) in messages.iter().enumerate() {
            let system_values = self.system_column_values(&msg.meta)?;
            for (record_index, line) in msg
                .value
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .enumerate()
            {
                typed_scratch.fill(TypedScratch::Empty);
                match parse_root_fields_typed(
                    line,
                    json_buf,
                    info,
                    typed_scratch,
                    typed_seen,
                    &self.kinds,
                ) {
                    Ok(true) => {
                        Self::append_root_line(
                            builders,
                            typed_scratch,
                            json_buf,
                            &system_values,
                            record_index as u64,
                            &self.system_kinds,
                        )?;
                    }
                    Ok(false) => dlq_records.push(dlq_record(
                        source_message,
                        subslice_range(&msg.value, line),
                        self.root_extraction_failure(typed_scratch),
                        record_index as u64,
                    )),
                    Err(_error) => dlq_records.push(dlq_record(
                        source_message,
                        subslice_range(&msg.value, line),
                        DlqReason::JsonParse,
                        record_index as u64,
                    )),
                }
            }
        }
        Ok(())
    }

    fn parse_all_root_nosplit(
        &self,
        messages: &[Message],
        info: &RootFieldInfo,
        builders: &mut [AnyBuilder],
        typed_scratch: &mut [TypedScratch],
        typed_seen: &mut [bool],
        json_buf: &mut Vec<u8>,
        dlq_records: &mut Vec<DlqRecord>,
    ) -> anyhow::Result<()> {
        for (source_message, msg) in messages.iter().enumerate() {
            let system_values = self.system_column_values(&msg.meta)?;
            typed_scratch.fill(TypedScratch::Empty);
            match parse_root_fields_typed(
                &msg.value,
                json_buf,
                info,
                typed_scratch,
                typed_seen,
                &self.kinds,
            ) {
                Ok(true) => Self::append_root_line(
                    builders,
                    typed_scratch,
                    json_buf,
                    &system_values,
                    0,
                    &self.system_kinds,
                )?,
                Ok(false) => dlq_records.push(dlq_record(
                    source_message,
                    0..msg.value.len(),
                    self.root_extraction_failure(typed_scratch),
                    0,
                )),
                Err(_error) => dlq_records.push(dlq_record(
                    source_message,
                    0..msg.value.len(),
                    DlqReason::JsonParse,
                    0,
                )),
            }
        }
        Ok(())
    }

    /// Fills and validates one parsed row before touching any Arrow builder.
    /// This two-phase contract prevents a late type/range error from leaving
    /// columns with different lengths.
    fn fill_row(&self, json: &Value, row: &mut Vec<Value>) -> Result<(), RowConversionError> {
        if matches!(self.unknown_fields, UnknownFieldPolicy::Fail) {
            let object = json
                .as_object()
                .ok_or_else(|| RowConversionError("JSON root is not an object".into()))?;
            if let Some(field) = object
                .keys()
                .find(|field| !self.mapped_top_level_fields.contains(*field))
            {
                return Err(RowConversionError(format!("unknown JSON field '{field}'")));
            }
        }
        row.clear();
        for (mapping, kind) in self.mappings.iter().zip(self.kinds.iter().copied()) {
            let failed = |detail: String| {
                RowConversionError(format!(
                    "JSONPath extraction failed for column '{}': {detail}",
                    mapping.column_name
                ))
            };
            let mut value = match self.extract_value(json, mapping) {
                Some(value) => value,
                None if !mapping.required => Value::Null,
                None => return Err(failed("required JSONPath is missing".into())),
            };
            if matches!(mapping.path, CompiledPath::Rest) {
                value = Value::String(
                    serde_json::to_string(&value).map_err(|error| failed(error.to_string()))?,
                );
            }
            if (value.is_null() && mapping.required)
                || !json_value_matches(mapping.json_data_type, &value)
            {
                return Err(failed(
                    "JSON value does not satisfy the declared conversion".into(),
                ));
            }
            if kind == ColumnKind::Json && !value.is_null() {
                value = Value::String(
                    serde_json::to_string(&value).map_err(|error| failed(error.to_string()))?,
                );
            }
            if let Some(conversion) = &mapping.time_conversion {
                value = convert_time_value(&value, conversion, kind)
                    .map_err(|error| failed(error.to_string()))?;
            }
            if let ColumnKind::Decimal128(precision, scale) = kind {
                if !value.is_null() && decimal_unscaled(&value, precision, scale).is_none() {
                    return Err(failed(format!(
                        "decimal value exceeds precision {precision} or cannot be represented exactly at scale {scale}"
                    )));
                }
            }
            if !value_matches_kind(kind, &value) {
                return Err(failed(
                    "converted value is outside the declared Arrow type".into(),
                ));
            }
            if mapping.max_length.is_some_and(|limit| {
                value
                    .as_str()
                    .is_some_and(|text| text.chars().count() > limit)
            }) {
                return Err(failed("string exceeds configured max_length".into()));
            }
            row.push(value);
        }
        Ok(())
    }

    fn append_mixed_row(builders: &mut [AnyBuilder], row: &[Value]) {
        for (builder, value) in builders.iter_mut().zip(row) {
            assert!(
                append_value(builder, value),
                "validated JSON value no longer matches its Arrow builder"
            );
        }
    }

    fn handle_parse_error(
        &self,
        dlq_records: &mut Vec<DlqRecord>,
        record: DlqRecord,
        message: &str,
    ) -> anyhow::Result<()> {
        match self.conversion_error {
            ConversionErrorPolicy::Dlq => dlq_records.push(record),
            ConversionErrorPolicy::Drop => {}
            ConversionErrorPolicy::Fail => anyhow::bail!(message.to_owned()),
        }
        Ok(())
    }

    fn parse_mixed_newline(
        &self,
        messages: &[Message],
        builders: &mut [AnyBuilder],
        dlq_records: &mut Vec<DlqRecord>,
        row: &mut Vec<Value>,
    ) -> anyhow::Result<()> {
        for (source_message, msg) in messages.iter().enumerate() {
            let system_values = self.system_column_values(&msg.meta)?;
            for (message_index, line) in msg
                .value
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .enumerate()
            {
                let message_index = message_index as u64;
                if self.has_duplicate_mapped_root_field(line) {
                    self.handle_parse_error(
                        dlq_records,
                        dlq_record(
                            source_message,
                            subslice_range(&msg.value, line),
                            self.duplicate_mapped_field_failure(),
                            message_index,
                        ),
                        "JSON object repeats a mapped field",
                    )?;
                    continue;
                }
                match serde_json::from_slice::<Value>(line) {
                    Ok(json) => {
                        if let Err(error) = self.fill_row(&json, row) {
                            self.handle_parse_error(
                                dlq_records,
                                dlq_record(
                                    source_message,
                                    subslice_range(&msg.value, line),
                                    DlqReason::ExtractionFailed(error.to_string()),
                                    message_index,
                                ),
                                &error.to_string(),
                            )?;
                        } else {
                            Self::append_mixed_row(builders, row);
                            append_system_columns(
                                builders,
                                self.mappings.len(),
                                &self.system_kinds,
                                &system_values,
                                message_index,
                            );
                        }
                    }
                    Err(error) => {
                        self.handle_parse_error(
                            dlq_records,
                            dlq_record(
                                source_message,
                                subslice_range(&msg.value, line),
                                DlqReason::JsonParse,
                                message_index,
                            ),
                            &format!("invalid JSON: {error}"),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn parse_mixed_nosplit(
        &self,
        messages: &[Message],
        builders: &mut [AnyBuilder],
        dlq_records: &mut Vec<DlqRecord>,
        row: &mut Vec<Value>,
    ) -> anyhow::Result<()> {
        for (source_message, msg) in messages.iter().enumerate() {
            let system_values = self.system_column_values(&msg.meta)?;
            if self.has_duplicate_mapped_root_field(&msg.value) {
                self.handle_parse_error(
                    dlq_records,
                    dlq_record(
                        source_message,
                        0..msg.value.len(),
                        self.duplicate_mapped_field_failure(),
                        0,
                    ),
                    "JSON object repeats a mapped field",
                )?;
                continue;
            }
            match serde_json::from_slice::<Value>(&msg.value) {
                Ok(json) => {
                    if let Err(error) = self.fill_row(&json, row) {
                        self.handle_parse_error(
                            dlq_records,
                            dlq_record(
                                source_message,
                                0..msg.value.len(),
                                DlqReason::ExtractionFailed(error.to_string()),
                                0,
                            ),
                            &error.to_string(),
                        )?;
                    } else {
                        Self::append_mixed_row(builders, row);
                        append_system_columns(
                            builders,
                            self.mappings.len(),
                            &self.system_kinds,
                            &system_values,
                            0,
                        );
                    }
                }
                Err(error) => self.handle_parse_error(
                    dlq_records,
                    dlq_record(source_message, 0..msg.value.len(), DlqReason::JsonParse, 0),
                    &format!("invalid JSON: {error}"),
                )?,
            }
        }
        Ok(())
    }

    fn parse_mixed(&self, messages: &[Message], ws: &mut ParserWorkspace) -> anyhow::Result<()> {
        let n_cols = self.mappings.len();
        let mut row: Vec<Value> = Vec::with_capacity(n_cols);
        match self.json_framing {
            JsonFramingMode::JsonLines | JsonFramingMode::JsonArray => {
                self.parse_mixed_newline(
                    messages,
                    &mut ws.builders,
                    &mut ws.dlq_records,
                    &mut row,
                )?;
            }
            JsonFramingMode::SingleDocument => {
                self.parse_mixed_nosplit(
                    messages,
                    &mut ws.builders,
                    &mut ws.dlq_records,
                    &mut row,
                )?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// parse_into — main hot path
// ---------------------------------------------------------------------------

impl JsonParser {
    pub fn parse_into(
        &self,
        messages: Vec<Message>,
        ws: &mut ParserWorkspace,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let messages = frame_json_arrays(self.json_framing, self.conversion_error, messages)?;
        // Count rows without retaining a second per-record index. Parsing
        // performs the same allocation-free split over the source buffers.
        let n_rows: usize = match self.json_framing {
            JsonFramingMode::JsonLines | JsonFramingMode::JsonArray => messages
                .iter()
                .map(|msg| self.json_framing.count_records(&msg.value))
                .sum(),
            JsonFramingMode::SingleDocument => messages.len(),
        };
        let input_bytes = messages.iter().fold(0_usize, |total, message| {
            total.saturating_add(message.value.len())
        });
        let estimated_dlq_rows = if matches!(
            self.json_framing,
            JsonFramingMode::JsonLines | JsonFramingMode::JsonArray
        ) {
            n_rows
        } else {
            messages.len()
        };
        // Total UTF-8 output cannot be estimated exactly before parsing. Sharing the
        // input-size estimate between string columns avoids the previous 128 × rows ×
        // columns allocation spike while still giving builders useful capacity.
        let string_columns = self
            .kinds
            .iter()
            .filter(|kind| matches!(kind, ColumnKind::Utf8 | ColumnKind::LargeUtf8))
            .count();
        let string_bytes = input_bytes / string_columns.max(1);

        ws.builders.clear();
        for (&kind, data_type) in self.kinds.iter().zip(&self.data_types) {
            ws.builders
                .push(make_builder(kind, data_type, n_rows, string_bytes));
        }
        for kind in &self.system_kinds {
            ws.builders.push(make_system_builder(*kind, n_rows));
        }

        ws.dlq_records.clear();
        ws.dlq_records.reserve_exact(estimated_dlq_rows);

        match &self.mode {
            ParseMode::AllRootField(info) => {
                let n_cols = info.index.len();
                let ParserWorkspace {
                    builders,
                    typed_scratch,
                    typed_seen,
                    json_buf,
                    dlq_records,
                    ..
                } = ws;
                typed_scratch.clear();
                typed_scratch.resize_with(n_cols, || TypedScratch::Empty);
                typed_seen.clear();
                typed_seen.resize(n_cols, false);
                match self.json_framing {
                    JsonFramingMode::JsonLines | JsonFramingMode::JsonArray => self
                        .parse_all_root_newline(
                            &messages,
                            info,
                            builders,
                            typed_scratch,
                            typed_seen,
                            json_buf,
                            dlq_records,
                        )?,
                    JsonFramingMode::SingleDocument => self.parse_all_root_nosplit(
                        &messages,
                        info,
                        builders,
                        typed_scratch,
                        typed_seen,
                        json_buf,
                        dlq_records,
                    )?,
                }
            }
            ParseMode::Mixed => {
                self.parse_mixed(&messages, ws)?;
            }
        }

        ws.arrays.clear();
        ws.arrays
            .extend(ws.builders.iter_mut().map(AnyBuilder::finish));
        let batch = RecordBatch::try_new(
            Arc::clone(&self.arrow_schema),
            core::mem::take(&mut ws.arrays),
        )?;

        let valid_batch = TableData {
            batch,
            table: Arc::clone(&self.table),
            is_dlq: false,
            system_columns: self.system_columns.clone(),
        };

        let dlq_records = core::mem::take(&mut ws.dlq_records);
        // simd-json scratch can be as large as the source record. Release it
        // before base64 materialization so both peaks cannot overlap.
        ws.release_large_scratch();
        let dlq_batch = if dlq_records.is_empty() {
            None
        } else {
            Some(self.build_dlq_batch(&messages, dlq_records)?)
        };

        Ok((valid_batch, dlq_batch))
    }
}

struct JsonParserSession {
    parser: Arc<JsonParser>,
    workspace: ParserWorkspace,
    memory_limit_bytes: usize,
}

impl ParserSession for JsonParserSession {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        self.parser.output_memory_bound(messages)
    }

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let estimated_working_set_bytes = self.parser.output_memory_bound(&messages);
        if estimated_working_set_bytes > self.memory_limit_bytes {
            let input_bytes = messages.iter().fold(0_usize, |total, message| {
                total.saturating_add(message.value.len())
            });
            tracing::warn!(
                input_bytes,
                message_count = messages.len(),
                estimated_working_set_bytes,
                pipeline_memory_limit_bytes = self.memory_limit_bytes,
                "single JSON parser delivery exceeds the pipeline memory budget; admitting it alone under pipeline backpressure"
            );
        }
        self.parser.parse_into(messages, &mut self.workspace)
    }
}

impl ParserFactory for JsonParser {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(JsonParserSession {
            parser: self,
            workspace: ParserWorkspace::new(),
            memory_limit_bytes,
        })
    }
}
// Regression tests — validate the simd-json invariant on real inputs
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/parser.rs"]
mod tests;
