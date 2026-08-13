use alloc::sync::Arc;
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Date64Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, LargeStringBuilder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use core::fmt;
use serde::{de, Deserializer};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::Write as _;

use crate::parsers::json_parser::config::{
    parse_arrow_type, ChunkSplitter, ConversionErrorPolicy, EpochUnit, JsonDataType,
    JsonParserConfig, TimeConversion, UnknownFieldPolicy,
};
use crate::parsers::{ParserFactory, ParserSession, SystemColumnsConfig};
use crate::types::message::{Message, MessageMeta};
use crate::types::schema::SchemaColumn;
use crate::types::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use crate::types::table_data::{dlq_name, TableData};

/// Hard bound for one parser delivery's materialized Arrow data and the
/// conservative working-set estimate used before builders allocate.
const MAX_DELIVERY_BYTES: usize = 256 * 1024 * 1024;
const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;

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

/// Adaptive column index: linear scan for ≤12 columns (faster — no hash),
/// `HashMap` for wider schemas.
enum ColumnIndex {
    Small(Vec<(String, usize)>),
    Large(HashMap<String, usize>),
}

impl ColumnIndex {
    fn len(&self) -> usize {
        match *self {
            Self::Small(ref v) => v.len(),
            Self::Large(ref m) => m.len(),
        }
    }

    #[inline]
    fn get(&self, key: &str) -> Option<&usize> {
        match *self {
            Self::Small(ref v) => v
                .iter()
                .find(|item| item.0.as_str() == key)
                .map(|item| &item.1),
            Self::Large(ref m) => m.get(key),
        }
    }
}

struct RootFieldInfo {
    index: ColumnIndex,
    /// Per-column requiredness (`true` == non-nullable). Indexed by column position.
    required: Vec<bool>,
    /// Number of `true` entries in `required` — the count that must be filled
    /// for a row to be valid (missing nullable fields become NULL, not DLQ).
    required_total: usize,
    reject_unknown: bool,
}

enum ParseMode {
    AllRootField(RootFieldInfo),
    Mixed,
}

// ---------------------------------------------------------------------------
// Arrow builder enum
// ---------------------------------------------------------------------------

enum AnyBuilder {
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
    Date32(Date32Builder),
    Date64(Date64Builder),
    TimestampSecond(TimestampSecondBuilder),
    TimestampMillisecond(TimestampMillisecondBuilder),
    TimestampMicrosecond(TimestampMicrosecondBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
}

#[derive(Clone, Copy)]
enum ColumnKind {
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
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Map(..)
            | DataType::RunEndEncoded(..) => return None,
        })
    }

    const fn fixed_width_bytes(self) -> Option<usize> {
        match self {
            Self::Utf8 | Self::LargeUtf8 => None,
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
            Self::Utf8(b) => Arc::new(b.finish()),
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
        ColumnKind::Utf8 | ColumnKind::LargeUtf8 => val.as_str().is_some(),
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
    }
}

fn json_value_matches(kind: JsonDataType, value: &Value) -> bool {
    value.is_null()
        || match kind {
            JsonDataType::String => value.is_string(),
            JsonDataType::Integer => value.as_i64().is_some(),
            JsonDataType::UnsignedInteger => value.as_u64().is_some(),
            JsonDataType::Number => value.as_f64().is_some_and(f64::is_finite),
            JsonDataType::Boolean => value.is_boolean(),
        }
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

// ---------------------------------------------------------------------------
// Zero-copy typed scratch — no String allocations for Utf8 columns
// ---------------------------------------------------------------------------

/// A byte range in `json_buf` that simd-json has proven to contain valid UTF-8.
///
/// **Type-level witness.** Only constructible via [`ValidatedStr::from_simd_json_str`],
/// which takes a `&str` reference returned by simd-json's validated parse. Because
/// `&str` is `#[repr(transparent)]` over valid UTF-8 bytes, constructing a
/// `ValidatedStr` from one is safe without re-validation.
///
/// This type guarantees that [`str_val`] receives only simd-json-validated ranges —
/// the compiler physically prevents any other code path from feeding arbitrary byte
/// ranges into the unsafe block.
#[derive(Clone, Copy, Debug)]
struct ValidatedStr {
    start: usize,
    end: usize,
}

impl ValidatedStr {
    /// Creates a `ValidatedStr` from a simd-json `&str` reference.
    ///
    /// `s` — the `&str` returned by simd-json (already UTF-8 validated).
    /// `buf_ptr` — base address of the JSON buffer (`buf.as_ptr()`).
    ///
    /// The byte range `[start..end)` is computed via pointer arithmetic and
    /// exactly matches the validated `&str` bytes in the buffer.
    #[inline]
    fn from_simd_json_str(s: &str, buf_ptr: *const u8) -> Self {
        let start = s.as_ptr() as usize - buf_ptr as usize;
        Self {
            start,
            end: start + s.len(),
        }
    }
}

/// Per-field scratch value. Strings are stored as [`ValidatedStr`] — byte
/// ranges whose UTF-8 validity is witnessed at the type level.
#[derive(Clone, Copy, Debug)]
enum TypedScratch {
    Empty,
    Str(ValidatedStr),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

/// Writes a deserialized value directly into `TypedScratch` according to `ColumnKind`.
/// Strings are stored as byte-range indices — no `String` allocation.
struct TypedValueWriter<'ctx> {
    target: &'ctx mut TypedScratch,
    /// Base pointer of the JSON buffer. Used to compute the byte offset of the
    /// `&str` returned by simd-json via pointer arithmetic:
    /// `offset = s.as_ptr() - buf_ptr`.
    buf_ptr: *const u8,
    kind: ColumnKind,
}

impl<'de> de::DeserializeSeed<'de> for TypedValueWriter<'_> {
    /// `true` means that the JSON value was non-null and was written.
    type Value = bool;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        use serde::Deserialize as _;
        match self.kind {
            ColumnKind::Utf8 | ColumnKind::LargeUtf8 => {
                let Some(s) = Option::<&str>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                // ValidatedStr captures the byte range of s within the simd-json
                // buffer. Because `s` is an `&str`, it is valid UTF-8 by definition
                // — simd-json already validated it. The pointer arithmetic gives us
                // the exact byte range without a manual position counter.
                *self.target = TypedScratch::Str(ValidatedStr::from_simd_json_str(s, self.buf_ptr));
            }
            ColumnKind::Int32 | ColumnKind::Date32 => {
                let Some(value) = Option::<i32>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                *self.target = TypedScratch::I64(i64::from(value));
            }
            ColumnKind::Int16 => {
                let Some(value) = Option::<i16>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                *self.target = TypedScratch::I64(i64::from(value));
            }
            ColumnKind::Int8 => {
                let Some(value) = Option::<i8>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                *self.target = TypedScratch::I64(i64::from(value));
            }
            ColumnKind::UInt64 => {
                let Some(value) = Option::<u64>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                *self.target = TypedScratch::U64(value);
            }
            ColumnKind::UInt32 => {
                let Some(value) = Option::<u32>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                *self.target = TypedScratch::U64(u64::from(value));
            }
            ColumnKind::UInt16 => {
                let Some(value) = Option::<u16>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                *self.target = TypedScratch::U64(u64::from(value));
            }
            ColumnKind::UInt8 => {
                let Some(value) = Option::<u8>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                *self.target = TypedScratch::U64(u64::from(value));
            }
            ColumnKind::Float64 => {
                let Some(value) = Option::<f64>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                if !value.is_finite() {
                    return Err(de::Error::custom("non-finite Float64 value"));
                }
                *self.target = TypedScratch::F64(value);
            }
            ColumnKind::Float32 => {
                let Some(value) = Option::<f64>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                if !value.is_finite() || value < f64::from(f32::MIN) || value > f64::from(f32::MAX)
                {
                    return Err(de::Error::custom("Float32 value is out of range"));
                }
                *self.target = TypedScratch::F64(value);
            }
            ColumnKind::Boolean => {
                let Some(value) = Option::<bool>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                *self.target = TypedScratch::Bool(value);
            }
            ColumnKind::Int64
            | ColumnKind::Date64
            | ColumnKind::TimestampMillisecond
            | ColumnKind::TimestampMicrosecond
            | ColumnKind::TimestampNanosecond
            | ColumnKind::TimestampSecond => {
                let Some(value) = Option::<i64>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                *self.target = TypedScratch::I64(value);
            }
        }
        Ok(true)
    }
}

/// Reconstructs a `&str` from a validated byte range — zero-copy.
///
/// The `range` is a [`ValidatedStr`] — a type-level witness that the bytes
/// at `json_buf[range.start..range.end]` have been proven to be valid UTF-8
/// by simd-json's parse pass. Skipping `from_utf8` saves an O(len) SIMD scan.
///
/// # SAFETY
///
/// The caller must ensure that `json_buf[range.start..range.end]` is valid UTF-8.
/// This invariant is upheld by the [`ValidatedStr`] type, which can **only** be
/// constructed via [`ValidatedStr::from_simd_json_str`]. That method receives a
/// `&str` from simd-json — a reference that is valid UTF-8 by definition
/// (`&str` is `#[repr(transparent)]` over `[u8]` with a UTF-8 validity
/// invariant). The byte range is computed via pointer arithmetic from that `&str`,
/// so `json_buf[start..end]` IS the exact same memory as the original `&str`.
///
/// There is no other public constructor for `ValidatedStr`, and the field is
/// private to this module. The compiler physically prevents any other code path
/// from calling this function with arbitrary byte ranges.
#[inline]
#[expect(
    unsafe_code,
    reason = "ValidatedStr proves this hot-path slice was already UTF-8 validated"
)]
fn str_val(json_buf: &[u8], range: ValidatedStr) -> &str {
    // SAFETY: ValidatedStr can only be constructed from a validated `str`
    // pointing into this exact buffer.
    unsafe { core::str::from_utf8_unchecked(&json_buf[range.start..range.end]) }
}

/// Appends a typed scratch value into the corresponding Arrow builder.
/// Strings are reconstructed from `json_buf` byte ranges — zero-copy.
/// Appends a NULL to any builder variant.
#[inline]
fn append_null(b: &mut AnyBuilder) {
    match b {
        AnyBuilder::Utf8(x) => x.append_null(),
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
        AnyBuilder::Date32(x) => x.append_null(),
        AnyBuilder::Date64(x) => x.append_null(),
        AnyBuilder::TimestampSecond(x) => x.append_null(),
        AnyBuilder::TimestampMillisecond(x) => x.append_null(),
        AnyBuilder::TimestampMicrosecond(x) => x.append_null(),
        AnyBuilder::TimestampNanosecond(x) => x.append_null(),
    }
}

#[inline]
fn append_typed(builder: &mut AnyBuilder, scratch: &TypedScratch, json_buf: &[u8]) {
    match scratch {
        TypedScratch::Str(range) => {
            let s = str_val(json_buf, *range);
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
            | AnyBuilder::Boolean(_) => append_null(builder),
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
            | AnyBuilder::Date32(_)
            | AnyBuilder::Date64(_)
            | AnyBuilder::TimestampSecond(_)
            | AnyBuilder::TimestampMillisecond(_)
            | AnyBuilder::TimestampMicrosecond(_)
            | AnyBuilder::TimestampNanosecond(_) => append_null(builder),
        },
        TypedScratch::Empty => append_null(builder),
    }
}

fn make_system_builder(kind: SystemColumnKind, capacity: usize) -> AnyBuilder {
    const MAX_INITIAL_ROWS: usize = 65_536;
    const MAX_INITIAL_TOPIC_BYTES: usize = 1024 * 1024;
    let capacity = capacity.min(MAX_INITIAL_ROWS);
    match kind {
        SystemColumnKind::Topic => AnyBuilder::Utf8(StringBuilder::with_capacity(
            capacity,
            capacity.saturating_mul(64).min(MAX_INITIAL_TOPIC_BYTES),
        )),
        SystemColumnKind::Partition
        | SystemColumnKind::Offset
        | SystemColumnKind::WriteTimestampMs => {
            AnyBuilder::Int64(Int64Builder::with_capacity(capacity))
        }
        SystemColumnKind::MessageIndex => {
            AnyBuilder::UInt64(UInt64Builder::with_capacity(capacity))
        }
    }
}

fn make_exact_system_builder(
    kind: SystemColumnKind,
    capacity: usize,
    topic_bytes: usize,
) -> AnyBuilder {
    match kind {
        SystemColumnKind::Topic => {
            AnyBuilder::Utf8(StringBuilder::with_capacity(capacity, topic_bytes))
        }
        SystemColumnKind::Partition
        | SystemColumnKind::Offset
        | SystemColumnKind::WriteTimestampMs => {
            AnyBuilder::Int64(Int64Builder::with_capacity(capacity))
        }
        SystemColumnKind::MessageIndex => {
            AnyBuilder::UInt64(UInt64Builder::with_capacity(capacity))
        }
    }
}

#[inline]
#[expect(
    clippy::expect_used,
    clippy::unreachable,
    reason = "metadata and builder preconditions are validated once before this per-row hot path"
)]
fn append_system_columns(
    builders: &mut [AnyBuilder],
    data_columns: usize,
    kinds: &[SystemColumnKind],
    meta: &MessageMeta,
    message_index: u64,
) {
    for (builder, kind) in builders[data_columns..].iter_mut().zip(kinds) {
        match (kind, builder) {
            (SystemColumnKind::Topic, AnyBuilder::Utf8(builder)) => {
                builder.append_value(
                    meta.topic
                        .as_deref()
                        .expect("system column preconditions require source topic"),
                );
            }
            (SystemColumnKind::Partition, AnyBuilder::Int64(builder)) => {
                builder.append_value(
                    meta.partition
                        .expect("system column preconditions require source partition"),
                );
            }
            (SystemColumnKind::Offset, AnyBuilder::Int64(builder)) => {
                builder.append_value(
                    meta.offset
                        .expect("system column preconditions require source offset"),
                );
            }
            (SystemColumnKind::MessageIndex, AnyBuilder::UInt64(builder)) => {
                builder.append_value(message_index);
            }
            (SystemColumnKind::WriteTimestampMs, AnyBuilder::Int64(builder)) => {
                builder.append_value(
                    meta.write_timestamp_ms
                        .expect("system column preconditions require source timestamp"),
                );
            }
            _ => unreachable!("system column builder must match its semantic kind"),
        }
    }
}

struct DlqRecord {
    source_message: u32,
    byte_start: u32,
    byte_end: u32,
    reason: DlqReason,
    record_index: u32,
}

struct ArrowStringConsumer<'a>(&'a mut StringBuilder);

impl base64::write::StrConsumer for ArrowStringConsumer<'_> {
    #[expect(
        clippy::expect_used,
        reason = "Arrow StringBuilder implements fmt::Write infallibly"
    )]
    fn consume(&mut self, encoded: &str) {
        fmt::Write::write_str(self.0, encoded)
            .expect("writing UTF-8 base64 into an Arrow string builder cannot fail");
    }
}

fn append_base64(builder: &mut StringBuilder, raw: &[u8]) -> anyhow::Result<()> {
    let mut encoder = base64::write::EncoderStringWriter::from_consumer(
        ArrowStringConsumer(builder),
        &base64::engine::general_purpose::STANDARD,
    );
    encoder.write_all(raw)?;
    let ArrowStringConsumer(builder) = encoder.into_inner();
    // Incremental writes append bytes to the current Arrow value; the next
    // append finalizes its offset without copying the encoded payload.
    builder.append_value("");
    Ok(())
}

#[expect(
    clippy::expect_used,
    reason = "subslice is produced directly by splitting parent"
)]
fn subslice_range(parent: &[u8], subslice: &[u8]) -> core::ops::Range<usize> {
    let parent_start = parent.as_ptr() as usize;
    let child_start = subslice.as_ptr() as usize;
    let start = child_start
        .checked_sub(parent_start)
        .expect("record slice must start inside its source message");
    let end = start
        .checked_add(subslice.len())
        .expect("record slice end overflow");
    assert!(
        end <= parent.len(),
        "record slice must end inside its source message"
    );
    start..end
}

#[inline]
#[expect(
    clippy::expect_used,
    reason = "upstream PQv1 delivery and decoded-size caps are strictly below u32::MAX"
)]
fn dlq_record(
    source_message: usize,
    byte_range: core::ops::Range<usize>,
    reason: DlqReason,
    record_index: u64,
) -> DlqRecord {
    DlqRecord {
        source_message: u32::try_from(source_message)
            .expect("PQv1 delivery message count is bounded far below u32::MAX"),
        byte_start: u32::try_from(byte_range.start)
            .expect("PQv1 decoded message size is bounded below u32::MAX"),
        byte_end: u32::try_from(byte_range.end)
            .expect("PQv1 decoded message size is bounded below u32::MAX"),
        reason,
        record_index: u32::try_from(record_index)
            .expect("PQv1 record count is bounded by decoded bytes below u32::MAX"),
    }
}

// ---------------------------------------------------------------------------
// Two-phase typed field extractor — writes to scratch, not builders
// ---------------------------------------------------------------------------

struct TypedFieldExtractor<'ctx> {
    index: &'ctx ColumnIndex,
    scratch: &'ctx mut [TypedScratch],
    kinds: &'ctx [ColumnKind],
    /// Per-column requiredness (non-nullable), indexed by column position.
    required: &'ctx [bool],
    reject_unknown: bool,
    /// Tracks mapped keys independently from their value because an explicit
    /// JSON null leaves the typed scratch empty.
    seen: &'ctx mut [bool],
    /// Base pointer of the JSON buffer passed to simd-json.
    /// Used to compute byte offsets for string values via pointer arithmetic.
    buf_ptr: *const u8,
    /// How many *required* columns have been filled so far.
    required_filled: usize,
    /// Total number of required columns — a row is valid once all are filled.
    required_total: usize,
    duplicate_mapped_field: bool,
    unknown_field: bool,
}

struct DuplicateMappedRootVisitor<'a> {
    fields: &'a [String],
}

impl<'de> de::Visitor<'de> for DuplicateMappedRootVisitor<'_> {
    type Value = bool;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
        let mut seen = vec![false; self.fields.len()];
        let mut duplicate = false;
        while let Some(key) = map.next_key::<&str>()? {
            if let Some(index) = self.fields.iter().position(|field| field == key) {
                duplicate |= seen[index];
                seen[index] = true;
            }
            map.next_value::<de::IgnoredAny>()?;
        }
        Ok(duplicate)
    }
}

#[expect(clippy::missing_trait_methods, reason = "default impls are sufficient")]
impl<'de, 'ctx> de::Visitor<'de> for &'ctx mut TypedFieldExtractor<'ctx> {
    type Value = bool;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
        while let Some(key) = map.next_key::<&str>()? {
            if let Some(&idx) = self.index.get(key) {
                if self.seen[idx] {
                    self.duplicate_mapped_field = true;
                }
                self.seen[idx] = true;
                let was_empty = matches!(self.scratch[idx], TypedScratch::Empty);
                let seed = TypedValueWriter {
                    target: &mut self.scratch[idx],
                    buf_ptr: self.buf_ptr,
                    kind: self.kinds[idx],
                };
                let present = map.next_value_seed(seed)?;
                if present && was_empty && self.required[idx] {
                    self.required_filled += 1;
                }
            } else {
                self.unknown_field = true;
                map.next_value::<de::IgnoredAny>()?;
            }
        }
        // Row is valid iff every *required* (non-nullable) column was found.
        // Missing nullable columns stay `TypedScratch::Empty` → appended as NULL.
        Ok(
            !(self.duplicate_mapped_field || self.reject_unknown && self.unknown_field)
                && self.required_filled == self.required_total,
        )
    }
}

// ---------------------------------------------------------------------------
// simd-json accelerated parse + zero-copy typed extraction
// ---------------------------------------------------------------------------

fn parse_root_fields_typed(
    bytes: &[u8],
    buf: &mut Vec<u8>,
    info: &RootFieldInfo,
    scratch: &mut [TypedScratch],
    seen: &mut [bool],
    kinds: &[ColumnKind],
) -> anyhow::Result<bool> {
    seen.fill(false);
    buf.clear();
    buf.extend_from_slice(bytes);
    // Snapshot the buffer pointer BEFORE simd-json borrows `buf` mutably.
    // The pointer itself is stable across Vec resizes (and we don't resize after
    // this point), so it remains valid through deserialization.
    let buf_ptr = buf.as_ptr();
    let mut de = simd_json::Deserializer::from_slice(buf).map_err(anyhow::Error::from)?;
    let mut extractor = TypedFieldExtractor {
        index: &info.index,
        scratch,
        kinds,
        required: &info.required,
        reject_unknown: info.reject_unknown,
        seen,
        buf_ptr,
        required_filled: 0,
        required_total: info.required_total,
        duplicate_mapped_field: false,
        unknown_field: false,
    };
    de.deserialize_map(&mut extractor).map_err(Into::into)
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

enum DlqReason {
    JsonParse,
    ExtractionFailed,
}

#[derive(Debug)]
struct RowConversionError(String);

impl core::fmt::Display for RowConversionError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl DlqReason {
    const fn as_str(&self) -> &str {
        match self {
            Self::JsonParse => "JSON parse error",
            Self::ExtractionFailed => "JSONPath extraction failed for one or more columns",
        }
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
    chunk_splitter: ChunkSplitter,
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
    path: CompiledPath,
    /// `true` when the column is non-nullable (a missing value routes the row to DLQ).
    required: bool,
    json_data_type: JsonDataType,
    max_length: Option<usize>,
    time_conversion: Option<TimeConversion>,
}

impl JsonParser {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        let row_counts = messages
            .iter()
            .map(|message| self.chunk_splitter.count_records(&message.value))
            .collect::<Vec<_>>();
        let rows = row_counts
            .iter()
            .fold(0_usize, |total, rows| total.saturating_add(*rows));
        let input_bytes = messages.iter().fold(0_usize, |total, message| {
            total.saturating_add(message.value.len())
        });
        let validity_bytes = rows.div_ceil(8).saturating_add(64);
        let mut main_bytes = 0_usize;
        for kind in &self.kinds {
            main_bytes = main_bytes.saturating_add(match kind {
                ColumnKind::Utf8 => {
                    input_bytes.saturating_add(rows.saturating_add(1).saturating_mul(4))
                }
                ColumnKind::LargeUtf8 => {
                    input_bytes.saturating_add(rows.saturating_add(1).saturating_mul(8))
                }
                ColumnKind::Boolean => rows.div_ceil(8),
                fixed => rows.saturating_mul(fixed.fixed_width_bytes().unwrap_or_default()),
            });
            main_bytes = main_bytes.saturating_add(validity_bytes);
        }
        for kind in &self.system_kinds {
            main_bytes = main_bytes.saturating_add(match kind {
                SystemColumnKind::Topic => rows.saturating_add(1).saturating_mul(4),
                SystemColumnKind::Partition
                | SystemColumnKind::Offset
                | SystemColumnKind::MessageIndex
                | SystemColumnKind::WriteTimestampMs => rows.saturating_mul(8),
            });
        }
        let topic_rows =
            messages
                .iter()
                .zip(&row_counts)
                .fold(0_usize, |total, (message, rows)| {
                    total.saturating_add(
                        message
                            .meta
                            .topic
                            .as_ref()
                            .map_or(0, |topic| topic.len().saturating_mul(*rows)),
                    )
                });
        let main_topic_bytes = if self.system_kinds.contains(&SystemColumnKind::Topic) {
            topic_rows
        } else {
            0
        };
        let dlq_topic_bytes = if self.dlq_system_columns.contains(SystemColumnKind::Topic) {
            topic_rows
        } else {
            0
        };
        main_bytes = main_bytes.saturating_add(main_topic_bytes);
        // Every source byte may instead become a base64 DLQ payload. Account its
        // encoded representation, offsets, error text and fixed timestamp columns.
        let dlq_bytes = input_bytes
            .div_ceil(3)
            .saturating_mul(4)
            .saturating_add(rows.saturating_mul(96))
            .saturating_add(dlq_topic_bytes);
        // `get_array_memory_size` includes array/schema structs in addition to
        // buffers. Keep a small fixed allowance per main/DLQ column so the
        // estimate remains conservative for tiny and empty records.
        let structural_bytes = self
            .kinds
            .len()
            .saturating_add(self.system_kinds.len().saturating_mul(2))
            .saturating_add(3)
            .saturating_mul(256);
        let retained_output_bytes = main_bytes
            .saturating_add(dlq_bytes)
            .saturating_add(structural_bytes)
            .max(1);
        // Arrow builders use growable Vec-backed buffers. Before finish, a
        // growth step can transiently retain both old and new allocations;
        // finished arrays may also retain spare capacity. Reserve a 2x
        // capacity envelope for admission and fail-fast decisions.
        retained_output_bytes.saturating_mul(2)
    }

    fn exceeds_safety_limits(&self, messages: &[Message]) -> bool {
        if self.output_memory_bound(messages) > MAX_DELIVERY_BYTES {
            return true;
        }
        messages.iter().any(|message| match self.chunk_splitter {
            ChunkSplitter::OneMessageOneRow => message.value.len() > MAX_RECORD_BYTES,
            ChunkSplitter::NewLine => message
                .value
                .split(|byte| *byte == b'\n')
                .any(|record| record.len() > MAX_RECORD_BYTES),
        })
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
            let arrow_type = parse_arrow_type(&col.arrow_type)?;
            let kind = ColumnKind::from_data_type(&arrow_type).ok_or_else(|| {
                anyhow::anyhow!(
                    "Column '{}': unsupported Arrow type {:?}",
                    col.column_name,
                    arrow_type
                )
            })?;
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
            && !matches!(config.unknown_fields, UnknownFieldPolicy::Rest { .. })
            && config.conversion_error == ConversionErrorPolicy::Dlq
            && config
                .columns
                .iter()
                .all(|column| column.time_conversion.is_none() && column.max_length.is_none())
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
        if let UnknownFieldPolicy::Rest { column_name } = &config.unknown_fields {
            anyhow::ensure!(
                all_root,
                "unknown_fields.action=rest currently requires only simple top-level JSONPaths"
            );
            fields.push(Field::new(column_name, DataType::Utf8, false));
            data_types.push(DataType::Utf8);
            kinds.push(ColumnKind::Utf8);
            mappings.push(ColumnMappingExt {
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
            let name = config.system_column_names.name(kind);
            if column_names.contains(name) {
                anyhow::bail!("user column '{name}' conflicts with reserved system column");
            }
        }
        for kind in &system_kinds {
            let name = config.system_column_names.name(*kind);
            let field = Field::new(name, kind.data_type(), false);
            schema_fields.push(if config.primary_key.iter().any(|key| key == name) {
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
                    name: Arc::from(config.system_column_names.name(*kind)),
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
                    name: Arc::from(config.system_column_names.name(*kind)),
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
            chunk_splitter: config.chunk_splitter,
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
                &message.meta,
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

    fn check_system_column_preconditions(&self, messages: &[Message]) -> anyhow::Result<()> {
        for msg in messages {
            for kind in &self.system_kinds {
                let present = match kind {
                    SystemColumnKind::Topic => msg.meta.topic.is_some(),
                    SystemColumnKind::Partition => msg.meta.partition.is_some(),
                    SystemColumnKind::Offset => msg.meta.offset.is_some(),
                    SystemColumnKind::MessageIndex => true,
                    SystemColumnKind::WriteTimestampMs => msg.meta.write_timestamp_ms.is_some(),
                };
                anyhow::ensure!(
                    present,
                    "source message is missing metadata required for system column '{}'",
                    self.system_columns.get(*kind).map_or_else(
                        || kind.default_name().to_owned(),
                        |column| column.name.to_string()
                    )
                );
            }
        }
        Ok(())
    }

    /// Appends one successfully parsed typed row and its configured system columns.
    fn append_root_line(
        builders: &mut [AnyBuilder],
        typed_scratch: &[TypedScratch],
        json_buf: &[u8],
        msg: &Message,
        message_index: u64,
        system_kinds: &[SystemColumnKind],
    ) {
        for (builder, s) in builders.iter_mut().zip(typed_scratch.iter()) {
            append_typed(builder, s, json_buf);
        }
        append_system_columns(
            builders,
            typed_scratch.len(),
            system_kinds,
            &msg.meta,
            message_index,
        );
    }

    /// Parses `NewLine` records directly from their source buffers. Keeping
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
    ) {
        for (source_message, msg) in messages.iter().enumerate() {
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
                            msg,
                            record_index as u64,
                            &self.system_kinds,
                        );
                    }
                    Ok(false) => dlq_records.push(dlq_record(
                        source_message,
                        subslice_range(&msg.value, line),
                        DlqReason::ExtractionFailed,
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
    ) {
        for (source_message, msg) in messages.iter().enumerate() {
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
                    msg,
                    0,
                    &self.system_kinds,
                ),
                Ok(false) => dlq_records.push(dlq_record(
                    source_message,
                    0..msg.value.len(),
                    DlqReason::ExtractionFailed,
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
            let mut value = match self.extract_value(json, mapping) {
                Some(value) => value,
                None if !mapping.required => Value::Null,
                None => return Err(RowConversionError("required JSONPath is missing".into())),
            };
            if matches!(mapping.path, CompiledPath::Rest) {
                value = Value::String(
                    serde_json::to_string(&value)
                        .map_err(|error| RowConversionError(error.to_string()))?,
                );
            }
            if (value.is_null() && mapping.required)
                || !json_value_matches(mapping.json_data_type, &value)
            {
                return Err(RowConversionError(
                    "JSON value does not satisfy the declared conversion".into(),
                ));
            }
            if let Some(conversion) = &mapping.time_conversion {
                value = convert_time_value(&value, conversion, kind)?;
            }
            if !value_matches_kind(kind, &value) {
                return Err(RowConversionError(
                    "converted value is outside the declared Arrow type".into(),
                ));
            }
            if mapping.max_length.is_some_and(|limit| {
                value
                    .as_str()
                    .is_some_and(|text| text.chars().count() > limit)
            }) {
                return Err(RowConversionError(
                    "string exceeds configured max_length".into(),
                ));
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

    fn parse_mixed_newline(
        &self,
        messages: &[Message],
        builders: &mut [AnyBuilder],
        dlq_records: &mut Vec<DlqRecord>,
        row: &mut Vec<Value>,
    ) -> anyhow::Result<()> {
        for (source_message, msg) in messages.iter().enumerate() {
            for (message_index, line) in msg
                .value
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .enumerate()
            {
                let message_index = message_index as u64;
                if self.has_duplicate_mapped_root_field(line) {
                    dlq_records.push(dlq_record(
                        source_message,
                        subslice_range(&msg.value, line),
                        DlqReason::ExtractionFailed,
                        message_index,
                    ));
                    continue;
                }
                match serde_json::from_slice::<Value>(line) {
                    Ok(json) => {
                        if let Err(error) = self.fill_row(&json, row) {
                            if self.conversion_error == ConversionErrorPolicy::Fail {
                                return Err(anyhow::Error::msg(error.to_string()));
                            }
                            dlq_records.push(dlq_record(
                                source_message,
                                subslice_range(&msg.value, line),
                                DlqReason::ExtractionFailed,
                                message_index,
                            ));
                        } else {
                            Self::append_mixed_row(builders, row);
                            append_system_columns(
                                builders,
                                self.mappings.len(),
                                &self.system_kinds,
                                &msg.meta,
                                message_index,
                            );
                        }
                    }
                    Err(_e) => {
                        dlq_records.push(dlq_record(
                            source_message,
                            subslice_range(&msg.value, line),
                            DlqReason::JsonParse,
                            message_index,
                        ));
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
            if self.has_duplicate_mapped_root_field(&msg.value) {
                dlq_records.push(dlq_record(
                    source_message,
                    0..msg.value.len(),
                    DlqReason::ExtractionFailed,
                    0,
                ));
                continue;
            }
            match serde_json::from_slice::<Value>(&msg.value) {
                Ok(json) => {
                    if let Err(error) = self.fill_row(&json, row) {
                        if self.conversion_error == ConversionErrorPolicy::Fail {
                            return Err(anyhow::Error::msg(error.to_string()));
                        }
                        dlq_records.push(dlq_record(
                            source_message,
                            0..msg.value.len(),
                            DlqReason::ExtractionFailed,
                            0,
                        ));
                    } else {
                        Self::append_mixed_row(builders, row);
                        append_system_columns(
                            builders,
                            self.mappings.len(),
                            &self.system_kinds,
                            &msg.meta,
                            0,
                        );
                    }
                }
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

    fn parse_mixed(&self, messages: &[Message], ws: &mut ParserWorkspace) -> anyhow::Result<()> {
        let n_cols = self.mappings.len();
        let mut row: Vec<Value> = Vec::with_capacity(n_cols);
        match self.chunk_splitter {
            ChunkSplitter::NewLine => {
                self.parse_mixed_newline(
                    messages,
                    &mut ws.builders,
                    &mut ws.dlq_records,
                    &mut row,
                )?;
            }
            ChunkSplitter::OneMessageOneRow => {
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
// ParserWorkspace — reusable buffers per partition
// ---------------------------------------------------------------------------

pub struct ParserWorkspace {
    builders: Vec<AnyBuilder>,
    typed_scratch: Vec<TypedScratch>,
    typed_seen: Vec<bool>,
    json_buf: Vec<u8>,
    /// Compact references into the current source delivery for failed rows.
    dlq_records: Vec<DlqRecord>,
    /// Reusable arrays buffer (avoids Vec alloc per `finish()` call).
    arrays: Vec<ArrayRef>,
}

impl Default for ParserWorkspace {
    fn default() -> Self {
        Self::new()
    }
}

impl ParserWorkspace {
    const MAX_RETAINED_SCRATCH_BYTES: usize = 1024 * 1024;

    #[must_use]
    pub fn new() -> Self {
        Self {
            builders: Vec::new(),
            typed_scratch: Vec::new(),
            typed_seen: Vec::new(),
            json_buf: Vec::new(),
            dlq_records: Vec::new(),
            arrays: Vec::new(),
        }
    }

    fn release_large_scratch(&mut self) {
        if self.json_buf.capacity() > Self::MAX_RETAINED_SCRATCH_BYTES {
            self.json_buf = Vec::new();
        } else {
            self.json_buf.clear();
        }
        if self.dlq_records.capacity() > Self::MAX_RETAINED_SCRATCH_BYTES / 32 {
            self.dlq_records = Vec::new();
        } else {
            self.dlq_records.clear();
        }
    }
}

// ---------------------------------------------------------------------------
// parse_into — main hot path
// ---------------------------------------------------------------------------

impl JsonParser {
    #[expect(
        clippy::needless_pass_by_value,
        reason = "the parser session consumes source ownership at this API boundary"
    )]
    pub fn parse_into(
        &self,
        messages: Vec<Message>,
        ws: &mut ParserWorkspace,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        self.check_system_column_preconditions(&messages)?;
        if self.exceeds_safety_limits(&messages) {
            anyhow::bail!(
                "JSON parser input exceeds the configured 256MiB delivery or 4MiB record safety limit"
            );
        }

        // Count rows without retaining a second per-record index. Parsing
        // performs the same allocation-free split over the source buffers.
        let n_rows: usize = match self.chunk_splitter {
            ChunkSplitter::NewLine => messages
                .iter()
                .map(|msg| self.chunk_splitter.count_records(&msg.value))
                .sum(),
            ChunkSplitter::OneMessageOneRow => messages.len(),
        };
        let input_bytes = messages.iter().fold(0_usize, |total, message| {
            total.saturating_add(message.value.len())
        });
        let estimated_dlq_rows = if matches!(self.chunk_splitter, ChunkSplitter::NewLine) {
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
                match self.chunk_splitter {
                    ChunkSplitter::NewLine => self.parse_all_root_newline(
                        &messages,
                        info,
                        builders,
                        typed_scratch,
                        typed_seen,
                        json_buf,
                        dlq_records,
                    ),
                    ChunkSplitter::OneMessageOneRow => self.parse_all_root_nosplit(
                        &messages,
                        info,
                        builders,
                        typed_scratch,
                        typed_seen,
                        json_buf,
                        dlq_records,
                    ),
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
}

impl ParserSession for JsonParserSession {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        self.parser.output_memory_bound(messages)
    }

    fn hard_output_limit(&self) -> Option<usize> {
        Some(MAX_DELIVERY_BYTES)
    }

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        self.parser.parse_into(messages, &mut self.workspace)
    }
}

impl ParserFactory for JsonParser {
    fn create_session(self: Arc<Self>) -> Box<dyn ParserSession> {
        Box::new(JsonParserSession {
            parser: self,
            workspace: ParserWorkspace::new(),
        })
    }
}
// Regression tests — validate the simd-json invariant on real inputs
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
