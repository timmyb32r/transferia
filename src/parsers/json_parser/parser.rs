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

use crate::parsers::json_parser::config::{parse_arrow_type, ChunkSplitter, JsonParserConfig};
use crate::parsers::{ParserFactory, ParserSession, SystemColumnsConfig};
use crate::types::message::{Message, MessageMeta};
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
fn str_val(json_buf: &[u8], range: ValidatedStr) -> &str {
    // SAFETY: see doc comment above — ValidatedStr acts as a type-level
    // witness that the byte range has already been UTF-8-validated by simd-json.
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
                builder.append_value(meta.topic.as_deref().unwrap_or_default());
            }
            (SystemColumnKind::Partition, AnyBuilder::Int64(builder)) => {
                builder.append_value(meta.partition.unwrap_or_default());
            }
            (SystemColumnKind::Offset, AnyBuilder::Int64(builder)) => {
                builder.append_value(meta.offset.unwrap_or_default());
            }
            (SystemColumnKind::MessageIndex, AnyBuilder::UInt64(builder)) => {
                builder.append_value(message_index);
            }
            (SystemColumnKind::WriteTimestampMs, AnyBuilder::Int64(builder)) => {
                builder.append_value(meta.write_timestamp_ms.unwrap_or_default());
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
                map.next_value::<de::IgnoredAny>()?;
            }
        }
        // Row is valid iff every *required* (non-nullable) column was found.
        // Missing nullable columns stay `TypedScratch::Empty` → appended as NULL.
        Ok(!self.duplicate_mapped_field && self.required_filled == self.required_total)
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
        seen,
        buf_ptr,
        required_filled: 0,
        required_total: info.required_total,
        duplicate_mapped_field: false,
    };
    de.deserialize_map(&mut extractor).map_err(Into::into)
}

// ---------------------------------------------------------------------------
// DLQ — source metadata uses the same typed system-column contract as main data
// ---------------------------------------------------------------------------
fn dlq_schema(system_kinds: &[SystemColumnKind]) -> Schema {
    let mut fields = vec![
        Field::new("raw_base64", DataType::Utf8, false),
        Field::new("error_message", DataType::Utf8, false),
        Field::new("source_write_timestamp_ms", DataType::Int64, false),
    ];
    for kind in system_kinds {
        fields.push(Field::new(kind.name(), kind.data_type(), false));
    }
    Schema::new(fields)
}

enum DlqReason {
    JsonParse,
    ExtractionFailed,
    ParserLimitExceeded,
}

impl DlqReason {
    const fn as_str(&self) -> &str {
        match self {
            Self::JsonParse => "JSON parse error",
            Self::ExtractionFailed => "JSONPath extraction failed for one or more columns",
            Self::ParserLimitExceeded => {
                "JSON parser safety limit exceeded; source message preserved without parsing"
            }
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
}

struct ColumnMappingExt {
    path: CompiledPath,
    /// `true` when the column is non-nullable (a missing value routes the row to DLQ).
    required: bool,
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
        // capacity envelope for admission/fallback decisions.
        retained_output_bytes.saturating_mul(2)
    }

    fn requires_safe_dlq_fallback(&self, messages: &[Message]) -> bool {
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

    fn preserve_unparsed_delivery(
        &self,
        messages: &[Message],
        ws: &mut ParserWorkspace,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let mut payloads = Vec::with_capacity(messages.len());
        for (source_message, message) in messages.iter().enumerate() {
            payloads.push(dlq_record(
                source_message,
                0..message.value.len(),
                DlqReason::ParserLimitExceeded,
                0,
            ));
        }
        ws.release_large_scratch();
        let main = TableData {
            batch: RecordBatch::new_empty(Arc::clone(&self.arrow_schema)),
            table: Arc::clone(&self.table),
            is_dlq: false,
            system_columns: self.system_columns.clone(),
        };
        let dlq = if payloads.is_empty() {
            None
        } else {
            Some(self.build_dlq_batch(messages, payloads)?)
        };
        Ok((main, dlq))
    }

    pub fn new(
        config: &JsonParserConfig,
        system_config: &SystemColumnsConfig,
        table: Arc<str>,
    ) -> anyhow::Result<Self> {
        if config.columns.is_empty() {
            anyhow::bail!("columns must not be empty");
        }
        let n = config.columns.len();
        let mut mappings = Vec::with_capacity(n);
        let mut kinds = Vec::with_capacity(n);
        let mut data_types = Vec::with_capacity(n);
        let mut all_root = true;
        let mut column_names = HashSet::with_capacity(n);
        let mut mapped_root_fields = Vec::new();

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
                if !mapped_root_fields.contains(field) {
                    mapped_root_fields.push(field.clone());
                }
            }
            kinds.push(kind);
            data_types.push(arrow_type);
            mappings.push(ColumnMappingExt {
                path,
                required: !col.nullable,
            });
        }

        let required: Vec<bool> = config.columns.iter().map(|c| !c.nullable).collect();
        let required_total = required.iter().filter(|r| **r).count();

        let mode = if all_root {
            let pairs: Vec<(String, usize)> = mappings
                .iter()
                .enumerate()
                .filter_map(|(i, m)| match &m.path {
                    CompiledPath::RootField(f) => Some((f.clone(), i)),
                    CompiledPath::Complex(_) => None,
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
                })
            } else {
                // One JSON field may feed multiple output columns. The single-index
                // extractor cannot represent that, so use the general compiled path.
                ParseMode::Mixed
            }
        } else {
            ParseMode::Mixed
        };

        let fields: Vec<Field> = config
            .columns
            .iter()
            .zip(data_types.iter())
            .map(|(col, dt)| Field::new(&col.column_name, dt.clone(), col.nullable))
            .collect();
        let mut schema_fields = fields;
        let system_kinds: Vec<_> = system_config.enabled().collect();
        for kind in [
            SystemColumnKind::Topic,
            SystemColumnKind::Partition,
            SystemColumnKind::Offset,
            SystemColumnKind::MessageIndex,
            SystemColumnKind::WriteTimestampMs,
        ] {
            if config
                .columns
                .iter()
                .any(|column| column.column_name == kind.name())
            {
                anyhow::bail!(
                    "user column '{}' conflicts with reserved system column",
                    kind.name()
                );
            }
        }
        for kind in &system_kinds {
            schema_fields.push(Field::new(kind.name(), kind.data_type(), false));
        }
        let arrow_schema = Arc::new(Schema::new(schema_fields));
        let dlq_table: Arc<str> = dlq_name(&table).into();
        let system_columns = SystemColumns::new(
            system_kinds
                .iter()
                .enumerate()
                .map(|(offset, kind)| SystemColumn {
                    kind: *kind,
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
            source_ts_builder.append_value(message.meta.write_timestamp_ms.unwrap_or_default());
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
        let batch = RecordBatch::try_new(Arc::new(dlq_schema(&self.system_kinds)), arrays)?;

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
                    kind.name()
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
    fn fill_row(&self, json: &Value, row: &mut Vec<Value>) -> bool {
        row.clear();
        for (mapping, kind) in self.mappings.iter().zip(self.kinds.iter().copied()) {
            let value = match self.extract_value(json, mapping) {
                Some(value) => value,
                None if !mapping.required => Value::Null,
                None => return false,
            };
            if (value.is_null() && mapping.required) || !value_matches_kind(kind, &value) {
                return false;
            }
            row.push(value);
        }
        true
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
    ) {
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
                        if self.fill_row(&json, row) {
                            Self::append_mixed_row(builders, row);
                            append_system_columns(
                                builders,
                                self.mappings.len(),
                                &self.system_kinds,
                                &msg.meta,
                                message_index,
                            );
                        } else {
                            dlq_records.push(dlq_record(
                                source_message,
                                subslice_range(&msg.value, line),
                                DlqReason::ExtractionFailed,
                                message_index,
                            ));
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
    }

    fn parse_mixed_nosplit(
        &self,
        messages: &[Message],
        builders: &mut [AnyBuilder],
        dlq_records: &mut Vec<DlqRecord>,
        row: &mut Vec<Value>,
    ) {
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
                    if self.fill_row(&json, row) {
                        Self::append_mixed_row(builders, row);
                        append_system_columns(
                            builders,
                            self.mappings.len(),
                            &self.system_kinds,
                            &msg.meta,
                            0,
                        );
                    } else {
                        dlq_records.push(dlq_record(
                            source_message,
                            0..msg.value.len(),
                            DlqReason::ExtractionFailed,
                            0,
                        ));
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
    }

    fn parse_mixed(&self, messages: &[Message], ws: &mut ParserWorkspace) {
        let n_cols = self.mappings.len();
        let mut row: Vec<Value> = Vec::with_capacity(n_cols);
        match self.chunk_splitter {
            ChunkSplitter::NewLine => {
                self.parse_mixed_newline(messages, &mut ws.builders, &mut ws.dlq_records, &mut row);
            }
            ChunkSplitter::OneMessageOneRow => {
                self.parse_mixed_nosplit(messages, &mut ws.builders, &mut ws.dlq_records, &mut row);
            }
        }
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
        if self.requires_safe_dlq_fallback(&messages) {
            return self.preserve_unparsed_delivery(&messages, ws);
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
                self.parse_mixed(&messages, ws);
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
mod tests {
    use super::*;
    use crate::parsers::json_parser::config::JsonParserConfig;
    use arrow::array::Array as _;
    use base64::Engine as _;
    use bytes::Bytes;

    fn parser_for(
        columns: Vec<crate::parsers::json_parser::ColumnMapping>,
    ) -> anyhow::Result<JsonParser> {
        JsonParser::new(
            &JsonParserConfig {
                columns,
                chunk_splitter: ChunkSplitter::OneMessageOneRow,
            },
            &crate::parsers::SystemColumnsConfig::default(),
            "test".into(),
        )
    }

    #[test]
    fn duplicate_root_path_populates_every_output_column() -> anyhow::Result<()> {
        use crate::parsers::json_parser::ColumnMapping;

        let parser = parser_for(vec![
            ColumnMapping::new("$.id".into(), "left".into(), "Int64".into(), false),
            ColumnMapping::new("$.id".into(), "right".into(), "Int64".into(), true),
        ])?;
        anyhow::ensure!(matches!(parser.mode, ParseMode::Mixed));
        let (main, dlq) = parser.parse_into(
            vec![Message::new(Bytes::from_static(b"{\"id\":7}"))],
            &mut ParserWorkspace::new(),
        )?;
        anyhow::ensure!(dlq.is_none());
        anyhow::ensure!(int64_col(&main.batch, 0)?.value(0) == 7);
        anyhow::ensure!(int64_col(&main.batch, 1)?.value(0) == 7);
        Ok(())
    }

    #[test]
    fn duplicate_mapped_root_key_reaches_dlq_in_fast_and_mixed_modes() -> anyhow::Result<()> {
        use crate::parsers::json_parser::ColumnMapping;

        let fast = parser_for(vec![ColumnMapping::new(
            "$.id".into(),
            "id".into(),
            "Int64".into(),
            false,
        )])?;
        anyhow::ensure!(matches!(fast.mode, ParseMode::AllRootField(_)));

        let mixed = parser_for(vec![
            ColumnMapping::new("$.id".into(), "left".into(), "Int64".into(), false),
            ColumnMapping::new("$.id".into(), "right".into(), "Int64".into(), true),
        ])?;
        anyhow::ensure!(matches!(mixed.mode, ParseMode::Mixed));

        for parser in [&fast, &mixed] {
            let (main, dlq) = parser.parse_into(
                vec![Message::new(Bytes::from_static(b"{\"id\":1,\"id\":null}"))],
                &mut ParserWorkspace::new(),
            )?;
            anyhow::ensure!(main.batch.num_rows() == 0);
            anyhow::ensure!(dlq.is_some_and(|batch| batch.batch.num_rows() == 1));
        }
        Ok(())
    }

    #[test]
    fn invalid_complex_jsonpath_is_rejected_at_startup() {
        use crate::parsers::json_parser::ColumnMapping;

        let error = parser_for(vec![ColumnMapping::new(
            "$.items[".into(),
            "value".into(),
            "Utf8".into(),
            true,
        )])
        .err()
        .expect("invalid JSONPath must fail parser construction");
        assert!(error.to_string().contains("invalid JSONPath"));
    }

    #[test]
    fn invalid_root_jsonpath_is_rejected_at_startup() {
        use crate::parsers::json_parser::ColumnMapping;

        let error = parser_for(vec![ColumnMapping::new(
            "$.".into(),
            "value".into(),
            "Utf8".into(),
            true,
        )])
        .err()
        .expect("invalid root JSONPath must fail parser construction");
        assert!(error.to_string().contains("invalid JSONPath"));
    }

    #[test]
    fn empty_one_message_record_is_sent_to_dlq() -> anyhow::Result<()> {
        use crate::parsers::json_parser::ColumnMapping;

        let parser = parser_for(vec![ColumnMapping::new(
            "$.id".into(),
            "id".into(),
            "Int64".into(),
            false,
        )])?;
        let message = Message::new(Bytes::new());
        let bound = parser.output_memory_bound(core::slice::from_ref(&message));
        let (main, dlq) = parser.parse_into(vec![message], &mut ParserWorkspace::new())?;
        let dlq = dlq.expect("empty JSON must reach DLQ");
        assert_eq!(main.batch.num_rows(), 0);
        assert_eq!(dlq.batch.num_rows(), 1);
        assert!(dlq.batch.get_array_memory_size() <= bound);
        Ok(())
    }

    #[test]
    fn dense_fixed_width_rows_have_a_type_aware_memory_bound() -> anyhow::Result<()> {
        use crate::parsers::json_parser::ColumnMapping;

        const ROWS: usize = 32_769;
        let parser = JsonParser::new(
            &JsonParserConfig {
                columns: (0..8)
                    .map(|index| {
                        ColumnMapping::new(
                            format!("$.c{index}"),
                            format!("c{index}"),
                            "Int64".into(),
                            false,
                        )
                    })
                    .collect(),
                chunk_splitter: ChunkSplitter::NewLine,
            },
            &crate::parsers::SystemColumnsConfig::default(),
            "test".into(),
        )?;
        let row = r#"{"c0":0,"c1":1,"c2":2,"c3":3,"c4":4,"c5":5,"c6":6,"c7":7}"#;
        let payload = Bytes::from(
            core::iter::repeat_n(row, ROWS)
                .collect::<Vec<_>>()
                .join("\n"),
        );
        let message = Message::new(payload);
        let bound = parser.output_memory_bound(core::slice::from_ref(&message));

        assert!(
            bound < 256 * 1024 * 1024,
            "dense primitive rows must not be rejected by a per-cell 1KiB heuristic: {bound}"
        );
        let (main, dlq) = parser.parse_into(vec![message], &mut ParserWorkspace::new())?;
        assert_eq!(main.batch.num_rows(), ROWS);
        assert!(dlq.is_none());
        Ok(())
    }

    #[test]
    fn dense_invalid_newline_rows_use_compact_dlq_descriptors() -> anyhow::Result<()> {
        use crate::parsers::json_parser::ColumnMapping;

        const ROWS: usize = 1_048_577;
        assert!(core::mem::size_of::<DlqRecord>() <= 20);
        let parser = JsonParser::new(
            &JsonParserConfig {
                columns: vec![ColumnMapping::new(
                    "$.id".into(),
                    "id".into(),
                    "Int64".into(),
                    false,
                )],
                chunk_splitter: ChunkSplitter::NewLine,
            },
            &crate::parsers::SystemColumnsConfig {
                message_index: true,
                ..crate::parsers::SystemColumnsConfig::default()
            },
            "test".into(),
        )?;
        let payload = Bytes::from(
            core::iter::repeat_n("x", ROWS)
                .collect::<Vec<_>>()
                .join("\n"),
        );
        assert!(!parser.requires_safe_dlq_fallback(&[Message::new(payload.clone())]));
        let (main, dlq) =
            parser.parse_into(vec![Message::new(payload)], &mut ParserWorkspace::new())?;

        assert_eq!(main.batch.num_rows(), 0);
        let dlq = dlq.expect("invalid records must reach DLQ");
        assert_eq!(dlq.batch.num_rows(), ROWS);
        let raw = string_col(&dlq.batch, 0)?;
        assert_eq!(raw.value(0), "eA==");
        assert_eq!(raw.value(ROWS - 1), "eA==");
        let index = dlq
            .system_columns
            .get(crate::types::system_columns::SystemColumnKind::MessageIndex)
            .expect("message index system column");
        let indexes = dlq
            .batch
            .column(index.index)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("message index values");
        assert_eq!(indexes.value(0), 0);
        assert_eq!(indexes.value(ROWS - 1), (ROWS - 1) as u64);
        Ok(())
    }

    #[test]
    fn oversized_parser_working_set_is_preserved_in_dlq_before_builder_allocation(
    ) -> anyhow::Result<()> {
        let parser = parser_for(
            (0..8)
                .map(|index| {
                    crate::parsers::json_parser::ColumnMapping::new(
                        format!("$.c{index}"),
                        format!("c{index}"),
                        "Utf8".into(),
                        false,
                    )
                })
                .collect(),
        )?;
        // OneMessageOneRow counts this as one row, but every string mapping may
        // retain the full source text. The preflight must reject the working
        // set before any Arrow builder gets that capacity.
        let payload = Bytes::from(vec![b'x'; 32 * 1024 * 1024]);
        let mut workspace = ParserWorkspace::new();
        let (main, dlq) = parser.parse_into(vec![Message::new(payload)], &mut workspace)?;
        assert_eq!(main.batch.num_rows(), 0);
        let dlq = dlq.expect("oversized source record must be preserved in DLQ");
        assert_eq!(dlq.batch.num_rows(), 1);
        let reasons = dlq
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("DLQ reason string");
        assert!(reasons.value(0).contains("safety limit"));
        assert!(workspace.builders.is_empty());
        Ok(())
    }

    #[test]
    fn base64_is_streamed_directly_into_arrow_builder() -> anyhow::Result<()> {
        let raw = vec![0x5a; 2 * 1024 * 1024 + 1];
        let mut builder = StringBuilder::with_capacity(1, 0);
        append_base64(&mut builder, &raw)?;
        let array = builder.finish();
        assert_eq!(array.len(), 1);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.decode(array.value(0))?,
            raw
        );
        Ok(())
    }

    #[test]
    fn dlq_source_timestamp_is_deterministic() -> anyhow::Result<()> {
        use crate::parsers::json_parser::ColumnMapping;
        use arrow::array::Int64Array;

        let parser = parser_for(vec![ColumnMapping::new(
            "$.id".into(),
            "id".into(),
            "Int64".into(),
            false,
        )])?;
        let mut message = Message::new(Bytes::from_static(b"invalid"));
        message.meta.write_timestamp_ms = Some(1_234);
        let (_main, dlq) = parser.parse_into(vec![message], &mut ParserWorkspace::new())?;
        let dlq = dlq.expect("invalid JSON must reach DLQ");
        assert_eq!(
            dlq.batch.schema().field(2).name(),
            "source_write_timestamp_ms"
        );
        let timestamps = dlq
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("DLQ source timestamp must be Int64");
        assert_eq!(timestamps.value(0), 1_234);
        Ok(())
    }

    #[test]
    fn dlq_preserves_non_utf8_payload_as_base64_and_releases_scratch() -> anyhow::Result<()> {
        use crate::parsers::json_parser::ColumnMapping;

        let parser = parser_for(vec![ColumnMapping::new(
            "$.id".into(),
            "id".into(),
            "Int64".into(),
            false,
        )])?;
        let mut workspace = ParserWorkspace::new();
        workspace
            .json_buf
            .reserve(ParserWorkspace::MAX_RETAINED_SCRATCH_BYTES + 1);
        let (_main, dlq) = parser.parse_into(
            vec![Message::new(Bytes::from_static(&[0xff, 0x00]))],
            &mut workspace,
        )?;
        let dlq = dlq.expect("invalid payload must reach DLQ");
        let raw = string_col(&dlq.batch, 0)?.value(0);
        anyhow::ensure!(base64::engine::general_purpose::STANDARD.decode(raw)? == [0xff, 0x00]);
        anyhow::ensure!(dlq.batch.schema().field(0).name() == "raw_base64");
        anyhow::ensure!(workspace.dlq_records.is_empty());
        anyhow::ensure!(
            workspace.json_buf.capacity() <= ParserWorkspace::MAX_RETAINED_SCRATCH_BYTES
        );
        Ok(())
    }

    /// Verifies the core invariant end-to-end: simd-json returns `&str`
    /// values whose bytes exactly match `json_buf[start..end]`.
    ///
    /// If this test fails, the safety comment on `str_val` is WRONG and
    /// the unsafe block is producing garbage (or UB).
    #[test]
    fn str_val_matches_simd_json_output() -> anyhow::Result<()> {
        // "Moscow" and "🚀" as explicit UTF-8 byte sequences
        let json = b"{\"name\":\"Alice\",\"city\":\"Moscow\",\"flag\":\"\xF0\x9F\x9A\x80\"}";

        let kinds = vec![
            ColumnKind::Utf8, // name
            ColumnKind::Utf8, // city
            ColumnKind::Utf8, // flag
        ];

        let idx = ColumnIndex::Small(vec![
            ("name".into(), 0),
            ("city".into(), 1),
            ("flag".into(), 2),
        ]);

        let info = RootFieldInfo {
            index: idx,
            required: vec![true, true, true],
            required_total: 3,
        };

        let mut scratch = vec![TypedScratch::Empty; kinds.len()];
        let mut seen = vec![false; kinds.len()];
        let mut buf = Vec::new();

        let ok = parse_root_fields_typed(json, &mut buf, &info, &mut scratch, &mut seen, &kinds)?;
        anyhow::ensure!(ok, "all fields should be found");

        // buf has been modified by simd-json in-situ parsing.
        // Now verify: json_buf[start..end] is valid UTF-8 AND matches the expected string.
        let expected = ["Alice", "Moscow", "\u{1F680}"];
        for (i, exp) in expected.iter().enumerate() {
            let TypedScratch::Str(range) = scratch[i] else {
                anyhow::bail!("Column {i}: expected Str, got {:?}", scratch[i]);
            };
            let reconstructed = str_val(&buf, range);
            anyhow::ensure!(
                reconstructed == *exp,
                "Column {i}: str_val({}..{}) = {reconstructed:?}, expected {exp:?}",
                range.start,
                range.end,
            );
        }
        Ok(())
    }

    /// Verifies that `str_val` correctly handles strings with escape sequences
    /// (simd-json unescapes them in-situ, so the byte range should contain
    /// the unescaped version).
    #[test]
    fn str_val_with_escapes() -> anyhow::Result<()> {
        // JSON with escape sequences that simd-json will process in-situ
        let json = br#"{"text":"Line1\nLine2\tTabbed"}"#;

        let kinds = vec![ColumnKind::Utf8];
        let idx = ColumnIndex::Small(vec![("text".into(), 0)]);
        let info = RootFieldInfo {
            index: idx,
            required: vec![true],
            required_total: 1,
        };

        let mut scratch = vec![TypedScratch::Empty; 1];
        let mut seen = vec![false; 1];
        let mut buf = Vec::new();

        let ok = parse_root_fields_typed(json, &mut buf, &info, &mut scratch, &mut seen, &kinds)?;
        anyhow::ensure!(ok, "parse should succeed");

        let TypedScratch::Str(range) = scratch[0] else {
            anyhow::bail!("expected Str, got {:?}", scratch[0]);
        };
        let s = str_val(&buf, range);
        // After unescaping: \n -> newline, \t -> tab
        anyhow::ensure!(
            s.contains('\n'),
            "should contain unescaped newline, got {s:?}"
        );
        anyhow::ensure!(s.contains('\t'), "should contain unescaped tab, got {s:?}");
        anyhow::ensure!(!s.contains('\\'), "should not contain backslash, got {s:?}");
        Ok(())
    }

    /// Verifies that `chunk_splitter: new-line` correctly splits multi-line
    /// messages and parses each line as a separate JSON row.
    #[test]
    fn newline_chunk_splitter() -> anyhow::Result<()> {
        use crate::parsers::json_parser::{ChunkSplitter, ColumnMapping};

        let config = JsonParserConfig {
            columns: vec![
                ColumnMapping {
                    jsonpath: "$.id".into(),
                    column_name: "id".into(),
                    arrow_type: "Utf8".into(),
                    nullable: false,
                },
                ColumnMapping {
                    jsonpath: "$.val".into(),
                    column_name: "val".into(),
                    arrow_type: "Int64".into(),
                    nullable: true,
                },
            ],
            chunk_splitter: ChunkSplitter::NewLine,
        };

        let parser = JsonParser::new(
            &config,
            &crate::parsers::SystemColumnsConfig::default(),
            "test".into(),
        )?;
        let mut ws = ParserWorkspace::new();

        // 3 JSONs separated by \n, one empty line
        let payload = b"{\"id\":\"a\",\"val\":1}\n{\"id\":\"b\",\"val\":2}\n\n{\"id\":\"c\"}";
        let msgs = vec![Message::new(Bytes::copy_from_slice(payload))];

        let (good, dlq) = parser.parse_into(msgs, &mut ws)?;

        anyhow::ensure!(
            good.batch.num_rows() == 3,
            "3 valid JSON lines \u{2192} 3 rows"
        );
        anyhow::ensure!(dlq.is_none(), "all 3 lines are valid JSON, no DLQ");

        // Check column values
        let id_col = string_col(&good.batch, 0)?;
        let val_col = int64_col(&good.batch, 1)?;
        anyhow::ensure!(id_col.value(0) == "a");
        anyhow::ensure!(id_col.value(1) == "b");
        anyhow::ensure!(id_col.value(2) == "c");
        anyhow::ensure!(val_col.value(0) == 1);
        anyhow::ensure!(val_col.value(1) == 2);
        anyhow::ensure!(good.batch.column(1).is_null(2));
        Ok(())
    }

    #[test]
    fn materializes_system_columns_on_main_and_dlq() -> anyhow::Result<()> {
        use crate::parsers::json_parser::{ChunkSplitter, ColumnMapping};
        use crate::parsers::SystemColumnsConfig;
        use crate::types::message::MessageMeta;
        use crate::types::system_columns::SystemColumnKind;

        let config = JsonParserConfig {
            columns: vec![ColumnMapping {
                jsonpath: "$.id".into(),
                column_name: "id".into(),
                arrow_type: "Utf8".into(),
                nullable: false,
            }],
            chunk_splitter: ChunkSplitter::NewLine,
        };
        let system = SystemColumnsConfig {
            topic: true,
            partition: true,
            offset: true,
            message_index: true,
            write_timestamp_ms: true,
        };
        let parser = JsonParser::new(&config, &system, "test".into())?;
        let message = Message {
            value: Bytes::from_static(b"{\"id\":\"ok\"}\nnot-json"),
            meta: MessageMeta {
                topic: Some(Arc::from("topic-a")),
                partition: Some(7),
                offset: Some(42),
                write_timestamp_ms: Some(1_234),
            },
        };
        let (good, dlq) = parser.parse_into(vec![message], &mut ParserWorkspace::new())?;
        let offset = good.system_columns.get(SystemColumnKind::Offset).unwrap();
        anyhow::ensure!(int64_col(&good.batch, offset.index)?.value(0) == 42);
        let dlq = dlq.ok_or_else(|| anyhow::anyhow!("invalid row must reach DLQ"))?;
        let index = dlq
            .system_columns
            .get(SystemColumnKind::MessageIndex)
            .unwrap();
        let values = dlq
            .batch
            .column(index.index)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| anyhow::anyhow!("message index has wrong type"))?;
        anyhow::ensure!(values.value(0) == 1);
        Ok(())
    }

    #[test]
    fn null_in_non_nullable_partition_candidate_goes_to_dlq() -> anyhow::Result<()> {
        use crate::parsers::json_parser::{ChunkSplitter, ColumnMapping};

        let config = JsonParserConfig {
            columns: vec![ColumnMapping {
                jsonpath: "$.tenant".into(),
                column_name: "tenant".into(),
                arrow_type: "Utf8".into(),
                nullable: false,
            }],
            chunk_splitter: ChunkSplitter::OneMessageOneRow,
        };
        let parser = JsonParser::new(
            &config,
            &crate::parsers::SystemColumnsConfig::default(),
            "test".into(),
        )?;
        let (main, dlq) = parser.parse_into(
            vec![Message::new(Bytes::from_static(b"{\"tenant\":null}"))],
            &mut ParserWorkspace::new(),
        )?;
        anyhow::ensure!(main.batch.num_rows() == 0);
        anyhow::ensure!(dlq.is_some_and(|batch| batch.batch.num_rows() == 1));
        Ok(())
    }

    #[test]
    fn invalid_types_and_ranges_go_to_dlq_in_root_and_mixed_modes() -> anyhow::Result<()> {
        use crate::parsers::json_parser::{ChunkSplitter, ColumnMapping};

        let cases = [
            ("Int8", "300"),
            ("UInt8", "-1"),
            ("UInt16", "70000"),
            ("Boolean", "\"true\""),
            ("Utf8", "42"),
            ("Float32", "1e39"),
        ];

        for (arrow_type, value) in cases {
            for (jsonpath, payload) in [
                ("$.value", format!("{{\"value\":{value}}}")),
                (
                    "$.nested.value",
                    format!("{{\"nested\":{{\"value\":{value}}}}}"),
                ),
            ] {
                let config = JsonParserConfig {
                    columns: vec![ColumnMapping {
                        jsonpath: jsonpath.into(),
                        column_name: "value".into(),
                        arrow_type: arrow_type.into(),
                        nullable: false,
                    }],
                    chunk_splitter: ChunkSplitter::OneMessageOneRow,
                };
                let parser = JsonParser::new(
                    &config,
                    &crate::parsers::SystemColumnsConfig::default(),
                    "test".into(),
                )?;
                let (main, dlq) = parser.parse_into(
                    vec![Message::new(Bytes::from(payload))],
                    &mut ParserWorkspace::new(),
                )?;
                anyhow::ensure!(
                    main.batch.num_rows() == 0,
                    "{arrow_type} accepted invalid value in {jsonpath}"
                );
                anyhow::ensure!(
                    dlq.is_some_and(|batch| batch.batch.num_rows() == 1),
                    "{arrow_type} invalid value did not reach DLQ in {jsonpath}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn nullable_root_and_mixed_values_accept_null_but_not_wrong_type() -> anyhow::Result<()> {
        use crate::parsers::json_parser::{ChunkSplitter, ColumnMapping};

        for (jsonpath, null_payload, invalid_payload) in [
            (
                "$.value",
                b"{\"value\":null}".as_slice(),
                b"{\"value\":\"bad\"}".as_slice(),
            ),
            (
                "$.nested.value",
                b"{\"nested\":{\"value\":null}}".as_slice(),
                b"{\"nested\":{\"value\":\"bad\"}}".as_slice(),
            ),
        ] {
            let config = JsonParserConfig {
                columns: vec![ColumnMapping {
                    jsonpath: jsonpath.into(),
                    column_name: "value".into(),
                    arrow_type: "Int32".into(),
                    nullable: true,
                }],
                chunk_splitter: ChunkSplitter::OneMessageOneRow,
            };
            let parser = JsonParser::new(
                &config,
                &crate::parsers::SystemColumnsConfig::default(),
                "test".into(),
            )?;
            let messages = vec![
                Message::new(Bytes::copy_from_slice(null_payload)),
                Message::new(Bytes::copy_from_slice(invalid_payload)),
            ];
            let (main, dlq) = parser.parse_into(messages, &mut ParserWorkspace::new())?;
            anyhow::ensure!(main.batch.num_rows() == 1, "{jsonpath}");
            anyhow::ensure!(main.batch.column(0).is_null(0), "{jsonpath}");
            anyhow::ensure!(
                dlq.is_some_and(|batch| batch.batch.num_rows() == 1),
                "{jsonpath}"
            );
        }
        Ok(())
    }

    #[test]
    fn timestamp_timezone_is_preserved_in_record_batch() -> anyhow::Result<()> {
        use crate::parsers::json_parser::{ChunkSplitter, ColumnMapping};

        let config = JsonParserConfig {
            columns: vec![ColumnMapping {
                jsonpath: "$.ts".into(),
                column_name: "ts".into(),
                arrow_type: "Timestamp(Millisecond, UTC)".into(),
                nullable: false,
            }],
            chunk_splitter: ChunkSplitter::OneMessageOneRow,
        };
        let parser = JsonParser::new(
            &config,
            &crate::parsers::SystemColumnsConfig::default(),
            "test".into(),
        )?;
        let (main, dlq) = parser.parse_into(
            vec![Message::new(Bytes::from_static(b"{\"ts\":123}"))],
            &mut ParserWorkspace::new(),
        )?;
        anyhow::ensure!(dlq.is_none());
        anyhow::ensure!(
            main.batch.schema().field(0).data_type()
                == &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        anyhow::ensure!(
            main.batch.column(0).data_type() == main.batch.schema().field(0).data_type()
        );
        Ok(())
    }

    fn string_col(batch: &RecordBatch, idx: usize) -> anyhow::Result<&arrow::array::StringArray> {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| anyhow::anyhow!("column {idx} is not StringArray"))
    }

    fn int64_col(batch: &RecordBatch, idx: usize) -> anyhow::Result<&arrow::array::Int64Array> {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("column {idx} is not Int64Array"))
    }
}
