use alloc::sync::Arc;
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Date64Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, LargeStringBuilder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use bytes::Bytes;
use core::fmt;
use serde::{de, Deserializer};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

use crate::parsers::json_parser::config::{parse_arrow_type, ChunkSplitter, JsonParserConfig};
use crate::parsers::{ParserFactory, ParserSession, SystemColumnsConfig};
use crate::types::message::{Message, MessageMeta};
use crate::types::schema::{DatasetSchema, SchemaColumn};
use crate::types::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use crate::types::table_data::{dlq_name, TableData};

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
}

#[inline]
fn make_builder(
    kind: ColumnKind,
    data_type: &DataType,
    n: usize,
    string_bytes: usize,
) -> AnyBuilder {
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
    match kind {
        SystemColumnKind::TopicName => {
            AnyBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 64))
        }
        SystemColumnKind::PartitionNum
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
            (SystemColumnKind::TopicName, AnyBuilder::Utf8(builder)) => {
                builder.append_value(meta.topic_path.as_deref().unwrap_or_default());
            }
            (SystemColumnKind::PartitionNum, AnyBuilder::Int64(builder)) => {
                builder.append_value(meta.partition_id.unwrap_or_default());
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

struct DlqPayload {
    raw: Bytes,
    reason: DlqReason,
    meta: MessageMeta,
    message_index: u64,
}

#[inline]
fn dlq_payload(raw: Bytes, reason: DlqReason, msg: &Message, message_index: u64) -> DlqPayload {
    DlqPayload {
        raw,
        reason,
        meta: msg.meta.clone(),
        message_index,
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
    /// Base pointer of the JSON buffer passed to simd-json.
    /// Used to compute byte offsets for string values via pointer arithmetic.
    buf_ptr: *const u8,
    /// How many *required* columns have been filled so far.
    required_filled: usize,
    /// Total number of required columns — a row is valid once all are filled.
    required_total: usize,
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
        Ok(self.required_filled == self.required_total)
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
    kinds: &[ColumnKind],
) -> anyhow::Result<bool> {
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
        buf_ptr,
        required_filled: 0,
        required_total: info.required_total,
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

#[must_use]
pub fn sink_dataset_schema(
    mut user_schema: DatasetSchema,
    config: &SystemColumnsConfig,
    keep_system_columns: bool,
) -> DatasetSchema {
    if keep_system_columns {
        user_schema.columns.extend(
            config
                .enabled()
                .map(|kind| SchemaColumn::new(kind.name().to_string(), kind.data_type(), false)),
        );
    }
    user_schema
}

#[must_use]
pub fn dlq_dataset_schema(
    config: &SystemColumnsConfig,
    keep_system_columns: bool,
) -> DatasetSchema {
    let system_kinds: Vec<_> = if keep_system_columns {
        config.enabled().collect()
    } else {
        Vec::new()
    };
    DatasetSchema::new(
        dlq_schema(&system_kinds)
            .fields()
            .iter()
            .map(|field| {
                SchemaColumn::new(
                    field.name().clone(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect(),
    )
}

enum DlqReason {
    JsonParse,
    ExtractionFailed,
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
}

struct ColumnMappingExt {
    path: CompiledPath,
    /// `true` when the column is non-nullable (a missing value routes the row to DLQ).
    required: bool,
}

impl JsonParser {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        const FIXED_BYTES_PER_COLUMN_ROW: usize = 1024;
        let rows = messages.iter().fold(0_usize, |total, message| {
            total.saturating_add(self.chunk_splitter.count_records(&message.value))
        });
        let input_bytes = messages.iter().fold(0_usize, |total, message| {
            total.saturating_add(message.value.len())
        });
        let string_columns = self
            .kinds
            .iter()
            .filter(|kind| matches!(kind, ColumnKind::Utf8 | ColumnKind::LargeUtf8))
            .count();
        let fixed = rows
            .saturating_mul(self.kinds.len().saturating_add(self.system_kinds.len()))
            .saturating_mul(FIXED_BYTES_PER_COLUMN_ROW);
        let strings = input_bytes.saturating_mul(string_columns.saturating_add(4));
        let topics = messages.iter().fold(0_usize, |total, message| {
            total.saturating_add(message.meta.topic_path.as_ref().map_or(0, |topic| {
                topic
                    .len()
                    .saturating_mul(self.chunk_splitter.count_records(&message.value))
            }))
        });
        fixed.saturating_add(strings).saturating_add(topics).max(1)
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
            SystemColumnKind::TopicName,
            SystemColumnKind::PartitionNum,
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

    fn build_dlq_batch(&self, dlq_payloads: &[DlqPayload]) -> anyhow::Result<TableData> {
        let n = dlq_payloads.len();
        let encoded_bytes = dlq_payloads.iter().fold(0_usize, |total, payload| {
            total.saturating_add(payload.raw.len().div_ceil(3).saturating_mul(4))
        });
        let mut raw_builder = StringBuilder::with_capacity(n, encoded_bytes);
        let mut err_builder = StringBuilder::with_capacity(n, n * 64);
        let mut source_ts_builder = Int64Builder::with_capacity(n);

        let mut system_builders: Vec<_> = self
            .system_kinds
            .iter()
            .map(|kind| make_system_builder(*kind, n))
            .collect();

        for payload in dlq_payloads {
            raw_builder
                .append_value(base64::engine::general_purpose::STANDARD.encode(&payload.raw));
            err_builder.append_value(payload.reason.as_str());
            source_ts_builder.append_value(payload.meta.write_timestamp_ms.unwrap_or_default());
            append_system_columns(
                &mut system_builders,
                0,
                &self.system_kinds,
                &payload.meta,
                payload.message_index,
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
                    SystemColumnKind::TopicName => msg.meta.topic_path.is_some(),
                    SystemColumnKind::PartitionNum => msg.meta.partition_id.is_some(),
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

    /// `NewLine` parse over pre-split records (`(line, msg_idx, row_idx)`), avoiding a
    /// second `split_into_records` pass — the records were split once in
    /// `parse_into` to size the builders and are reused here. `msg_idx`
    /// preserves the owning `Message` for per-row source metadata columns.
    fn parse_all_root_newline_records(
        &self,
        records: &[(&[u8], usize, u64)],
        messages: &[Message],
        info: &RootFieldInfo,
        builders: &mut [AnyBuilder],
        typed_scratch: &mut [TypedScratch],
        json_buf: &mut Vec<u8>,
        dlq_payloads: &mut Vec<DlqPayload>,
    ) {
        for &(line, msg_idx, message_index) in records {
            let msg = &messages[msg_idx];
            typed_scratch.fill(TypedScratch::Empty);
            match parse_root_fields_typed(line, json_buf, info, typed_scratch, &self.kinds) {
                Ok(true) => {
                    Self::append_root_line(
                        builders,
                        typed_scratch,
                        json_buf,
                        msg,
                        message_index,
                        &self.system_kinds,
                    );
                }
                Ok(false) => {
                    dlq_payloads.push(dlq_payload(
                        Bytes::copy_from_slice(line),
                        DlqReason::ExtractionFailed,
                        msg,
                        message_index,
                    ));
                }
                Err(_e) => {
                    dlq_payloads.push(dlq_payload(
                        Bytes::copy_from_slice(line),
                        DlqReason::JsonParse,
                        msg,
                        message_index,
                    ));
                }
            }
        }
    }

    fn parse_all_root_nosplit(
        &self,
        messages: Vec<Message>,
        info: &RootFieldInfo,
        builders: &mut [AnyBuilder],
        typed_scratch: &mut [TypedScratch],
        json_buf: &mut Vec<u8>,
        dlq_payloads: &mut Vec<DlqPayload>,
    ) {
        for mut msg in messages {
            typed_scratch.fill(TypedScratch::Empty);
            match parse_root_fields_typed(&msg.value, json_buf, info, typed_scratch, &self.kinds) {
                Ok(true) => Self::append_root_line(
                    builders,
                    typed_scratch,
                    json_buf,
                    &msg,
                    0,
                    &self.system_kinds,
                ),
                Ok(false) => {
                    let raw = core::mem::take(&mut msg.value);
                    dlq_payloads.push(dlq_payload(raw, DlqReason::ExtractionFailed, &msg, 0));
                }
                Err(_e) => {
                    let raw = core::mem::take(&mut msg.value);
                    dlq_payloads.push(dlq_payload(raw, DlqReason::JsonParse, &msg, 0));
                }
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
        messages: Vec<Message>,
        builders: &mut [AnyBuilder],
        dlq_payloads: &mut Vec<DlqPayload>,
        row: &mut Vec<Value>,
    ) {
        for msg in messages {
            for (message_index, line) in self
                .chunk_splitter
                .split_into_records(&msg.value)
                .into_iter()
                .enumerate()
            {
                let message_index = message_index as u64;
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
                            dlq_payloads.push(dlq_payload(
                                Bytes::copy_from_slice(line),
                                DlqReason::ExtractionFailed,
                                &msg,
                                message_index,
                            ));
                        }
                    }
                    Err(_e) => {
                        dlq_payloads.push(dlq_payload(
                            Bytes::copy_from_slice(line),
                            DlqReason::JsonParse,
                            &msg,
                            message_index,
                        ));
                    }
                }
            }
        }
    }

    fn parse_mixed_nosplit(
        &self,
        messages: Vec<Message>,
        builders: &mut [AnyBuilder],
        dlq_payloads: &mut Vec<DlqPayload>,
        row: &mut Vec<Value>,
    ) {
        for mut msg in messages {
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
                        let raw = core::mem::take(&mut msg.value);
                        dlq_payloads.push(dlq_payload(raw, DlqReason::ExtractionFailed, &msg, 0));
                    }
                }
                Err(_e) => {
                    let raw = core::mem::take(&mut msg.value);
                    dlq_payloads.push(dlq_payload(raw, DlqReason::JsonParse, &msg, 0));
                }
            }
        }
    }

    fn parse_mixed(&self, messages: Vec<Message>, ws: &mut ParserWorkspace) {
        let n_cols = self.mappings.len();
        let mut row: Vec<Value> = Vec::with_capacity(n_cols);
        match self.chunk_splitter {
            ChunkSplitter::NewLine => {
                self.parse_mixed_newline(
                    messages,
                    &mut ws.builders,
                    &mut ws.dlq_payloads,
                    &mut row,
                );
            }
            ChunkSplitter::OneMessageOneRow => {
                self.parse_mixed_nosplit(
                    messages,
                    &mut ws.builders,
                    &mut ws.dlq_payloads,
                    &mut row,
                );
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
    json_buf: Vec<u8>,
    /// DLQ rows retain the source metadata of the row that failed parsing.
    dlq_payloads: Vec<DlqPayload>,
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
            json_buf: Vec::new(),
            dlq_payloads: Vec::new(),
            arrays: Vec::new(),
        }
    }

    fn release_large_scratch(&mut self) {
        if self.json_buf.capacity() > Self::MAX_RETAINED_SCRATCH_BYTES {
            self.json_buf = Vec::new();
        } else {
            self.json_buf.clear();
        }
        if self.dlq_payloads.capacity() > Self::MAX_RETAINED_SCRATCH_BYTES / 64 {
            self.dlq_payloads = Vec::new();
        } else {
            self.dlq_payloads.clear();
        }
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
        self.check_system_column_preconditions(&messages)?;

        // Pre-split once for AllRootField+NewLine — the result sizes the
        // builders AND is reused for parsing (no second `split_into_records`
        // pass on the hot path). Mixed+NewLine keeps the alloc-free
        // `count_records` because `parse_mixed` re-splits (records not reused).
        // OneMessageOneRow: one record per message, no split needed.
        let newline_records: Option<Vec<(&[u8], usize, u64)>> =
            match (&self.mode, self.chunk_splitter) {
                (ParseMode::AllRootField(_), ChunkSplitter::NewLine) => {
                    let mut recs: Vec<(&[u8], usize, u64)> = Vec::new();
                    for (i, msg) in messages.iter().enumerate() {
                        for (message_index, line) in self
                            .chunk_splitter
                            .split_into_records(&msg.value)
                            .into_iter()
                            .enumerate()
                        {
                            recs.push((line, i, message_index as u64));
                        }
                    }
                    Some(recs)
                }
                _ => None,
            };
        let n_rows: usize = match (&self.mode, self.chunk_splitter) {
            (ParseMode::Mixed, ChunkSplitter::NewLine) => messages
                .iter()
                .map(|msg| self.chunk_splitter.count_records(&msg.value))
                .sum(),
            _ => newline_records.as_ref().map_or(messages.len(), Vec::len),
        };
        let input_bytes = messages.iter().fold(0_usize, |total, message| {
            total.saturating_add(message.value.len())
        });
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

        ws.dlq_payloads.clear();

        match &self.mode {
            ParseMode::AllRootField(info) => {
                let n_cols = info.index.len();
                let ParserWorkspace {
                    builders,
                    typed_scratch,
                    json_buf,
                    dlq_payloads,
                    ..
                } = ws;
                typed_scratch.clear();
                typed_scratch.resize_with(n_cols, || TypedScratch::Empty);
                match newline_records {
                    Some(recs) => self.parse_all_root_newline_records(
                        &recs,
                        &messages,
                        info,
                        builders,
                        typed_scratch,
                        json_buf,
                        dlq_payloads,
                    ),
                    None => self.parse_all_root_nosplit(
                        messages,
                        info,
                        builders,
                        typed_scratch,
                        json_buf,
                        dlq_payloads,
                    ),
                }
            }
            ParseMode::Mixed => {
                // Mixed does its own split; release the pre-split borrow (if any)
                // before `messages` is moved into `parse_mixed`.
                drop(newline_records);
                self.parse_mixed(messages, ws);
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

        let dlq_payloads = core::mem::take(&mut ws.dlq_payloads);
        let dlq_batch = if dlq_payloads.is_empty() {
            None
        } else {
            Some(self.build_dlq_batch(&dlq_payloads)?)
        };
        drop(dlq_payloads);
        ws.release_large_scratch();

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
        anyhow::ensure!(workspace.dlq_payloads.is_empty());
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
        let mut buf = Vec::new();

        let ok = parse_root_fields_typed(json, &mut buf, &info, &mut scratch, &kinds)?;
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
        let mut buf = Vec::new();

        let ok = parse_root_fields_typed(json, &mut buf, &info, &mut scratch, &kinds)?;
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
            topic_name: true,
            partition_num: true,
            offset: true,
            message_index: true,
            write_timestamp_ms: true,
        };
        let parser = JsonParser::new(&config, &system, "test".into())?;
        let message = Message {
            value: Bytes::from_static(b"{\"id\":\"ok\"}\nnot-json"),
            meta: MessageMeta {
                topic_path: Some(Arc::from("topic-a")),
                partition_id: Some(7),
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

    #[test]
    fn sink_schemas_follow_system_column_visibility() {
        let system_config = crate::parsers::SystemColumnsConfig {
            offset: true,
            message_index: true,
            ..crate::parsers::SystemColumnsConfig::default()
        };
        let user_schema = DatasetSchema::new(vec![SchemaColumn::new(
            "value".into(),
            DataType::Int64,
            false,
        )]);

        let hidden = sink_dataset_schema(user_schema.clone(), &system_config, false);
        assert_eq!(
            hidden
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["value"]
        );
        let visible = sink_dataset_schema(user_schema, &system_config, true);
        assert_eq!(
            visible
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["value", "_system_offset", "_system_message_index"]
        );
        assert_eq!(dlq_dataset_schema(&system_config, false).columns.len(), 3);
        assert_eq!(dlq_dataset_schema(&system_config, true).columns.len(), 5);
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
