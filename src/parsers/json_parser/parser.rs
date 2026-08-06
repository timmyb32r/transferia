use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Date64Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    LargeStringBuilder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use serde::{de, Deserializer};
use serde_json::Value;
use std::collections::HashMap;
use core::fmt;
use alloc::sync::Arc;
use time::format_description::well_known::Rfc3339;

use crate::config::yaml::{parse_arrow_type, ChunkSplitter, SchemaConfig};
use crate::parsers::json_parser::config::JsonParserConfig;
use crate::parsers::Parser;
use crate::types::exactly_once::{ExactlyOnceKey, PartitionKey};
use crate::types::message::Message;
use crate::types::table_data::{dlq_name, TableData};

// ---------------------------------------------------------------------------
// Compiled JSONPath
// ---------------------------------------------------------------------------

enum CompiledPath {
    RootField(String),
    Complex(String),
}

fn compile_path(raw: &str) -> CompiledPath {
    if let Some(field) = raw.strip_prefix("$.") {
        if !field.contains('.') && !field.contains('[') && !field.contains('*') && !field.contains('$') {
            return CompiledPath::RootField(field.to_string());
        }
    }
    CompiledPath::Complex(raw.to_string())
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
    Utf8, LargeUtf8,
    Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
    Float32, Float64,
    Boolean,
    Date32, Date64,
    TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
}

impl ColumnKind {
    const fn from_data_type(dt: &DataType) -> Option<Self> {
        Some(match dt {
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Int8 => Self::Int8, DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32, DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8, DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32, DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32, DataType::Float64 => Self::Float64,
            DataType::Boolean => Self::Boolean,
            DataType::Date32 => Self::Date32, DataType::Date64 => Self::Date64,
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

/// Fallback value appended for non-numeric JSON values in float columns.
const FALLBACK_F64: f64 = 0.0;

#[inline]
fn make_builder(kind: ColumnKind, n: usize) -> AnyBuilder {
    const STR_BYTES_PER_ROW: usize = 128;
    match kind {
        ColumnKind::Utf8 => AnyBuilder::Utf8(StringBuilder::with_capacity(n, n * STR_BYTES_PER_ROW)),
        ColumnKind::LargeUtf8 => AnyBuilder::LargeUtf8(LargeStringBuilder::with_capacity(n, n * STR_BYTES_PER_ROW)),
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
        ColumnKind::TimestampMillisecond => AnyBuilder::TimestampMillisecond(TimestampMillisecondBuilder::with_capacity(n)),
        ColumnKind::TimestampMicrosecond => AnyBuilder::TimestampMicrosecond(TimestampMicrosecondBuilder::with_capacity(n)),
        ColumnKind::TimestampNanosecond => AnyBuilder::TimestampNanosecond(TimestampNanosecondBuilder::with_capacity(n)),
        ColumnKind::TimestampSecond => AnyBuilder::TimestampSecond(TimestampSecondBuilder::with_capacity(n)),
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
fn append_value(builder: &mut AnyBuilder, val: &Value) {
    if val.is_null() {
        append_null(builder);
        return;
    }
    match builder {
        AnyBuilder::Utf8(b) => match val.as_str() {
            Some(s) => b.append_value(s),
            None => b.append_value(val.to_string()),
        },
        AnyBuilder::LargeUtf8(b) => match val.as_str() {
            Some(s) => b.append_value(s),
            None => b.append_value(val.to_string()),
        },
        AnyBuilder::Int64(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::Int32(b) => b.append_value(val.as_i64().unwrap_or(0) as i32),
        AnyBuilder::Int16(b) => b.append_value(val.as_i64().unwrap_or(0) as i16),
        AnyBuilder::Int8(b) => b.append_value(val.as_i64().unwrap_or(0) as i8),
        AnyBuilder::UInt64(b) => b.append_value(val.as_u64().unwrap_or(0)),
        AnyBuilder::UInt32(b) => b.append_value(val.as_u64().unwrap_or(0) as u32),
        AnyBuilder::UInt16(b) => b.append_value(val.as_u64().unwrap_or(0) as u16),
        AnyBuilder::UInt8(b) => b.append_value(val.as_u64().unwrap_or(0) as u8),
        AnyBuilder::Float64(b) => b.append_value(val.as_f64().unwrap_or(FALLBACK_F64)),
        AnyBuilder::Float32(b) => b.append_value(val.as_f64().unwrap_or(FALLBACK_F64) as f32),
        AnyBuilder::Boolean(b) => b.append_value(val.as_bool().unwrap_or(false)),
        AnyBuilder::TimestampMillisecond(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::TimestampMicrosecond(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::TimestampNanosecond(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::TimestampSecond(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::Date32(b) => b.append_value(val.as_i64().unwrap_or(0) as i32),
        AnyBuilder::Date64(b) => b.append_value(val.as_i64().unwrap_or(0)),
    }
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
        Self { start, end: start + s.len() }
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
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        use serde::Deserialize as _;
        match self.kind {
            ColumnKind::Utf8 | ColumnKind::LargeUtf8 => {
                let s = <&str>::deserialize(deserializer)?;
                // ValidatedStr captures the byte range of s within the simd-json
                // buffer. Because `s` is an `&str`, it is valid UTF-8 by definition
                // — simd-json already validated it. The pointer arithmetic gives us
                // the exact byte range without a manual position counter.
                *self.target = TypedScratch::Str(ValidatedStr::from_simd_json_str(s, self.buf_ptr));
            }
            ColumnKind::Int32 | ColumnKind::Date32 => {
                *self.target = TypedScratch::I64(i64::from(i32::deserialize(deserializer)?));
            }
            ColumnKind::Int16 => {
                *self.target = TypedScratch::I64(i64::from(i16::deserialize(deserializer)?));
            }
            ColumnKind::Int8 => {
                *self.target = TypedScratch::I64(i64::from(i8::deserialize(deserializer)?));
            }
            ColumnKind::UInt64 => {
                *self.target = TypedScratch::U64(u64::deserialize(deserializer)?);
            }
            ColumnKind::UInt32 => {
                *self.target = TypedScratch::U64(u64::from(u32::deserialize(deserializer)?));
            }
            ColumnKind::UInt16 => {
                *self.target = TypedScratch::U64(u64::from(u16::deserialize(deserializer)?));
            }
            ColumnKind::UInt8 => {
                *self.target = TypedScratch::U64(u64::from(u8::deserialize(deserializer)?));
            }
            ColumnKind::Float64 => {
                *self.target = TypedScratch::F64(f64::deserialize(deserializer)?);
            }
            ColumnKind::Float32 => {
                *self.target = TypedScratch::F64(f64::from(f32::deserialize(deserializer)?));
            }
            ColumnKind::Boolean => {
                *self.target = TypedScratch::Bool(bool::deserialize(deserializer)?);
            }
            ColumnKind::Int64
            | ColumnKind::Date64
            | ColumnKind::TimestampMillisecond
            | ColumnKind::TimestampMicrosecond
            | ColumnKind::TimestampNanosecond
            | ColumnKind::TimestampSecond => {
                *self.target = TypedScratch::I64(i64::deserialize(deserializer)?);
            }
        }
        Ok(())
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
        AnyBuilder::Utf8(x) => x.append_null(), AnyBuilder::LargeUtf8(x) => x.append_null(),
        AnyBuilder::Int64(x) => x.append_null(), AnyBuilder::Int32(x) => x.append_null(),
        AnyBuilder::Int16(x) => x.append_null(), AnyBuilder::Int8(x) => x.append_null(),
        AnyBuilder::UInt64(x) => x.append_null(), AnyBuilder::UInt32(x) => x.append_null(),
        AnyBuilder::UInt16(x) => x.append_null(), AnyBuilder::UInt8(x) => x.append_null(),
        AnyBuilder::Float64(x) => x.append_null(), AnyBuilder::Float32(x) => x.append_null(),
        AnyBuilder::Boolean(x) => x.append_null(),
        AnyBuilder::Date32(x) => x.append_null(), AnyBuilder::Date64(x) => x.append_null(),
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

/// Appends a `PartitionKey` value into the partition column builder
/// (`AnyBuilder::Int64` for YDS, `AnyBuilder::Utf8` for S3).
#[inline]
fn append_partition_value(builder: &mut AnyBuilder, pk: &PartitionKey) {
    match builder {
        AnyBuilder::Int64(b) => match pk {
            PartitionKey::Int(v) => b.append_value(*v),
            PartitionKey::Str(_) => b.append_null(),
        },
        AnyBuilder::Utf8(b) => match pk {
            PartitionKey::Str(v) => b.append_value(v),
            PartitionKey::Int(_) => b.append_null(),
        },
        other @ (AnyBuilder::LargeUtf8(_)
            | AnyBuilder::Int8(_)
            | AnyBuilder::Int16(_)
            | AnyBuilder::Int32(_)
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
            | AnyBuilder::TimestampNanosecond(_)) => append_null(other),
    }
}

/// Appends the exactly-once key columns for one successfully parsed row.
/// The two key builders are always the **last two** entries of `builders`:
/// `partition` (const per Message) at `len-2`, `offset` (per-row) at `len-1`.
/// Only called when `exactly_once_key` is `Some`; DLQ rows never reach this
/// path (they carry their own key columns in the DLQ batch — spec §3.1).
#[inline]
fn append_key_columns(builders: &mut [AnyBuilder], msg: &Message) {
    let n = builders.len();
    append_partition_value(&mut builders[n - 2], msg.partition.as_ref().unwrap_or(&PartitionKey::Int(0)));
    match &mut builders[n - 1] {
        AnyBuilder::Int64(b) => b.append_value(msg.offset.unwrap_or(0)),
        other @ (AnyBuilder::Utf8(_)
            | AnyBuilder::LargeUtf8(_)
            | AnyBuilder::Int8(_)
            | AnyBuilder::Int16(_)
            | AnyBuilder::Int32(_)
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
            | AnyBuilder::TimestampNanosecond(_)) => append_null(other),
    }
}

/// DLQ payload record: raw bytes + reason + the source position
/// (`offset`, `partition`) of the Message that produced it (spec §7).
/// In at-least-once mode the position is unused (DLQ uses `partition_id`).
#[inline]
fn dlq_tuple(raw: Bytes, reason: DlqReason, msg: &Message) -> (Bytes, DlqReason, i64, PartitionKey) {
    (
        raw,
        reason,
        msg.offset.unwrap_or(0),
        msg.partition.clone().unwrap_or(PartitionKey::Int(0)),
    )
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
                map.next_value_seed(seed)?;
                if was_empty && self.required[idx] {
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
// DLQ — dynamic schema derived from the exactly-once key (spec §7)
// ---------------------------------------------------------------------------

/// Partition column type of the exactly-once key: Int64 for YDS, Utf8 for S3.
///
/// The key descriptor carries only column names (spec §2 defaults:
/// YDS = `__system_partition`, S3 = `__system_filename`); the partition
/// type follows that source convention.
/// `true` when the partition column of the exactly-once key is `Utf8`
/// (S3 `__system_filename`); `false` → Int64 (YDS `__system_partition`).
fn partition_is_utf8(key: &ExactlyOnceKey) -> bool {
    key.partition.name.as_ref() == "__system_filename"
}

fn partition_data_type(key: &ExactlyOnceKey) -> DataType {
    if partition_is_utf8(key) {
        DataType::Utf8
    } else {
        DataType::Int64
    }
}

/// Arrow schema of the DLQ table — **dynamic**, built from the parser's
/// `ExactlyOnceKey` (spec §7; not a static `LazyLock` anymore).
///
/// - `None` (at-least-once): `raw_bytes, error_message, partition_id, timestamp`.
/// - `Some` (exactly-once): `partition_id` is replaced by the key columns
///   (non-nullable): partition (`__system_partition Int64` / `__system_filename Utf8`)
///   + offset (`__system_offset Int64`).
fn dlq_schema(exactly_once: Option<&ExactlyOnceKey>) -> Schema {
    let mut fields = vec![
        Field::new("raw_bytes", DataType::Utf8, false),
        Field::new("error_message", DataType::Utf8, false),
    ];
    match exactly_once {
        Some(key) => {
            fields.push(Field::new("timestamp", DataType::Utf8, false));
            fields.push(Field::new(key.partition.name.as_ref(), partition_data_type(key), false));
            fields.push(Field::new(key.offset.name.as_ref(), DataType::Int64, false));
        }
        None => {
            fields.push(Field::new("partition_id", DataType::Int64, false));
            fields.push(Field::new("timestamp", DataType::Utf8, false));
        }
    }
    Schema::new(fields)
}

/// `ClickHouse` column definitions for the DLQ table, kept in sync with [`dlq_schema`].
/// Used by `create_tables` to create the DLQ table.
#[must_use]
pub fn dlq_ch_columns(exactly_once: Option<&ExactlyOnceKey>) -> Vec<(&str, &str)> {
    let mut cols = vec![
        ("raw_bytes", "String"),
        ("error_message", "String"),
    ];
    match exactly_once {
        Some(key) => {
            cols.push(("timestamp", "String"));
            cols.push((
                key.partition.name.as_ref(),
                if partition_is_utf8(key) { "String" } else { "Int64" },
            ));
            cols.push((key.offset.name.as_ref(), "Int64"));
        }
        None => {
            cols.push(("partition_id", "Int64"));
            cols.push(("timestamp", "String"));
        }
    }
    cols
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
    _data_types: Vec<DataType>,
    /// How to split incoming message bytes into individual JSON objects.
    chunk_splitter: ChunkSplitter,
    /// Exactly-once key descriptor. `None` → at-least-once.
    /// When `Some`, `arrow_schema` is extended with the two key columns and the
    /// DLQ schema is derived from it (spec §7). Must match the `exactly_once_key`
    /// passed to `parse_into` (they originate from the same source config).
    exactly_once_key: Option<ExactlyOnceKey>,
    /// Stored config for DDL / schema access.
    config: JsonParserConfig,
}

struct ColumnMappingExt {
    path: CompiledPath,
    /// `true` when the column is non-nullable (a missing value routes the row to DLQ).
    required: bool,
}

impl JsonParser {
    #[must_use]
    pub fn schema_config(&self) -> SchemaConfig {
        self.config.to_schema_config()
    }

    #[must_use]
    pub fn order_by(&self) -> &[String] {
        &self.config.order_by
    }

    pub fn new(
        config: &JsonParserConfig,
        table: Arc<str>,
        exactly_once_key: Option<ExactlyOnceKey>,
    ) -> anyhow::Result<Self> {
        if config.columns.is_empty() {
            anyhow::bail!("columns must not be empty");
        }
        let n = config.columns.len();
        let mut mappings = Vec::with_capacity(n);
        let mut kinds = Vec::with_capacity(n);
        let mut data_types = Vec::with_capacity(n);
        let mut all_root = true;

        for col in &config.columns {
            let arrow_type = parse_arrow_type(&col.arrow_type)?;
            let kind = ColumnKind::from_data_type(&arrow_type)
                .ok_or_else(|| anyhow::anyhow!(
                    "Column '{}': unsupported Arrow type {:?}", col.column_name, arrow_type
                ))?;
            let path = compile_path(&col.jsonpath);
            if matches!(&path, CompiledPath::Complex(_)) {
                all_root = false;
            }
            kinds.push(kind);
            data_types.push(arrow_type);
            mappings.push(ColumnMappingExt { path, required: !col.nullable });
        }

        let required: Vec<bool> = config.columns.iter().map(|c| !c.nullable).collect();
        let required_total = required.iter().filter(|r| **r).count();

        let mode = if all_root {
            let pairs: Vec<(String, usize)> = mappings.iter()
                .enumerate()
                .filter_map(|(i, m)| match &m.path {
                    CompiledPath::RootField(f) => Some((f.clone(), i)),
                    CompiledPath::Complex(_) => None,
                })
                .collect();
            // Adaptive: linear scan for ≤12 cols (no hash overhead), HashMap for more
            let index = if n <= 12 {
                ColumnIndex::Small(pairs)
            } else {
                ColumnIndex::Large(pairs.into_iter().collect())
            };
            ParseMode::AllRootField(RootFieldInfo { index, required, required_total })
        } else {
            ParseMode::Mixed
        };

        let fields: Vec<Field> = config.columns.iter().zip(data_types.iter())
            .map(|(col, dt)| Field::new(&col.column_name, dt.clone(), true))
            .collect();
        // Exactly-once (spec §3.1): extend the schema with the two key columns
        // at the end — partition (type by source) + offset (non-nullable Int64).
        let mut schema_fields = fields;
        if let Some(key) = &exactly_once_key {
            schema_fields.push(Field::new(key.partition.name.as_ref(), partition_data_type(key), false));
            schema_fields.push(Field::new(key.offset.name.as_ref(), DataType::Int64, false));
        }
        let arrow_schema = Arc::new(Schema::new(schema_fields));
        let dlq_table: Arc<str> = dlq_name(&table).into();

        Ok(Self { mappings, kinds, arrow_schema, mode, table, dlq_table, _data_types: data_types, chunk_splitter: config.chunk_splitter, exactly_once_key, config: config.clone() })
    }

    #[inline]
    fn extract_value(&self, json: &Value, mapping: &ColumnMappingExt) -> Option<Value> {
        match &mapping.path {
            CompiledPath::RootField(field) => json.get(field).cloned(),
            CompiledPath::Complex(path) => {
                jsonpath_lib::select(json, path).ok()
                    .and_then(|r| r.first().map(|v| (*v).clone()))
            }
        }
    }

    fn build_dlq_batch(
        &self,
        dlq_payloads: &[(Bytes, DlqReason, i64, PartitionKey)],
        partition_id: i64,
        now: time::OffsetDateTime,
    ) -> anyhow::Result<TableData> {
        let n = dlq_payloads.len();
        let mut raw_builder = StringBuilder::with_capacity(n, n * 64);
        let mut err_builder = StringBuilder::with_capacity(n, n * 64);
        let mut ts_builder = StringBuilder::with_capacity(n, n * 32);
        let ts = now.format(&Rfc3339)
            .map_err(|e| anyhow::anyhow!("time format: {e}"))?;

        let key = self.exactly_once_key.as_ref();
        // Exactly-once: the DLQ's own key columns (partition + offset per payload).
        // At-least-once: the legacy `partition_id` (per-batch constant).
        let mut partition_builder: Option<AnyBuilder> = key.map(|k| {
            if partition_is_utf8(k) {
                AnyBuilder::Utf8(StringBuilder::with_capacity(n, n * 64))
            } else {
                AnyBuilder::Int64(Int64Builder::with_capacity(n))
            }
        });
        let mut offset_builder = Int64Builder::with_capacity(n);
        let mut pid_builder = Int64Builder::with_capacity(n);

        for (raw_bytes, reason, offset, partition) in dlq_payloads {
            raw_builder.append_value(&String::from_utf8_lossy(raw_bytes));
            err_builder.append_value(reason.as_str());
            ts_builder.append_value(&ts);
            match &mut partition_builder {
                Some(b) => {
                    append_partition_value(b, partition);
                    offset_builder.append_value(*offset);
                }
                None => pid_builder.append_value(partition_id),
            }
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(5);
        arrays.push(Arc::new(raw_builder.finish()));
        arrays.push(Arc::new(err_builder.finish()));
        match &mut partition_builder {
            Some(b) => {
                // raw_bytes, error_message, timestamp, partition, offset
                arrays.push(Arc::new(ts_builder.finish()));
                arrays.push(b.finish());
                arrays.push(Arc::new(offset_builder.finish()));
            }
            None => {
                // raw_bytes, error_message, partition_id, timestamp
                arrays.push(Arc::new(pid_builder.finish()));
                arrays.push(Arc::new(ts_builder.finish()));
            }
        }
        let batch = RecordBatch::try_new(Arc::new(dlq_schema(key)), arrays)?;

        Ok(TableData {
            batch,
            table: Arc::clone(&self.dlq_table),
            is_dlq: true,
            batch_id: crate::batch_id(),
            // DLQ is deduplicated with the same key as main (spec §7).
            exactly_once_key: self.exactly_once_key.clone(),
        })
    }

    /// Exactly-once preconditions (fatal before batch construction, §3.1):
    /// every Message must carry offset+partition, and no user data column may
    /// collide with the system column names.
    fn check_exactly_once_preconditions(&self, messages: &[Message]) -> anyhow::Result<()> {
        // System column names must not collide with user data columns.
        const SYSTEM_COLUMNS: [&str; 3] = ["__system_partition", "__system_offset", "__system_filename"];
        for msg in messages {
            if msg.offset.is_none() {
                anyhow::bail!(
                    "exactly-once requires Message.offset to be set (got None); \
                     the source must fill offset when exactly_once is enabled"
                );
            }
            if msg.partition.is_none() {
                anyhow::bail!(
                    "exactly-once requires Message.partition to be set (got None); \
                     the source must fill partition when exactly_once is enabled"
                );
            }
        }
        let data_names: Vec<&str> = self.arrow_schema.fields().iter()
            .take(self.mappings.len())
            .map(|f| f.name().as_str())
            .collect();
        for sys in SYSTEM_COLUMNS {
            if data_names.contains(&sys) {
                anyhow::bail!(
                    "Column '{sys}' conflicts with a user data field; rename the field or disable exactly_once"
                );
            }
        }
        Ok(())
    }

    /// Appends one successfully parsed typed row into `builders` (plus the
    /// exactly-once key columns when enabled).
    fn append_root_line(
        builders: &mut [AnyBuilder],
        typed_scratch: &[TypedScratch],
        json_buf: &[u8],
        msg: &Message,
        exactly_once_key: Option<&ExactlyOnceKey>,
    ) {
        for (builder, s) in builders.iter_mut().zip(typed_scratch.iter()) {
            append_typed(builder, s, json_buf);
        }
        if exactly_once_key.is_some() {
            append_key_columns(builders, msg);
        }
    }

    fn parse_all_root_newline(
        &self,
        messages: Vec<Message>,
        info: &RootFieldInfo,
        builders: &mut [AnyBuilder],
        typed_scratch: &mut [TypedScratch],
        json_buf: &mut Vec<u8>,
        dlq_payloads: &mut Vec<(Bytes, DlqReason, i64, PartitionKey)>,
        exactly_once_key: Option<&ExactlyOnceKey>,
    ) {
        for msg in messages {
            for line in self.chunk_splitter.split_into_records(&msg.value) {
                typed_scratch.fill(TypedScratch::Empty);
                match parse_root_fields_typed(line, json_buf, info, typed_scratch, &self.kinds) {
                    Ok(true) => Self::append_root_line(builders, typed_scratch, json_buf, &msg, exactly_once_key),
                    Ok(false) => {
                        dlq_payloads.push(dlq_tuple(Bytes::copy_from_slice(line), DlqReason::ExtractionFailed, &msg));
                    }
                    Err(_e) => {
                        dlq_payloads.push(dlq_tuple(Bytes::copy_from_slice(line), DlqReason::JsonParse, &msg));
                    }
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
        dlq_payloads: &mut Vec<(Bytes, DlqReason, i64, PartitionKey)>,
        exactly_once_key: Option<&ExactlyOnceKey>,
    ) {
        for mut msg in messages {
            typed_scratch.fill(TypedScratch::Empty);
            match parse_root_fields_typed(&msg.value, json_buf, info, typed_scratch, &self.kinds) {
                Ok(true) => Self::append_root_line(builders, typed_scratch, json_buf, &msg, exactly_once_key),
                Ok(false) => {
                    dlq_payloads.push(dlq_tuple(core::mem::take(&mut msg.value), DlqReason::ExtractionFailed, &msg));
                }
                Err(_e) => {
                    dlq_payloads.push(dlq_tuple(core::mem::take(&mut msg.value), DlqReason::JsonParse, &msg));
                }
            }
        }
    }

    fn parse_all_root_field(
        &self,
        messages: Vec<Message>,
        info: &RootFieldInfo,
        builders: &mut [AnyBuilder],
        typed_scratch: &mut [TypedScratch],
        json_buf: &mut Vec<u8>,
        dlq_payloads: &mut Vec<(Bytes, DlqReason, i64, PartitionKey)>,
        exactly_once_key: Option<&ExactlyOnceKey>,
    ) {
        match self.chunk_splitter {
            ChunkSplitter::NewLine => self.parse_all_root_newline(
                messages, info, builders, typed_scratch, json_buf, dlq_payloads, exactly_once_key,
            ),
            ChunkSplitter::NoSplit => self.parse_all_root_nosplit(
                messages, info, builders, typed_scratch, json_buf, dlq_payloads, exactly_once_key,
            ),
        }
    }

    /// Fills `row` from one parsed JSON object. Returns `false` when a required
    /// column is missing (row goes to DLQ).
    fn fill_row(&self, json: &Value, row: &mut Vec<Value>) -> bool {
        row.clear();
        let mut all_ok = true;
        for m in &self.mappings {
            match self.extract_value(json, m) {
                Some(val) => row.push(val),
                None if !m.required => row.push(Value::Null),
                None => { all_ok = false; break; }
            }
        }
        all_ok
    }

    fn parse_mixed_newline(
        &self,
        messages: Vec<Message>,
        builders: &mut [AnyBuilder],
        dlq_payloads: &mut Vec<(Bytes, DlqReason, i64, PartitionKey)>,
        exactly_once_key: Option<&ExactlyOnceKey>,
        row: &mut Vec<Value>,
    ) {
        for msg in messages {
            for line in self.chunk_splitter.split_into_records(&msg.value) {
                match serde_json::from_slice::<Value>(line) {
                    Ok(json) => {
                        if self.fill_row(&json, row) {
                            for (builder, val) in builders.iter_mut().zip(row.iter()) {
                                append_value(builder, val);
                            }
                            if exactly_once_key.is_some() {
                                append_key_columns(builders, &msg);
                            }
                        } else {
                            dlq_payloads.push(dlq_tuple(Bytes::copy_from_slice(line), DlqReason::ExtractionFailed, &msg));
                        }
                    }
                    Err(_e) => {
                        dlq_payloads.push(dlq_tuple(Bytes::copy_from_slice(line), DlqReason::JsonParse, &msg));
                    }
                }
            }
        }
    }

    fn parse_mixed_nosplit(
        &self,
        messages: Vec<Message>,
        builders: &mut [AnyBuilder],
        dlq_payloads: &mut Vec<(Bytes, DlqReason, i64, PartitionKey)>,
        exactly_once_key: Option<&ExactlyOnceKey>,
        row: &mut Vec<Value>,
    ) {
        for mut msg in messages {
            match serde_json::from_slice::<Value>(&msg.value) {
                Ok(json) => {
                    if self.fill_row(&json, row) {
                        for (builder, val) in builders.iter_mut().zip(row.iter()) {
                            append_value(builder, val);
                        }
                        if exactly_once_key.is_some() {
                            append_key_columns(builders, &msg);
                        }
                    } else {
                        dlq_payloads.push(dlq_tuple(core::mem::take(&mut msg.value), DlqReason::ExtractionFailed, &msg));
                    }
                }
                Err(_e) => {
                    dlq_payloads.push(dlq_tuple(core::mem::take(&mut msg.value), DlqReason::JsonParse, &msg));
                }
            }
        }
    }

    fn parse_mixed(
        &self,
        messages: Vec<Message>,
        ws: &mut ParserWorkspace,
        exactly_once_key: Option<&ExactlyOnceKey>,
    ) {
        let n_cols = self.mappings.len();
        let mut row: Vec<Value> = Vec::with_capacity(n_cols);
        match self.chunk_splitter {
            ChunkSplitter::NewLine => self.parse_mixed_newline(
                messages, &mut ws.builders, &mut ws.dlq_payloads, exactly_once_key, &mut row,
            ),
            ChunkSplitter::NoSplit => self.parse_mixed_nosplit(
                messages, &mut ws.builders, &mut ws.dlq_payloads, exactly_once_key, &mut row,
            ),
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
    /// DLQ rows: raw bytes + reason + source position (offset, partition) of
    /// the Message that produced them (spec §7).
    dlq_payloads: Vec<(Bytes, DlqReason, i64, PartitionKey)>,
    /// Reusable arrays buffer (avoids Vec alloc per `finish()` call).
    arrays: Vec<ArrayRef>,
    /// Cached timestamp + Instant for coarse-grained `Utc::now()` (1ms resolution).
    cached_ts: Option<(time::OffsetDateTime, std::time::Instant)>,
}

impl Default for ParserWorkspace {
    fn default() -> Self {
        Self::new()
    }
}

impl ParserWorkspace {
    #[must_use]
    pub fn new() -> Self {
        Self {
            builders: Vec::new(),
            typed_scratch: Vec::new(),
            json_buf: Vec::new(),
            dlq_payloads: Vec::new(),
            arrays: Vec::new(),
            cached_ts: None,
        }
    }

    fn now(&mut self) -> time::OffsetDateTime {
        let now_inst = std::time::Instant::now();
        if let Some((ts, last)) = &self.cached_ts {
            if now_inst.duration_since(*last).as_millis() < 1 {
                return *ts;
            }
        }
        let ts = time::OffsetDateTime::now_utc();
        self.cached_ts = Some((ts, now_inst));
        ts
    }
}

// ---------------------------------------------------------------------------
// parse_into — main hot path
// ---------------------------------------------------------------------------

impl Parser for JsonParser {
    fn parse_into(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
        exactly_once_key: Option<ExactlyOnceKey>,
        ws: &mut ParserWorkspace,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let now = ws.now();

        // Exactly-once preconditions (fatal before batch construction, §3.1).
        if exactly_once_key.is_some() {
            self.check_exactly_once_preconditions(&messages)?;
        }

        // Pre-count rows for exact builder pre-allocation.
        let n_rows: usize = match self.chunk_splitter {
            ChunkSplitter::NewLine => messages.iter()
                .map(|msg| self.chunk_splitter.count_records(&msg.value))
                .sum(),
            ChunkSplitter::NoSplit => messages.len(),
        };

        ws.builders.clear();
        for &k in &self.kinds {
            ws.builders.push(make_builder(k, n_rows));
        }
        // Key columns are always the last two builders: partition (const per
        // Message) + offset (per-row). They must match the schema extension
        // done in `new()` (the same key), so `RecordBatch::try_new` below stays
        // consistent. DLQ rows never append here (spec §3.1).
        if let Some(key) = &exactly_once_key {
            if partition_is_utf8(key) {
                ws.builders.push(AnyBuilder::Utf8(StringBuilder::with_capacity(n_rows, n_rows * 128)));
            } else {
                ws.builders.push(AnyBuilder::Int64(Int64Builder::with_capacity(n_rows)));
            }
            ws.builders.push(AnyBuilder::Int64(Int64Builder::with_capacity(n_rows)));
        }

        ws.dlq_payloads.clear();

        match &self.mode {
            ParseMode::AllRootField(info) => {
                let n_cols = info.index.len();
                let ParserWorkspace { builders, typed_scratch, json_buf, dlq_payloads, .. } = ws;
                typed_scratch.clear();
                typed_scratch.resize_with(n_cols, || TypedScratch::Empty);
                self.parse_all_root_field(
                    messages,
                    info,
                    builders,
                    typed_scratch,
                    json_buf,
                    dlq_payloads,
                    exactly_once_key.as_ref(),
                );
            }
            ParseMode::Mixed => {
                self.parse_mixed(messages, ws, exactly_once_key.as_ref());
            }
        }

        ws.arrays.clear();
        ws.arrays.extend(ws.builders.iter_mut().map(AnyBuilder::finish));
        let batch = RecordBatch::try_new(Arc::clone(&self.arrow_schema), core::mem::take(&mut ws.arrays))?;

        let valid_batch = TableData {
            batch,
            table: Arc::clone(&self.table),
            is_dlq: false,
            batch_id: crate::batch_id(),
            exactly_once_key,
        };

        let dlq_batch = if ws.dlq_payloads.is_empty() {
            None
        } else {
            Some(self.build_dlq_batch(&ws.dlq_payloads, partition_id, now)?)
        };

        Ok((valid_batch, dlq_batch))
    }
}
// Regression tests — validate the simd-json invariant on real inputs
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::json_parser::config::JsonParserConfig;

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
            ColumnKind::Utf8,    // name
            ColumnKind::Utf8,    // city
            ColumnKind::Utf8,    // flag
        ];

        let idx = ColumnIndex::Small(vec![
            ("name".into(), 0),
            ("city".into(), 1),
            ("flag".into(), 2),
        ]);

        let info = RootFieldInfo { index: idx, required: vec![true, true, true], required_total: 3 };

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
                range.start, range.end,
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
        let info = RootFieldInfo { index: idx, required: vec![true], required_total: 1 };

        let mut scratch = vec![TypedScratch::Empty; 1];
        let mut buf = Vec::new();

        let ok = parse_root_fields_typed(json, &mut buf, &info, &mut scratch, &kinds)?;
        anyhow::ensure!(ok, "parse should succeed");

        let TypedScratch::Str(range) = scratch[0] else {
            anyhow::bail!("expected Str, got {:?}", scratch[0]);
        };
        let s = str_val(&buf, range);
        // After unescaping: \n -> newline, \t -> tab
        anyhow::ensure!(s.contains('\n'), "should contain unescaped newline, got {s:?}");
        anyhow::ensure!(s.contains('\t'), "should contain unescaped tab, got {s:?}");
        anyhow::ensure!(!s.contains('\\'), "should not contain backslash, got {s:?}");
        Ok(())
    }

    /// Verifies that `chunk_splitter: new-line` correctly splits multi-line
    /// messages and parses each line as a separate JSON row.
    #[test]
    fn newline_chunk_splitter() -> anyhow::Result<()> {
        use crate::config::yaml::{ChunkSplitter, ColumnMapping};

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
            raw_payload_field: None,
            order_by: vec![],
            chunk_splitter: ChunkSplitter::NewLine,
            skip_null_columns: false,
        };

        let parser = JsonParser::new(&config, "test".into(), None)?;
        let mut ws = ParserWorkspace::new();

        // 3 JSONs separated by \n, one empty line
        let payload = b"{\"id\":\"a\",\"val\":1}\n{\"id\":\"b\",\"val\":2}\n\n{\"id\":\"c\"}";
        let msgs = vec![Message { value: Bytes::copy_from_slice(payload), offset: None, partition: None }];

        let (good, dlq) = parser.parse_into(msgs, 0, None, &mut ws)?;

        anyhow::ensure!(good.batch.num_rows() == 3, "3 valid JSON lines \u{2192} 3 rows");
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

    /// YDS-style exactly-once key (spec §3.1): the batch gains the two key columns
    /// at the end — `__system_partition` (const per Message) + `__system_offset`
    /// (per-row). DLQ rows do not append to the main offset column.
    #[test]
    fn exactly_once_yds_key_columns_filled() -> anyhow::Result<()> {
        use crate::config::yaml::{ChunkSplitter, ColumnMapping};
        use crate::types::exactly_once::{ExactlyOnceColumn, ExactlyOnceKey};

        let config = JsonParserConfig {
            columns: vec![
                ColumnMapping {
                    jsonpath: "$.id".into(),
                    column_name: "id".into(),
                    arrow_type: "Utf8".into(),
                    nullable: false,
                },
            ],
            raw_payload_field: None,
            order_by: vec![],
            chunk_splitter: ChunkSplitter::NoSplit,
            skip_null_columns: false,
        };
        let key = ExactlyOnceKey {
            partition: ExactlyOnceColumn { name: "__system_partition".into() },
            offset: ExactlyOnceColumn { name: "__system_offset".into() },
        };
        let parser = JsonParser::new(&config, "test".into(), Some(key.clone()))?;
        let mut ws = ParserWorkspace::new();

        let msgs = vec![
            Message { value: Bytes::copy_from_slice(b"{\"id\":\"a\"}"), offset: Some(10), partition: Some(PartitionKey::Int(7)) },
            Message { value: Bytes::copy_from_slice(b"{\"id\":\"b\"}"), offset: Some(11), partition: Some(PartitionKey::Int(7)) },
        ];
        let (good, dlq) = parser.parse_into(msgs, 7, Some(key), &mut ws)?;
        anyhow::ensure!(dlq.is_none(), "both rows valid \u{2192} no DLQ");
        anyhow::ensure!(good.batch.num_rows() == 2, "expected 2 rows");
        anyhow::ensure!(good.batch.num_columns() == 3, "id + partition + offset");

        let part = int64_col(&good.batch, 1)?;
        let off = int64_col(&good.batch, 2)?;
        anyhow::ensure!(part.value(0) == 7);
        anyhow::ensure!(part.value(1) == 7, "partition is const per Message");
        anyhow::ensure!(off.value(0) == 10);
        anyhow::ensure!(off.value(1) == 11, "offset is per-row");

        // Key columns are non-nullable in the schema.
        anyhow::ensure!(!good.batch.schema().field(1).is_nullable());
        anyhow::ensure!(!good.batch.schema().field(2).is_nullable());
        Ok(())
    }

    /// S3-style key: partition column is Utf8 (`__system_filename`), filled from
    /// `Message.partition = Str(full S3 key)`.
    #[test]
    fn exactly_once_s3_key_columns_filled() -> anyhow::Result<()> {
        use crate::config::yaml::{ChunkSplitter, ColumnMapping};
        use crate::types::exactly_once::{ExactlyOnceColumn, ExactlyOnceKey};

        let config = JsonParserConfig {
            columns: vec![
                ColumnMapping {
                    jsonpath: "$.id".into(),
                    column_name: "id".into(),
                    arrow_type: "Utf8".into(),
                    nullable: false,
                },
            ],
            raw_payload_field: None,
            order_by: vec![],
            chunk_splitter: ChunkSplitter::NoSplit,
            skip_null_columns: false,
        };
        let key = ExactlyOnceKey {
            partition: ExactlyOnceColumn { name: "__system_filename".into() },
            offset: ExactlyOnceColumn { name: "__system_offset".into() },
        };
        let parser = JsonParser::new(&config, "test".into(), Some(key.clone()))?;
        let mut ws = ParserWorkspace::new();

        let msgs = vec![Message {
            value: Bytes::copy_from_slice(b"{\"id\":\"a\"}"),
            offset: Some(3),
            partition: Some(PartitionKey::Str("prefix-a/2024/data.json".into())),
        }];
        let (good, _dlq) = parser.parse_into(msgs, 0, Some(key), &mut ws)?;

        let part = string_col(&good.batch, 1)?;
        let off = int64_col(&good.batch, 2)?;
        anyhow::ensure!(part.value(0) == "prefix-a/2024/data.json");
        anyhow::ensure!(off.value(0) == 3);
        Ok(())
    }

    /// DLQ batch gets its own key columns (spec §7): offset + partition from the
    /// Message that produced the DLQ row — YDS Int64 partition.
    #[test]
    fn exactly_once_dlq_has_key_columns() -> anyhow::Result<()> {
        use crate::config::yaml::{ChunkSplitter, ColumnMapping};
        use crate::types::exactly_once::{ExactlyOnceColumn, ExactlyOnceKey};

        let config = JsonParserConfig {
            columns: vec![
                ColumnMapping {
                    jsonpath: "$.id".into(),
                    column_name: "id".into(),
                    arrow_type: "Utf8".into(),
                    nullable: false,
                },
            ],
            raw_payload_field: None,
            order_by: vec![],
            chunk_splitter: ChunkSplitter::NewLine,
            skip_null_columns: false,
        };
        let key = ExactlyOnceKey {
            partition: ExactlyOnceColumn { name: "__system_partition".into() },
            offset: ExactlyOnceColumn { name: "__system_offset".into() },
        };
        let parser = JsonParser::new(&config, "test".into(), Some(key.clone()))?;
        let mut ws = ParserWorkspace::new();

        // One valid line + one line that is not valid JSON.
        let payload = b"{\"id\":\"a\"}\nnot json";
        let msgs = vec![Message {
            value: Bytes::copy_from_slice(payload),
            offset: Some(5),
            partition: Some(PartitionKey::Int(3)),
        }];
        let (good, dlq) = parser.parse_into(msgs, 3, Some(key), &mut ws)?;

        anyhow::ensure!(good.batch.num_rows() == 1, "valid line \u{2192} main batch");
        let dlq_batch = dlq.ok_or_else(|| anyhow::anyhow!("invalid line produced no DLQ batch"))?;
        // raw_bytes, error_message, timestamp, __system_partition, __system_offset
        anyhow::ensure!(dlq_batch.batch.num_columns() == 5, "DLQ carries its own key columns");
        let part = int64_col(&dlq_batch.batch, 3)?;
        let off = int64_col(&dlq_batch.batch, 4)?;
        anyhow::ensure!(part.value(0) == 3, "partition from the Message");
        anyhow::ensure!(off.value(0) == 5, "offset from the Message");
        Ok(())
    }

    /// `Message.offset == None` (or partition None) with exactly-once → fatal
    /// before batch construction (spec §3.1).
    #[test]
    fn exactly_once_missing_offset_fails() -> anyhow::Result<()> {
        use crate::config::yaml::{ChunkSplitter, ColumnMapping};
        use crate::types::exactly_once::{ExactlyOnceColumn, ExactlyOnceKey};

        let config = JsonParserConfig {
            columns: vec![
                ColumnMapping {
                    jsonpath: "$.id".into(),
                    column_name: "id".into(),
                    arrow_type: "Utf8".into(),
                    nullable: false,
                },
            ],
            raw_payload_field: None,
            order_by: vec![],
            chunk_splitter: ChunkSplitter::NoSplit,
            skip_null_columns: false,
        };
        let key = ExactlyOnceKey {
            partition: ExactlyOnceColumn { name: "__system_partition".into() },
            offset: ExactlyOnceColumn { name: "__system_offset".into() },
        };
        let parser = JsonParser::new(&config, "test".into(), Some(key.clone()))?;
        let mut ws = ParserWorkspace::new();

        // offset missing → bail with a readable message
        let msgs = vec![Message {
            value: Bytes::copy_from_slice(b"{\"id\":\"a\"}"),
            offset: None,
            partition: Some(PartitionKey::Int(1)),
        }];
        let err = parser
            .parse_into(msgs, 1, Some(key.clone()), &mut ws)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected parse_into to fail when offset is missing"))?;
        anyhow::ensure!(err.to_string().contains("Message.offset"), "got: {err}");

        // partition missing → bail as well (rules are symmetric)
        let msgs_no_partition = vec![Message {
            value: Bytes::copy_from_slice(b"{\"id\":\"a\"}"),
            offset: Some(1),
            partition: None,
        }];
        let err_partition = parser
            .parse_into(msgs_no_partition, 1, Some(key), &mut ws)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected parse_into to fail when partition is missing"))?;
        anyhow::ensure!(err_partition.to_string().contains("Message.partition"), "got: {err_partition}");
        Ok(())
    }

    /// User data column named like a system column + exactly-once → fatal with a
    /// readable message (spec §2, runtime collision guard).
    #[test]
    fn exactly_once_system_column_collision_fails() -> anyhow::Result<()> {
        use crate::config::yaml::{ChunkSplitter, ColumnMapping};
        use crate::types::exactly_once::{ExactlyOnceColumn, ExactlyOnceKey};

        let config = JsonParserConfig {
            columns: vec![
                ColumnMapping {
                    jsonpath: "$.id".into(),
                    column_name: "__system_offset".into(),
                    arrow_type: "Utf8".into(),
                    nullable: false,
                },
            ],
            raw_payload_field: None,
            order_by: vec![],
            chunk_splitter: ChunkSplitter::NoSplit,
            skip_null_columns: false,
        };
        let key = ExactlyOnceKey {
            partition: ExactlyOnceColumn { name: "__system_partition".into() },
            offset: ExactlyOnceColumn { name: "__system_offset".into() },
        };
        let parser = JsonParser::new(&config, "test".into(), Some(key.clone()))?;
        let mut ws = ParserWorkspace::new();

        let msgs = vec![Message {
            value: Bytes::copy_from_slice(b"{\"id\":\"a\"}"),
            offset: Some(1),
            partition: Some(PartitionKey::Int(1)),
        }];
        let err = parser
            .parse_into(msgs, 1, Some(key), &mut ws)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected parse_into to fail on system column collision"))?;
        anyhow::ensure!(err.to_string().contains("conflicts"), "got: {err}");
        Ok(())
    }

    fn string_col(batch: &RecordBatch, idx: usize) -> anyhow::Result<&arrow::array::StringArray> {
        batch.column(idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| anyhow::anyhow!("column {idx} is not StringArray"))
    }

    fn int64_col(batch: &RecordBatch, idx: usize) -> anyhow::Result<&arrow::array::Int64Array> {
        batch.column(idx)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("column {idx} is not Int64Array"))
    }
}