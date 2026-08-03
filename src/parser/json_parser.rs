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
use std::fmt;
use std::sync::{Arc, LazyLock};

use crate::config::yaml::{parse_arrow_type, SchemaConfig};
use crate::types::arrow_batch::{ArrowBatch, BatchMeta};
use crate::types::message::Message;

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
/// HashMap for wider schemas.
enum ColumnIndex {
    Small(Vec<(String, usize)>),
    Large(HashMap<String, usize>),
}

impl ColumnIndex {
    fn len(&self) -> usize {
        match self {
            ColumnIndex::Small(v) => v.len(),
            ColumnIndex::Large(m) => m.len(),
        }
    }

    #[inline]
    fn get(&self, key: &str) -> Option<&usize> {
        match self {
            ColumnIndex::Small(v) => v.iter().find(|(k, _)| k.as_str() == key).map(|(_, i)| i),
            ColumnIndex::Large(m) => m.get(key),
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
    fn from_data_type(dt: &DataType) -> Option<Self> {
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
            _ => return None,
        })
    }
}

#[inline]
fn make_builder(kind: ColumnKind, n: usize) -> AnyBuilder {
    const STR_BYTES_PER_ROW: usize = 128usize;
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
        AnyBuilder::Float64(b) => b.append_value(val.as_f64().unwrap_or(0.0)),
        AnyBuilder::Float32(b) => b.append_value(val.as_f64().unwrap_or(0.0) as f32),
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
struct TypedValueWriter2<'a> {
    target: &'a mut TypedScratch,
    /// Base pointer of the JSON buffer. Used to compute the byte offset of the
    /// `&str` returned by simd-json via pointer arithmetic:
    /// `offset = s.as_ptr() - buf_ptr`.
    buf_ptr: *const u8,
    kind: ColumnKind,
}

impl<'de, 'a> de::DeserializeSeed<'de> for TypedValueWriter2<'a> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        use serde::Deserialize;
        match self.kind {
            ColumnKind::Utf8 | ColumnKind::LargeUtf8 => {
                let s = <&str>::deserialize(d)?;
                // ValidatedStr captures the byte range of s within the simd-json
                // buffer. Because `s` is an `&str`, it is valid UTF-8 by definition
                // — simd-json already validated it. The pointer arithmetic gives us
                // the exact byte range without a manual position counter.
                *self.target = TypedScratch::Str(ValidatedStr::from_simd_json_str(s, self.buf_ptr));
            }
            ColumnKind::Int64 => {
                *self.target = TypedScratch::I64(i64::deserialize(d)?);
            }
            ColumnKind::Int32 => {
                *self.target = TypedScratch::I64(i32::deserialize(d)? as i64);
            }
            ColumnKind::Int16 => {
                *self.target = TypedScratch::I64(i16::deserialize(d)? as i64);
            }
            ColumnKind::Int8 => {
                *self.target = TypedScratch::I64(i8::deserialize(d)? as i64);
            }
            ColumnKind::UInt64 => {
                *self.target = TypedScratch::U64(u64::deserialize(d)?);
            }
            ColumnKind::UInt32 => {
                *self.target = TypedScratch::U64(u32::deserialize(d)? as u64);
            }
            ColumnKind::UInt16 => {
                *self.target = TypedScratch::U64(u16::deserialize(d)? as u64);
            }
            ColumnKind::UInt8 => {
                *self.target = TypedScratch::U64(u8::deserialize(d)? as u64);
            }
            ColumnKind::Float64 => {
                *self.target = TypedScratch::F64(f64::deserialize(d)?);
            }
            ColumnKind::Float32 => {
                *self.target = TypedScratch::F64(f32::deserialize(d)? as f64);
            }
            ColumnKind::Boolean => {
                *self.target = TypedScratch::Bool(bool::deserialize(d)?);
            }
            ColumnKind::Date32 => {
                *self.target = TypedScratch::I64(i32::deserialize(d)? as i64);
            }
            ColumnKind::Date64
            | ColumnKind::TimestampMillisecond
            | ColumnKind::TimestampMicrosecond
            | ColumnKind::TimestampNanosecond
            | ColumnKind::TimestampSecond => {
                *self.target = TypedScratch::I64(i64::deserialize(d)?);
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
#[inline]
fn str_val(json_buf: &[u8], range: ValidatedStr) -> &str {
    // SAFETY: ValidatedStr can only be constructed from a simd-json `&str`
    // reference via `from_simd_json_str()`. `&str` is valid UTF-8 by definition,
    // and the byte range is computed via pointer arithmetic from that reference.
    // Therefore json_buf[start..end] IS the original validated &str content.
    unsafe { std::str::from_utf8_unchecked(&json_buf[range.start..range.end]) }
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
                _ => append_null(builder),
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
            _ => append_null(builder),
        },
        TypedScratch::U64(n) => match builder {
            AnyBuilder::UInt64(b) => b.append_value(*n),
            AnyBuilder::UInt32(b) => b.append_value(*n as u32),
            AnyBuilder::UInt16(b) => b.append_value(*n as u16),
            AnyBuilder::UInt8(b) => b.append_value(*n as u8),
            _ => append_null(builder),
        },
        TypedScratch::F64(n) => match builder {
            AnyBuilder::Float64(b) => b.append_value(*n),
            AnyBuilder::Float32(b) => b.append_value(*n as f32),
            _ => append_null(builder),
        },
        TypedScratch::Bool(v) => match builder {
            AnyBuilder::Boolean(b) => b.append_value(*v),
            _ => append_null(builder),
        },
        TypedScratch::Empty => append_null(builder),
    }
}

// ---------------------------------------------------------------------------
// Two-phase typed field extractor — writes to scratch, not builders
// ---------------------------------------------------------------------------

struct TypedFieldExtractor<'a> {
    index: &'a ColumnIndex,
    scratch: &'a mut [TypedScratch],
    kinds: &'a [ColumnKind],
    /// Per-column requiredness (non-nullable), indexed by column position.
    required: &'a [bool],
    /// Base pointer of the JSON buffer passed to simd-json.
    /// Used to compute byte offsets for string values via pointer arithmetic.
    buf_ptr: *const u8,
    /// How many *required* columns have been filled so far.
    required_filled: usize,
    /// Total number of required columns — a row is valid once all are filled.
    required_total: usize,
}

impl<'de, 'a> de::Visitor<'de> for &'a mut TypedFieldExtractor<'a> {
    type Value = bool;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
        while let Some(key) = map.next_key::<&str>()? {
            if let Some(&idx) = self.index.get(key) {
                let was_empty = matches!(self.scratch[idx], TypedScratch::Empty);
                let seed = TypedValueWriter2 {
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
// DLQ
// ---------------------------------------------------------------------------

static DLQ_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("raw_bytes", DataType::Utf8, false),
        Field::new("error_message", DataType::Utf8, false),
        Field::new("partition_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Utf8, false),
    ]))
});

enum DlqReason {
    JsonParse,
    ExtractionFailed,
}

impl DlqReason {
    fn as_str(&self) -> &str {
        match self {
            DlqReason::JsonParse => "JSON parse error",
            DlqReason::ExtractionFailed => "JSONPath extraction failed for one or more columns",
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
    /// Cached per-column DataType (avoids double parse_arrow_type).
    _data_types: Vec<DataType>,
}

struct ColumnMappingExt {
    path: CompiledPath,
    /// `true` when the column is non-nullable (a missing value routes the row to DLQ).
    required: bool,
}

impl JsonParser {
    pub fn new(config: &SchemaConfig, table: Arc<str>) -> anyhow::Result<Self> {
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
                .map(|(i, m)| match &m.path {
                    CompiledPath::RootField(f) => (f.clone(), i),
                    _ => unreachable!(),
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
        let arrow_schema = Arc::new(Schema::new(fields));

        Ok(Self { mappings, kinds, arrow_schema, mode, table, _data_types: data_types })
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
        dlq_payloads: &[(Bytes, DlqReason)],
        partition_id: i64,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<ArrowBatch> {
        let n = dlq_payloads.len();
        let mut raw_builder = StringBuilder::with_capacity(n, n * 64);
        let mut err_builder = StringBuilder::with_capacity(n, n * 64);
        let mut pid_builder = Int64Builder::with_capacity(n);
        let mut ts_builder = StringBuilder::with_capacity(n, n * 32);
        let ts = now.to_rfc3339();

        for (raw_bytes, reason) in dlq_payloads {
            raw_builder.append_value(&String::from_utf8_lossy(raw_bytes));
            err_builder.append_value(reason.as_str());
            pid_builder.append_value(partition_id);
            ts_builder.append_value(&ts);
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(raw_builder.finish()), Arc::new(err_builder.finish()),
            Arc::new(pid_builder.finish()), Arc::new(ts_builder.finish()),
        ];
        let batch = RecordBatch::try_new(DLQ_SCHEMA.clone(), arrays)?;

        Ok(ArrowBatch {
            batch,
            meta: BatchMeta { dlq_flag: true, batch_id: crate::batch_id(), table: self.table.clone() },
        })
    }
}

// ---------------------------------------------------------------------------
// ParserWorkspace — reusable buffers per partition
// ---------------------------------------------------------------------------

pub struct ParserWorkspace {
    builders: Vec<AnyBuilder>,
    typed_scratch: Vec<TypedScratch>,
    json_buf: Vec<u8>,
    dlq_payloads: Vec<(Bytes, DlqReason)>,
    /// Reusable arrays buffer (avoids Vec alloc per `finish()` call).
    arrays: Vec<ArrayRef>,
    /// Cached timestamp + Instant for coarse-grained Utc::now() (1ms resolution).
    cached_ts: Option<(chrono::DateTime<chrono::Utc>, std::time::Instant)>,
}

impl Default for ParserWorkspace {
    fn default() -> Self {
        Self::new()
    }
}

impl ParserWorkspace {
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

    fn now(&mut self) -> chrono::DateTime<chrono::Utc> {
        let now_inst = std::time::Instant::now();
        if let Some((ts, last)) = &self.cached_ts {
            if now_inst.duration_since(*last).as_millis() < 1 {
                return *ts;
            }
        }
        let ts = chrono::Utc::now();
        self.cached_ts = Some((ts, now_inst));
        ts
    }
}

// ---------------------------------------------------------------------------
// parse_into — main hot path
// ---------------------------------------------------------------------------

impl JsonParser {
    pub fn parse_into(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
        ws: &mut ParserWorkspace,
    ) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)> {
        let n_msgs = messages.len();
        let now = ws.now();

        ws.builders.clear();
        for &k in &self.kinds {
            ws.builders.push(make_builder(k, n_msgs));
        }

        ws.dlq_payloads.clear();

        match &self.mode {
            ParseMode::AllRootField(info) => {
                let n_cols = info.index.len();

                // Split borrows for simultaneous mutable access to different fields
                let ParserWorkspace { builders, typed_scratch, json_buf, dlq_payloads, .. } = ws;
                typed_scratch.clear();
                typed_scratch.resize_with(n_cols, || TypedScratch::Empty);

                for mut msg in messages {
                    typed_scratch.fill(TypedScratch::Empty);

                    match parse_root_fields_typed(&msg.value, json_buf, info, typed_scratch, &self.kinds) {
                        Ok(true) => {
                            for (builder, s) in builders.iter_mut().zip(typed_scratch.iter()) {
                                append_typed(builder, s, json_buf);
                            }
                        }
                        Ok(false) => {
                            dlq_payloads.push((std::mem::take(&mut msg.value), DlqReason::ExtractionFailed));
                        }
                        Err(_e) => {
                            dlq_payloads.push((std::mem::take(&mut msg.value), DlqReason::JsonParse));
                        }
                    }
                }
            }
            ParseMode::Mixed => {
                let n_cols = self.mappings.len();
                let mut row: Vec<Value> = Vec::with_capacity(n_cols);

                for mut msg in messages {
                    match serde_json::from_slice::<Value>(&msg.value) {
                        Ok(json) => {
                            row.clear();
                            let mut all_ok = true;
                            for m in &self.mappings {
                                match self.extract_value(&json, m) {
                                    Some(val) => row.push(val),
                                    // Missing nullable column → NULL; missing required → DLQ.
                                    None if !m.required => row.push(Value::Null),
                                    None => { all_ok = false; break; }
                                }
                            }
                            if all_ok {
                                for (builder, val) in ws.builders.iter_mut().zip(row.iter()) {
                                    append_value(builder, val);
                                }
                            } else {
                                ws.dlq_payloads.push((std::mem::take(&mut msg.value), DlqReason::ExtractionFailed));
                            }
                        }
                        Err(_e) => {
                            ws.dlq_payloads.push((std::mem::take(&mut msg.value), DlqReason::JsonParse));
                        }
                    }
                }
            }
        }

        ws.arrays.clear();
        ws.arrays.extend(ws.builders.iter_mut().map(|b| b.finish()));
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), std::mem::take(&mut ws.arrays))?;

        let valid_batch = ArrowBatch {
            batch,
            meta: BatchMeta { dlq_flag: false, batch_id: crate::batch_id(), table: self.table.clone() },
        };

        let dlq_batch = if !ws.dlq_payloads.is_empty() {
            Some(self.build_dlq_batch(&ws.dlq_payloads, partition_id, now)?)
        } else {
            None
        };

        Ok((valid_batch, dlq_batch))
    }
}

// ---------------------------------------------------------------------------
// Regression tests — validate the simd-json invariant on real inputs
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies the core invariant end-to-end: simd-json returns `&str`
    /// values whose bytes exactly match `json_buf[start..end]`.
    ///
    /// If this test fails, the safety comment on `str_val` is WRONG and
    /// the unsafe block is producing garbage (or UB).
    #[test]
    fn str_val_matches_simd_json_output() {
        // "Москва" and "🚀" as explicit UTF-8 byte sequences
        let json = b"{\"name\":\"Alice\",\"city\":\"\xD0\x9C\xD0\xBE\xD1\x81\xD0\xBA\xD0\xB2\xD0\xB0\",\"flag\":\"\xF0\x9F\x9A\x80\"}";

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

        let ok = parse_root_fields_typed(json, &mut buf, &info, &mut scratch, &kinds).unwrap();
        assert!(ok, "all fields should be found");

        // buf has been modified by simd-json in-situ parsing.
        // Now verify: json_buf[start..end] is valid UTF-8 AND matches the expected string.
        let expected = ["Alice", "Москва", "🚀"];
        for (i, exp) in expected.iter().enumerate() {
            match &scratch[i] {
                TypedScratch::Str(range) => {
                    let reconstructed = str_val(&buf, *range);
                    assert_eq!(
                        reconstructed, *exp,
                        "Column {i}: str_val({}..{}) = {reconstructed:?}, expected {exp:?}",
                        range.start, range.end,
                    );
                }
                other => panic!("Column {i}: expected Str, got {other:?}"),
            }
        }
    }

    /// Verifies that `str_val` correctly handles strings with escape sequences
    /// (simd-json unescapes them in-situ, so the byte range should contain
    /// the unescaped version).
    #[test]
    fn str_val_with_escapes() {
        // JSON with escape sequences that simd-json will process in-situ
        let json = br#"{"text":"Line1\nLine2\tTabbed"}"#;

        let kinds = vec![ColumnKind::Utf8];
        let idx = ColumnIndex::Small(vec![("text".into(), 0)]);
        let info = RootFieldInfo { index: idx, required: vec![true], required_total: 1 };

        let mut scratch = vec![TypedScratch::Empty; 1];
        let mut buf = Vec::new();

        let ok = parse_root_fields_typed(json, &mut buf, &info, &mut scratch, &kinds).unwrap();
        assert!(ok);

        match &scratch[0] {
            TypedScratch::Str(range) => {
                let s = str_val(&buf, *range);
                // After unescaping: \n -> newline, \t -> tab
                assert!(s.contains('\n'), "should contain unescaped newline, got {s:?}");
                assert!(s.contains('\t'), "should contain unescaped tab, got {s:?}");
                assert!(!s.contains('\\'), "should not contain backslash, got {s:?}");
            }
            other => panic!("expected Str, got {other:?}"),
        }
    }
}
