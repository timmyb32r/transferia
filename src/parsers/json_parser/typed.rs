use serde::{de, Deserializer};

use super::parser::ColumnKind;

/// A byte range in the JSON buffer that simd-json has proven to contain valid
/// UTF-8. The range is inseparable from the exact allocation that was parsed.
#[derive(Clone, Copy, Debug)]
pub(super) struct ValidatedStr {
    start: usize,
    end: usize,
    owner_ptr: usize,
    owner_len: usize,
}

impl ValidatedStr {
    #[inline]
    fn from_simd_json_str(s: &str, buf_ptr: *const u8, buf_len: usize) -> Option<Self> {
        let owner_ptr = buf_ptr as usize;
        let start = (s.as_ptr() as usize).checked_sub(owner_ptr)?;
        let end = start.checked_add(s.len())?;
        (end <= buf_len).then_some(Self {
            start,
            end,
            owner_ptr,
            owner_len: buf_len,
        })
    }

    fn belongs_to(self, json_buf: &[u8]) -> bool {
        self.owner_ptr == json_buf.as_ptr() as usize
            && self.owner_len == json_buf.len()
            && self.end <= json_buf.len()
    }

    #[cfg(test)]
    pub(super) const fn byte_range(self) -> (usize, usize) {
        (self.start, self.end)
    }
}

/// Per-field scratch value. Strings retain a validated range into the current
/// parser input instead of allocating an owned `String`.
#[derive(Clone, Copy, Debug)]
pub(super) enum TypedScratch {
    Empty,
    Str(ValidatedStr),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

/// Deserializes one JSON value directly into typed scratch storage.
pub(super) struct TypedValueWriter<'ctx> {
    pub target: &'ctx mut TypedScratch,
    pub buf_ptr: *const u8,
    pub buf_len: usize,
    pub kind: ColumnKind,
}

impl<'de> de::DeserializeSeed<'de> for TypedValueWriter<'_> {
    type Value = bool;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        use serde::Deserialize as _;
        match self.kind {
            ColumnKind::Utf8 | ColumnKind::LargeUtf8 => {
                let Some(s) = Option::<&str>::deserialize(deserializer)? else {
                    return Ok(false);
                };
                let validated = ValidatedStr::from_simd_json_str(s, self.buf_ptr, self.buf_len)
                    .ok_or_else(|| {
                        de::Error::custom("simd-json string does not belong to its input buffer")
                    })?;
                *self.target = TypedScratch::Str(validated);
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
            ColumnKind::Json | ColumnKind::Decimal128(..) => {
                return Err(de::Error::custom(
                    "JSON and Decimal128 columns use the exact general parser",
                ));
            }
        }
        Ok(true)
    }
}

/// Reconstructs a string after checking that the validated range still belongs
/// to the exact buffer allocation from which simd-json produced it.
///
/// # Safety invariant
///
/// `ValidatedStr::from_simd_json_str` accepts only a valid `str`, verifies its
/// bounds, and records the input allocation identity. This function repeats
/// those identity and bounds checks immediately before skipping UTF-8 validation.
#[inline]
#[expect(
    unsafe_code,
    reason = "ValidatedStr proves this hot-path slice was already UTF-8 validated"
)]
pub(super) fn str_val(json_buf: &[u8], range: ValidatedStr) -> anyhow::Result<&str> {
    anyhow::ensure!(
        range.belongs_to(json_buf),
        "validated JSON string was paired with a different source buffer"
    );
    // SAFETY: construction and the checks above establish valid UTF-8, exact
    // allocation identity, and in-bounds slicing.
    Ok(unsafe { core::str::from_utf8_unchecked(&json_buf[range.start..range.end]) })
}
