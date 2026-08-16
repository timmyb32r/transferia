use core::fmt;
use std::collections::HashMap;

use serde::{de, Deserializer as _};

use super::parser::{ColumnKind, TypedScratch, TypedValueWriter};

/// Adaptive column index: linear scan for narrow schemas and a hash table for
/// wider schemas.
pub(super) enum ColumnIndex {
    Small(Vec<(String, usize)>),
    Large(HashMap<String, usize>),
}

impl ColumnIndex {
    pub fn len(&self) -> usize {
        match *self {
            Self::Small(ref values) => values.len(),
            Self::Large(ref values) => values.len(),
        }
    }

    #[inline]
    fn get(&self, key: &str) -> Option<&usize> {
        match *self {
            Self::Small(ref values) => values
                .iter()
                .find(|item| item.0.as_str() == key)
                .map(|item| &item.1),
            Self::Large(ref values) => values.get(key),
        }
    }
}

pub(super) struct RootFieldInfo {
    pub index: ColumnIndex,
    pub required: Vec<bool>,
    pub required_total: usize,
    pub reject_unknown: bool,
}

struct TypedFieldExtractor<'ctx> {
    index: &'ctx ColumnIndex,
    scratch: &'ctx mut [TypedScratch],
    kinds: &'ctx [ColumnKind],
    required: &'ctx [bool],
    reject_unknown: bool,
    seen: &'ctx mut [bool],
    buf_ptr: *const u8,
    required_filled: usize,
    required_total: usize,
    duplicate_mapped_field: bool,
    unknown_field: bool,
}

pub(super) struct DuplicateMappedRootVisitor<'a> {
    pub fields: &'a [String],
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
            if let Some(&index) = self.index.get(key) {
                if self.seen[index] {
                    self.duplicate_mapped_field = true;
                }
                self.seen[index] = true;
                let was_empty = matches!(self.scratch[index], TypedScratch::Empty);
                let seed = TypedValueWriter {
                    target: &mut self.scratch[index],
                    buf_ptr: self.buf_ptr,
                    kind: self.kinds[index],
                };
                let present = map.next_value_seed(seed)?;
                if present && was_empty && self.required[index] {
                    self.required_filled += 1;
                }
            } else {
                self.unknown_field = true;
                map.next_value::<de::IgnoredAny>()?;
            }
        }
        Ok(
            !(self.duplicate_mapped_field || self.reject_unknown && self.unknown_field)
                && self.required_filled == self.required_total,
        )
    }
}

pub(super) fn parse_root_fields_typed(
    bytes: &[u8],
    buffer: &mut Vec<u8>,
    info: &RootFieldInfo,
    scratch: &mut [TypedScratch],
    seen: &mut [bool],
    kinds: &[ColumnKind],
) -> anyhow::Result<bool> {
    seen.fill(false);
    buffer.clear();
    buffer.extend_from_slice(bytes);
    let buffer_pointer = buffer.as_ptr();
    let mut deserializer =
        simd_json::Deserializer::from_slice(buffer).map_err(anyhow::Error::from)?;
    let mut extractor = TypedFieldExtractor {
        index: &info.index,
        scratch,
        kinds,
        required: &info.required,
        reject_unknown: info.reject_unknown,
        seen,
        buf_ptr: buffer_pointer,
        required_filled: 0,
        required_total: info.required_total,
        duplicate_mapped_field: false,
        unknown_field: false,
    };
    deserializer
        .deserialize_map(&mut extractor)
        .map_err(Into::into)
}
