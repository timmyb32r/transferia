use object_store::path::Path;
use ring::digest::{digest, SHA256};

/// Amazon S3 measures object-key length in UTF-8 bytes.
pub(super) const MAX_OBJECT_KEY_BYTES: usize = 1024;
/// Dynamic values are bounded independently so a single source row cannot
/// turn an otherwise valid static layout into a permanently failing key.
pub(super) const MAX_DYNAMIC_COMPONENT_BYTES: usize = 192;
const HASH_MARKER: &str = "~sha256=";
const SHA256_HEX_BYTES: usize = 64;
const LOWER_HEX: &[u8; 16] = b"0123456789abcdef";

/// A fully constructed object key which satisfies both the generic
/// `object_store` path contract and the stricter S3 byte-length limit.
#[derive(Debug)]
pub(super) struct ObjectKey(String);

impl ObjectKey {
    pub(super) fn for_json_object(
        prefix: &str,
        table: &str,
        partition_path: &str,
        topic: &str,
        source_partition: i64,
        start_offset: i64,
    ) -> anyhow::Result<Self> {
        let topic = bounded_dynamic_component(topic.as_bytes());
        let filename = format!("{topic}+{source_partition}+{start_offset}.json");
        let prefix = prefix.trim_matches('/');
        let key = if prefix.is_empty() {
            format!("{table}/{partition_path}/{filename}")
        } else {
            format!("{prefix}/{table}/{partition_path}/{filename}")
        };
        Self::parse(key)
    }

    fn parse(key: String) -> anyhow::Result<Self> {
        anyhow::ensure!(
            key.len() <= MAX_OBJECT_KEY_BYTES,
            "S3 object key is {} UTF-8 bytes, exceeding the {MAX_OBJECT_KEY_BYTES}-byte limit",
            key.len()
        );
        let parsed = Path::parse(&key)
            .map_err(|error| anyhow::anyhow!("invalid S3 object key '{key}': {error}"))?;
        anyhow::ensure!(
            parsed.as_ref() == key,
            "S3 object key must be a normalized relative path"
        );
        Ok(Self(key))
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

const fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn push_percent_encoded(output: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    if is_unreserved(byte) {
        output.push(char::from(byte));
    } else {
        output.push('%');
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
}

fn percent_encoded_len(value: &[u8]) -> usize {
    value.iter().fold(0_usize, |length, &byte| {
        length.saturating_add(if is_unreserved(byte) { 1 } else { 3 })
    })
}

/// Percent-encode one data-derived path component. Overlong values retain a
/// readable prefix and the complete-value digest, making the result both
/// deterministic and bounded without silently aliasing common prefixes.
pub(super) fn bounded_dynamic_component(value: &[u8]) -> String {
    if percent_encoded_len(value) <= MAX_DYNAMIC_COMPONENT_BYTES {
        return percent_encode(value);
    }

    let prefix_bytes = MAX_DYNAMIC_COMPONENT_BYTES - HASH_MARKER.len() - SHA256_HEX_BYTES;
    let mut encoded = String::with_capacity(MAX_DYNAMIC_COMPONENT_BYTES);
    for &byte in value {
        let encoded_byte_len = if is_unreserved(byte) { 1 } else { 3 };
        if encoded.len().saturating_add(encoded_byte_len) > prefix_bytes {
            break;
        }
        push_percent_encoded(&mut encoded, byte);
    }
    encoded.push_str(HASH_MARKER);
    for &byte in digest(&SHA256, value).as_ref() {
        encoded.push(char::from(LOWER_HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(LOWER_HEX[usize::from(byte & 0x0f)]));
    }
    debug_assert!(encoded.len() <= MAX_DYNAMIC_COMPONENT_BYTES);
    encoded
}

/// Bound every segment of a data-derived relative path independently while
/// preserving its hierarchy. Chrono accepts years beyond four digits, so a
/// few startup samples cannot prove every formatted record-time key length.
pub(super) fn bounded_dynamic_path(path: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        !path.is_empty() && !path.starts_with('/') && !path.ends_with('/'),
        "dynamic path must be a non-empty relative path without empty edge segments"
    );
    path.split('/')
        .map(|segment| {
            anyhow::ensure!(
                !segment.is_empty(),
                "dynamic path contains an empty segment"
            );
            Ok(if segment.len() <= MAX_DYNAMIC_COMPONENT_BYTES {
                segment.to_string()
            } else {
                bounded_dynamic_component(segment.as_bytes())
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|segments| segments.join("/"))
}

fn percent_encode(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len());
    for &byte in value {
        push_percent_encoded(&mut encoded, byte);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_key_layout_has_a_stable_golden_value() -> anyhow::Result<()> {
        let key =
            ObjectKey::for_json_object("streams/", "events", "tenant=alpha", "topic/a", 3, 77)?;
        assert_eq!(
            key.as_str(),
            "streams/events/tenant=alpha/topic%2Fa+3+77.json"
        );
        Ok(())
    }

    #[test]
    fn rejects_static_layout_overhead_beyond_the_s3_utf8_byte_limit() {
        let table = "x".repeat(MAX_OBJECT_KEY_BYTES);
        let error = ObjectKey::for_json_object("", &table, "partition=0", "topic", 0, 0)
            .expect_err("an overlong key must fail before object-store I/O");
        assert!(error.to_string().contains("1024-byte limit"));
    }

    #[test]
    fn accepts_the_exact_s3_object_key_byte_limit() -> anyhow::Result<()> {
        let suffix = "/partition=0/topic+0+0.json";
        let table = "x".repeat(MAX_OBJECT_KEY_BYTES - suffix.len());
        let key = ObjectKey::for_json_object("", &table, "partition=0", "topic", 0, 0)?;
        assert_eq!(key.as_str().len(), MAX_OBJECT_KEY_BYTES);
        Ok(())
    }

    #[test]
    fn overlong_dynamic_topic_is_bounded_before_final_validation() -> anyhow::Result<()> {
        let mut second_topic = "a".repeat(300);
        second_topic.replace_range(299.., "b");
        let first =
            ObjectKey::for_json_object("", "events", "partition=0", &"a".repeat(300), 0, 0)?;
        let replay =
            ObjectKey::for_json_object("", "events", "partition=0", &"a".repeat(300), 0, 0)?;
        let second = ObjectKey::for_json_object("", "events", "partition=0", &second_topic, 0, 0)?;

        assert_eq!(first.as_str(), replay.as_str());
        assert_ne!(first.as_str(), second.as_str());
        assert!(first.as_str().len() <= MAX_OBJECT_KEY_BYTES);
        assert!(first.as_str().contains("~sha256="));
        Ok(())
    }

    #[test]
    fn path_validation_is_part_of_final_key_construction() {
        let error = ObjectKey::for_json_object("", "events", "../escape", "topic", 0, 0)
            .expect_err("relative path segments must not reach object-store I/O");
        assert!(error.to_string().contains("invalid S3 object key"));
    }

    #[test]
    fn dynamic_path_bounds_segments_without_flattening_hierarchy() -> anyhow::Result<()> {
        let first = bounded_dynamic_path(&format!("year=2024/value={}", "a".repeat(400)))?;
        let replay = bounded_dynamic_path(&format!("year=2024/value={}", "a".repeat(400)))?;
        let second = bounded_dynamic_path(&format!("year=2024/value={}", "b".repeat(400)))?;

        assert_eq!(first, replay);
        assert_ne!(first, second);
        assert!(first.starts_with("year=2024/"));
        assert!(first
            .split('/')
            .all(|segment| segment.len() <= MAX_DYNAMIC_COMPONENT_BYTES));
        Ok(())
    }
}
