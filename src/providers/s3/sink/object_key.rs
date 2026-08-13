use object_store::path::{Path, PathPart};

/// Amazon S3 measures object-key length in UTF-8 bytes.
pub(super) const MAX_OBJECT_KEY_BYTES: usize = 1024;

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
        validate_path_component("source topic", topic)?;
        let filename = format!("{topic}+{source_partition}+{start_offset}.json");
        let key = if prefix.is_empty() {
            format!("{table}/{partition_path}/{filename}")
        } else {
            format!("{prefix}/{table}/{partition_path}/{filename}")
        };
        Self::parse(key)
    }

    pub(super) fn parse(key: impl Into<String>) -> anyhow::Result<Self> {
        let key = key.into();
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

/// Validate a user- or data-derived object-key component without rewriting it.
pub(super) fn validate_path_component(label: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!value.is_empty(), "S3 {label} must not be empty");
    PathPart::parse(value).map(|_| ()).map_err(|error| {
        anyhow::anyhow!("S3 {label} {value:?} is not a valid object-key path component: {error}")
    })
}

#[cfg(test)]
mod tests;
