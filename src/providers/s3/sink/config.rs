use core::fmt;
use std::time::Duration;

use object_store::ObjectStore;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

const MIB: usize = 1024 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3CredentialsConfig {
    pub access_key: String,
    pub secret_key: String,
}

impl fmt::Debug for S3CredentialsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3CredentialsConfig")
            .field("access_key", &"[REDACTED]")
            .field("secret_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3SinkConfig {
    pub bucket: String,
    /// Version of the deterministic object key/payload/epoch contract. Keep
    /// this pinned while uncommitted source data can replay.
    #[serde(default = "default_object_layout_version")]
    pub object_layout_version: u32,
    #[serde(default)]
    pub prefix: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default)]
    pub credentials: Option<S3CredentialsConfig>,
    #[serde(default)]
    pub partitioning: PartitioningConfig,
    #[serde(default)]
    pub rotation: RotationConfig,
    #[serde(default)]
    pub buffering: BufferingConfig,
    #[serde(default)]
    pub upload: UploadConfig,
    #[serde(default)]
    pub retry: RetryConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PartitioningConfig {
    #[default]
    Source,
    Fields {
        columns: Vec<String>,
    },
    RecordTime {
        window: DurationValue,
        #[serde(default = "default_time_path")]
        path: String,
        #[serde(default = "default_timezone")]
        timezone: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RotationConfig {
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    #[serde(default = "default_max_object_bytes")]
    pub max_bytes: ByteSize,
    #[serde(default)]
    pub record_time_interval: Option<DurationValue>,
    #[serde(default)]
    pub wall_clock_interval: Option<DurationValue>,
    #[serde(default)]
    pub on_partition_path_change: PartitionPathChange,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionPathChange {
    Rotate,
    #[default]
    KeepEpoch,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferingConfig {
    /// Soft deterministic limit for open objects in one partition actor.
    #[serde(default = "default_max_epoch_buffers")]
    pub max_epoch_buffers: usize,
    /// Soft admission limit for pending uploads in one partition actor. An
    /// atomic source message may temporarily take the actor above this value;
    /// pending upload timing never changes deterministic object boundaries.
    #[serde(default = "default_max_pending_upload_objects")]
    pub max_pending_upload_objects: usize,
    /// Soft admission limit for serialized bytes in one partition actor.
    #[serde(default = "default_max_buffered_bytes")]
    pub max_buffered_bytes: ByteSize,
    /// Stable limit for serialized payload plus retained routing metadata in
    /// one epoch. Metadata is measured as its UTF-8 lengths plus a fixed
    /// 128-byte logical overhead per row, independent of Rust's ABI. When
    /// omitted, the limit is derived only from sink configuration, never from
    /// runtime memory state.
    #[serde(default)]
    pub max_epoch_bytes: Option<ByteSize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadConfig {
    #[serde(default = "default_multipart_threshold")]
    pub multipart_threshold: ByteSize,
    #[serde(default = "default_part_size")]
    pub part_size: ByteSize,
    #[serde(default = "default_parallel_parts")]
    pub parallel_parts: usize,
    /// Maximum concurrent object uploads in one partition actor.
    #[serde(default = "default_max_in_flight_objects")]
    pub max_in_flight_objects: usize,
    /// Deadline applied independently to each object-store request.
    #[serde(default = "default_operation_timeout")]
    pub operation_timeout: DurationValue,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    #[serde(default = "default_initial_backoff")]
    pub initial_backoff: DurationValue,
    #[serde(default = "default_max_backoff")]
    pub max_backoff: DurationValue,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationValue(pub Duration);

#[derive(Deserialize)]
#[serde(untagged)]
enum HumanValue {
    Integer(u64),
    Text(String),
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = HumanValue::deserialize(deserializer)?;
        let bytes = match raw {
            HumanValue::Integer(value) => value,
            HumanValue::Text(value) => parse_human_value(
                &value,
                &[
                    ("KiB", 1024),
                    ("MiB", 1024 * 1024),
                    ("GiB", 1024 * 1024 * 1024),
                    ("B", 1),
                ],
            )
            .map_err(D::Error::custom)?,
        };
        usize::try_from(bytes)
            .map(Self)
            .map_err(|_| D::Error::custom("byte size does not fit usize"))
    }
}

impl<'de> Deserialize<'de> for DurationValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = HumanValue::deserialize(deserializer)?;
        let millis = match raw {
            HumanValue::Integer(value) => value,
            HumanValue::Text(value) => parse_human_value(
                &value,
                &[
                    ("ms", 1),
                    ("s", 1000),
                    ("m", 60_000),
                    ("h", 3_600_000),
                    ("d", 86_400_000),
                ],
            )
            .map_err(D::Error::custom)?,
        };
        Ok(Self(Duration::from_millis(millis)))
    }
}

fn parse_human_value(value: &str, suffixes: &[(&str, u64)]) -> anyhow::Result<u64> {
    for &(suffix, multiplier) in suffixes {
        if let Some(number) = value.strip_suffix(suffix) {
            let number = number.trim().parse::<u64>()?;
            return number
                .checked_mul(multiplier)
                .ok_or_else(|| anyhow::anyhow!("value '{value}' overflows u64"));
        }
    }
    anyhow::bail!("unsupported value '{value}'")
}

impl S3SinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.bucket.is_empty(), "s3.bucket must not be empty");
        anyhow::ensure!(
            self.object_layout_version == default_object_layout_version(),
            "unsupported s3.object_layout_version {}; this binary supports only version {}",
            self.object_layout_version,
            default_object_layout_version()
        );
        anyhow::ensure!(
            self.rotation.max_rows > 0,
            "s3.rotation.max_rows must be positive"
        );
        anyhow::ensure!(
            self.rotation.max_bytes.0 > 0,
            "s3.rotation.max_bytes must be positive"
        );
        anyhow::ensure!(
            self.buffering.max_epoch_buffers > 0,
            "s3.buffering.max_epoch_buffers must be positive"
        );
        anyhow::ensure!(
            self.buffering.max_pending_upload_objects > 0,
            "s3.buffering.max_pending_upload_objects must be positive"
        );
        anyhow::ensure!(
            self.buffering.max_buffered_bytes.0 > 0,
            "s3.buffering.max_buffered_bytes must be positive"
        );
        if let Some(max_epoch_bytes) = self.buffering.max_epoch_bytes {
            anyhow::ensure!(
                max_epoch_bytes.0 > 0,
                "s3.buffering.max_epoch_bytes must be positive"
            );
        }
        anyhow::ensure!(
            self.epoch_byte_limit() <= self.buffering.max_buffered_bytes.0,
            "effective s3.buffering.max_epoch_bytes must not exceed max_buffered_bytes; \
             set max_epoch_bytes explicitly when using a buffer below 128MiB"
        );
        anyhow::ensure!(
            self.upload.multipart_threshold.0 > 0,
            "s3.upload.multipart_threshold must be positive"
        );
        anyhow::ensure!(
            self.upload.part_size.0 >= 5 * MIB,
            "s3.upload.part_size must be at least 5MiB"
        );
        anyhow::ensure!(
            self.upload.parallel_parts > 0,
            "s3.upload.parallel_parts must be positive"
        );
        anyhow::ensure!(
            self.upload.max_in_flight_objects > 0,
            "s3.upload.max_in_flight_objects must be positive"
        );
        anyhow::ensure!(
            self.upload.operation_timeout.0 > Duration::ZERO,
            "s3.upload.operation_timeout must be positive"
        );
        anyhow::ensure!(
            self.retry.initial_backoff.0 > Duration::ZERO,
            "s3.retry.initial_backoff must be positive"
        );
        anyhow::ensure!(
            self.retry.max_backoff.0 >= self.retry.initial_backoff.0,
            "s3.retry.max_backoff must be >= initial_backoff"
        );
        anyhow::ensure!(
            self.retry.max_attempts > 0,
            "s3.retry.max_attempts must be positive"
        );
        match &self.partitioning {
            PartitioningConfig::Fields { columns } => {
                anyhow::ensure!(
                    !columns.is_empty(),
                    "s3.partitioning.columns must not be empty"
                );
                let unique = columns.iter().collect::<std::collections::HashSet<_>>();
                anyhow::ensure!(
                    unique.len() == columns.len(),
                    "s3.partitioning.columns contains duplicates"
                );
            }
            PartitioningConfig::RecordTime {
                window,
                path,
                timezone,
            } => {
                anyhow::ensure!(
                    window.0 > Duration::ZERO,
                    "s3.partitioning.window must be positive"
                );
                anyhow::ensure!(!path.is_empty(), "s3.partitioning.path must not be empty");
                let _: chrono_tz::Tz = timezone
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid IANA timezone '{timezone}'"))?;
            }
            PartitioningConfig::Source => {}
        }
        Ok(())
    }

    #[must_use]
    pub(super) fn epoch_byte_limit(&self) -> usize {
        self.buffering
            .max_epoch_bytes
            .unwrap_or_else(default_max_epoch_bytes)
            .0
    }

    pub fn build_store(&self) -> anyhow::Result<std::sync::Arc<dyn ObjectStore>> {
        let store_retry = object_store::RetryConfig {
            // Whole-object retry, jitter, cancellation and metrics are owned
            // by `upload_with_retry`; hidden SDK attempts would multiply that
            // budget and make the configured attempt count inaccurate.
            max_retries: 0,
            ..object_store::RetryConfig::default()
        };
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_allow_http(self.allow_http)
            .with_retry(store_retry);
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(credentials) = &self.credentials {
            builder = builder
                .with_access_key_id(&credentials.access_key)
                .with_secret_access_key(&credentials.secret_key);
        }
        Ok(std::sync::Arc::new(builder.build()?))
    }
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            max_rows: default_max_rows(),
            max_bytes: default_max_object_bytes(),
            record_time_interval: None,
            wall_clock_interval: None,
            on_partition_path_change: PartitionPathChange::default(),
        }
    }
}
impl Default for BufferingConfig {
    fn default() -> Self {
        Self {
            max_epoch_buffers: default_max_epoch_buffers(),
            max_pending_upload_objects: default_max_pending_upload_objects(),
            max_buffered_bytes: default_max_buffered_bytes(),
            max_epoch_bytes: None,
        }
    }
}
impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            multipart_threshold: default_multipart_threshold(),
            part_size: default_part_size(),
            parallel_parts: default_parallel_parts(),
            max_in_flight_objects: default_max_in_flight_objects(),
            operation_timeout: default_operation_timeout(),
        }
    }
}
impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff: default_initial_backoff(),
            max_backoff: default_max_backoff(),
            max_attempts: default_max_attempts(),
        }
    }
}

fn default_region() -> String {
    "us-east-1".into()
}
fn default_timezone() -> String {
    "UTC".into()
}
fn default_time_path() -> String {
    "year=%Y/month=%m/day=%d/hour=%H".into()
}
const fn default_max_rows() -> usize {
    100_000
}
const fn default_max_object_bytes() -> ByteSize {
    ByteSize(128 * MIB)
}
const fn default_max_epoch_buffers() -> usize {
    128
}
const fn default_max_pending_upload_objects() -> usize {
    1024
}
const fn default_max_buffered_bytes() -> ByteSize {
    ByteSize(256 * MIB)
}
const fn default_max_epoch_bytes() -> ByteSize {
    ByteSize(128 * MIB)
}
const fn default_multipart_threshold() -> ByteSize {
    ByteSize(25 * MIB)
}
const fn default_part_size() -> ByteSize {
    ByteSize(25 * MIB)
}
const fn default_parallel_parts() -> usize {
    4
}
const fn default_max_in_flight_objects() -> usize {
    4
}
const fn default_operation_timeout() -> DurationValue {
    DurationValue(Duration::from_mins(1))
}
const fn default_initial_backoff() -> DurationValue {
    DurationValue(Duration::from_millis(200))
}
const fn default_max_backoff() -> DurationValue {
    DurationValue(Duration::from_secs(30))
}
const fn default_object_layout_version() -> u32 {
    3
}
const fn default_max_attempts() -> usize {
    10
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_parallel_object_uploads() -> anyhow::Result<()> {
        let config: S3SinkConfig =
            serde_yaml::from_str("bucket: test\nupload: { max_in_flight_objects: 0 }\n")?;
        let error = config.validate().expect_err("zero concurrency must fail");
        assert!(error.to_string().contains("max_in_flight_objects"));
        Ok(())
    }

    #[test]
    fn rejects_zero_operation_timeout() -> anyhow::Result<()> {
        let config: S3SinkConfig =
            serde_yaml::from_str("bucket: test\nupload: { operation_timeout: 0ms }\n")?;
        let error = config.validate().expect_err("zero timeout must fail");
        assert!(error.to_string().contains("operation_timeout"));
        Ok(())
    }

    #[test]
    fn rejects_unknown_object_layout_version() -> anyhow::Result<()> {
        let config: S3SinkConfig =
            serde_yaml::from_str("bucket: test\nobject_layout_version: 1\n")?;
        let error = config
            .validate()
            .expect_err("unknown layout must not silently change replay semantics");
        assert!(error.to_string().contains("object_layout_version"));
        Ok(())
    }

    #[test]
    fn credentials_are_redacted_from_debug_output() -> anyhow::Result<()> {
        let config: S3SinkConfig = serde_yaml::from_str(
            "bucket: test\ncredentials: { access_key: visible-key, secret_key: secret-value }\n",
        )?;
        let debug = format!("{config:?}");
        assert!(!debug.contains("visible-key"));
        assert!(!debug.contains("secret-value"));
        assert!(debug.contains("[REDACTED]"));
        Ok(())
    }

    #[test]
    fn validates_explicit_epoch_boundary() -> anyhow::Result<()> {
        let config: S3SinkConfig = serde_yaml::from_str(
            "bucket: test\nbuffering: { max_buffered_bytes: 64MiB, max_epoch_bytes: 65MiB }\n",
        )?;
        let error = config
            .validate()
            .expect_err("epoch boundary beyond the total buffer must fail");
        assert!(error.to_string().contains("max_epoch_bytes"));
        Ok(())
    }

    #[test]
    fn rejects_zero_pending_upload_objects_and_retry_attempts() -> anyhow::Result<()> {
        let no_pending: S3SinkConfig =
            serde_yaml::from_str("bucket: test\nbuffering: { max_pending_upload_objects: 0 }\n")?;
        assert!(no_pending
            .validate()
            .expect_err("zero pending objects must fail")
            .to_string()
            .contains("max_pending_upload_objects"));

        let no_attempts: S3SinkConfig =
            serde_yaml::from_str("bucket: test\nretry: { max_attempts: 0 }\n")?;
        assert!(no_attempts
            .validate()
            .expect_err("zero retry attempts must fail")
            .to_string()
            .contains("max_attempts"));
        Ok(())
    }

    #[test]
    fn default_epoch_boundary_is_independent_of_operational_buffering() -> anyhow::Result<()> {
        let defaults: S3SinkConfig = serde_yaml::from_str("bucket: test\n")?;
        assert_eq!(defaults.epoch_byte_limit(), 128 * MIB);

        let config: S3SinkConfig =
            serde_yaml::from_str("bucket: test\nbuffering: { max_buffered_bytes: 32MiB }\n")?;
        assert_eq!(config.epoch_byte_limit(), 128 * MIB);
        assert!(config.validate().is_err());

        let explicit: S3SinkConfig = serde_yaml::from_str(
            "bucket: test\nbuffering: { max_buffered_bytes: 32MiB, max_epoch_bytes: 7MiB }\n",
        )?;
        assert_eq!(explicit.epoch_byte_limit(), 7 * MIB);
        explicit.validate()?;
        Ok(())
    }
}
