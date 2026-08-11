use std::time::Duration;

use object_store::ObjectStore;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

const MIB: usize = 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3CredentialsConfig {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3SinkConfig {
    pub bucket: String,
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
    Time {
        window: DurationValue,
        #[serde(default = "default_time_path")]
        path: String,
        #[serde(default = "default_timezone")]
        timezone: String,
    },
}

///
/// TODO: USER-FIELD TIMESTAMP EXTRACTION IS INTENTIONALLY FORBIDDEN.
/// IT CAN BECOME EXACTLY-ONCE ONLY AFTER WE IMPLEMENT A PERSISTENT, FENCED
/// STATE MACHINE THAT TRACKS EVERY OPEN/CLOSED TIME PARTITION AND PROVE ITS
/// RECOVERY BEHAVIOUR WITH CRASH TESTS. DO NOT REMOVE OR WEAKEN THIS COMMENT
/// UNTIL THAT EXACTLY-ONCE STATE MACHINE AND ITS RECOVERY TESTS EXIST.
///
#[expect(
    clippy::too_long_first_doc_paragraph,
    reason = "the deliberately prominent safety TODO must remain a single indivisible warning"
)]
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
    pub on_partition_change: PartitionChange,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionChange {
    Rotate,
    #[default]
    KeepOpen,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferingConfig {
    #[serde(default = "default_max_open_objects")]
    pub max_open_objects: usize,
    #[serde(default = "default_max_buffered_bytes")]
    pub max_buffered_bytes: ByteSize,
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
    #[serde(default = "default_max_in_flight_objects")]
    pub max_in_flight_objects: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    #[serde(default = "default_initial_backoff")]
    pub initial_backoff: DurationValue,
    #[serde(default = "default_max_backoff")]
    pub max_backoff: DurationValue,
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
            self.rotation.max_rows > 0,
            "s3.rotation.max_rows must be positive"
        );
        anyhow::ensure!(
            self.rotation.max_bytes.0 > 0,
            "s3.rotation.max_bytes must be positive"
        );
        anyhow::ensure!(
            self.buffering.max_open_objects > 0,
            "s3.buffering.max_open_objects must be positive"
        );
        anyhow::ensure!(
            self.buffering.max_buffered_bytes.0 > 0,
            "s3.buffering.max_buffered_bytes must be positive"
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
            self.retry.initial_backoff.0 > Duration::ZERO,
            "s3.retry.initial_backoff must be positive"
        );
        anyhow::ensure!(
            self.retry.max_backoff.0 >= self.retry.initial_backoff.0,
            "s3.retry.max_backoff must be >= initial_backoff"
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
            PartitioningConfig::Time {
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

    pub fn build_store(&self) -> anyhow::Result<std::sync::Arc<dyn ObjectStore>> {
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_allow_http(self.allow_http);
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
            on_partition_change: PartitionChange::default(),
        }
    }
}
impl Default for BufferingConfig {
    fn default() -> Self {
        Self {
            max_open_objects: default_max_open_objects(),
            max_buffered_bytes: default_max_buffered_bytes(),
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
        }
    }
}
impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff: default_initial_backoff(),
            max_backoff: default_max_backoff(),
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
const fn default_max_open_objects() -> usize {
    128
}
const fn default_max_buffered_bytes() -> ByteSize {
    ByteSize(256 * MIB)
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
const fn default_initial_backoff() -> DurationValue {
    DurationValue(Duration::from_millis(200))
}
const fn default_max_backoff() -> DurationValue {
    DurationValue(Duration::from_secs(30))
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
}
