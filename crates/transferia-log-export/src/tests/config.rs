use std::time::Duration;

use super::raw_config;
use crate::{LogExportConfig, RawLogExportConfig};

const VALID_YAML: &str = "\
queue_capacity_records: 16
queue_capacity_bytes: 4096
max_record_bytes: 256
max_batch_records: 8
max_batch_bytes: 1024
flush_interval_ms: 10
write_timeout_ms: 1000
max_write_attempts: 3
retry_backoff_min_ms: 20
retry_backoff_max_ms: 80
shutdown_timeout_ms: 5000
";

fn rejection(adjust: impl FnOnce(&mut RawLogExportConfig)) -> String {
    let mut raw = raw_config();
    adjust(&mut raw);
    LogExportConfig::try_from(raw)
        .expect_err("limits must be rejected")
        .to_string()
}

#[test]
fn accepts_valid_limits() {
    let config = LogExportConfig::try_from(raw_config()).expect("limits are valid");
    assert_eq!(config.queue_capacity_records(), 16);
    assert_eq!(config.queue_capacity_bytes(), 4096);
    assert_eq!(config.max_record_bytes(), 256);
    assert_eq!(config.max_batch_records(), 8);
    assert_eq!(config.max_batch_bytes(), 1024);
    assert_eq!(config.flush_interval(), Duration::from_millis(10));
    assert_eq!(config.write_timeout(), Duration::from_secs(1));
    assert_eq!(config.max_write_attempts(), 3);
    assert_eq!(config.retry_backoff_min(), Duration::from_millis(20));
    assert_eq!(config.retry_backoff_max(), Duration::from_millis(80));
    assert_eq!(config.shutdown_timeout(), Duration::from_secs(5));
}

#[test]
fn rejects_zero_limits() {
    assert!(rejection(|raw| raw.queue_capacity_records = 0).contains("queue_capacity_records"));
    assert!(rejection(|raw| raw.queue_capacity_bytes = 0).contains("queue_capacity_bytes"));
    assert!(rejection(|raw| raw.max_record_bytes = 0).contains("max_record_bytes"));
    assert!(rejection(|raw| {
        raw.queue_capacity_records = usize::MAX;
    })
    .contains("queue_capacity_records"));
    assert!(rejection(|raw| raw.max_batch_records = 0).contains("max_batch_records"));
    assert!(rejection(|raw| raw.max_batch_bytes = 0).contains("max_batch_bytes"));
    assert!(rejection(|raw| raw.flush_interval_ms = 0).contains("flush_interval_ms"));
    assert!(rejection(|raw| raw.write_timeout_ms = 0).contains("write_timeout_ms"));
    assert!(rejection(|raw| raw.max_write_attempts = 0).contains("max_write_attempts"));
    assert!(rejection(|raw| raw.retry_backoff_min_ms = 0).contains("retry_backoff_min_ms"));
    assert!(rejection(|raw| raw.shutdown_timeout_ms = 0).contains("shutdown_timeout_ms"));
}

#[test]
fn rejects_a_record_that_cannot_fit_a_batch_or_the_queue() {
    assert!(rejection(|raw| raw.max_batch_bytes = 255).contains("max_batch_bytes"));
    assert!(rejection(|raw| raw.queue_capacity_bytes = 255).contains("queue_capacity_bytes"));
}

#[test]
fn rejects_an_unordered_backoff_range() {
    assert!(rejection(|raw| raw.retry_backoff_max_ms = 19).contains("retry_backoff_max_ms"));
}

#[test]
fn deserialization_validates_limits() {
    let config: LogExportConfig = serde_yaml::from_str(VALID_YAML).expect("limits are valid");
    assert_eq!(config.max_batch_bytes(), 1024);

    let invalid = VALID_YAML.replace("max_batch_bytes: 1024", "max_batch_bytes: 1");
    let error = serde_yaml::from_str::<LogExportConfig>(&invalid)
        .expect_err("a batch smaller than a record must be rejected");
    assert!(error.to_string().contains("max_batch_bytes"));
}

#[test]
fn deserialization_rejects_unknown_and_missing_limits() {
    let unknown = format!("{VALID_YAML}queue_capacity: 1\n");
    assert!(serde_yaml::from_str::<LogExportConfig>(&unknown).is_err());

    let missing = VALID_YAML.replace("max_write_attempts: 3\n", "");
    assert!(serde_yaml::from_str::<LogExportConfig>(&missing).is_err());
}
