use std::time::Duration;

use serde::Deserialize;

/// Unvalidated limits as written in configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawLogExportConfig {
    /// Encoded records that may wait for the sink.
    pub queue_capacity_records: usize,

    /// Bytes of encoded records that may wait for the sink, the batch being written included.
    pub queue_capacity_bytes: usize,

    /// Largest encoded record that is exported; a larger one is dropped and counted.
    pub max_record_bytes: usize,

    pub max_batch_records: usize,

    pub max_batch_bytes: usize,

    /// How long a batch waits for more records after its first one.
    pub flush_interval_ms: u64,

    /// Upper bound of one sink write; an expired write is retried like a failed one.
    pub write_timeout_ms: u64,

    /// Writes of one batch before it is dropped and counted.
    pub max_write_attempts: u32,

    pub retry_backoff_min_ms: u64,

    pub retry_backoff_max_ms: u64,

    /// Time allowed to write the queued records when the export is shut down.
    pub shutdown_timeout_ms: u64,
}

/// Validated limits of the export. Every value is positive, one record always
/// fits a batch and the queue, and the backoff range is ordered.
///
/// Records beyond the queue capacity, records above `max_record_bytes` and
/// batches that exhaust `max_write_attempts` are dropped; nothing here applies
/// backpressure to the code that logs.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "RawLogExportConfig")]
pub struct LogExportConfig {
    queue_capacity_records: usize,

    queue_capacity_bytes: usize,

    max_record_bytes: usize,

    max_batch_records: usize,

    max_batch_bytes: usize,

    flush_interval: Duration,

    write_timeout: Duration,

    max_write_attempts: u32,

    retry_backoff_min: Duration,

    retry_backoff_max: Duration,

    shutdown_timeout: Duration,
}

impl TryFrom<RawLogExportConfig> for LogExportConfig {
    type Error = anyhow::Error;

    fn try_from(raw: RawLogExportConfig) -> anyhow::Result<Self> {
        for (name, value) in [
            ("queue_capacity_records", raw.queue_capacity_records),
            ("queue_capacity_bytes", raw.queue_capacity_bytes),
            ("max_record_bytes", raw.max_record_bytes),
            ("max_batch_records", raw.max_batch_records),
            ("max_batch_bytes", raw.max_batch_bytes),
        ] {
            anyhow::ensure!(value > 0, "log export {name} must be positive");
        }
        for (name, value) in [
            ("flush_interval_ms", raw.flush_interval_ms),
            ("write_timeout_ms", raw.write_timeout_ms),
            ("retry_backoff_min_ms", raw.retry_backoff_min_ms),
            ("retry_backoff_max_ms", raw.retry_backoff_max_ms),
            ("shutdown_timeout_ms", raw.shutdown_timeout_ms),
        ] {
            anyhow::ensure!(value > 0, "log export {name} must be positive");
        }
        anyhow::ensure!(
            raw.queue_capacity_records <= tokio::sync::Semaphore::MAX_PERMITS,
            "log export queue_capacity_records must not exceed {}",
            tokio::sync::Semaphore::MAX_PERMITS
        );
        anyhow::ensure!(
            raw.max_write_attempts > 0,
            "log export max_write_attempts must be positive"
        );
        anyhow::ensure!(
            raw.max_batch_bytes >= raw.max_record_bytes,
            "log export max_batch_bytes must not be less than max_record_bytes"
        );
        anyhow::ensure!(
            raw.queue_capacity_bytes >= raw.max_record_bytes,
            "log export queue_capacity_bytes must not be less than max_record_bytes"
        );
        anyhow::ensure!(
            raw.retry_backoff_max_ms >= raw.retry_backoff_min_ms,
            "log export retry_backoff_max_ms must not be less than retry_backoff_min_ms"
        );
        Ok(Self {
            queue_capacity_records: raw.queue_capacity_records,
            queue_capacity_bytes: raw.queue_capacity_bytes,
            max_record_bytes: raw.max_record_bytes,
            max_batch_records: raw.max_batch_records,
            max_batch_bytes: raw.max_batch_bytes,
            flush_interval: Duration::from_millis(raw.flush_interval_ms),
            write_timeout: Duration::from_millis(raw.write_timeout_ms),
            max_write_attempts: raw.max_write_attempts,
            retry_backoff_min: Duration::from_millis(raw.retry_backoff_min_ms),
            retry_backoff_max: Duration::from_millis(raw.retry_backoff_max_ms),
            shutdown_timeout: Duration::from_millis(raw.shutdown_timeout_ms),
        })
    }
}

impl LogExportConfig {
    #[must_use]
    pub const fn queue_capacity_records(&self) -> usize {
        self.queue_capacity_records
    }

    #[must_use]
    pub const fn queue_capacity_bytes(&self) -> usize {
        self.queue_capacity_bytes
    }

    #[must_use]
    pub const fn max_record_bytes(&self) -> usize {
        self.max_record_bytes
    }

    #[must_use]
    pub const fn max_batch_records(&self) -> usize {
        self.max_batch_records
    }

    #[must_use]
    pub const fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    #[must_use]
    pub const fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    #[must_use]
    pub const fn write_timeout(&self) -> Duration {
        self.write_timeout
    }

    #[must_use]
    pub const fn max_write_attempts(&self) -> u32 {
        self.max_write_attempts
    }

    #[must_use]
    pub const fn retry_backoff_min(&self) -> Duration {
        self.retry_backoff_min
    }

    #[must_use]
    pub const fn retry_backoff_max(&self) -> Duration {
        self.retry_backoff_max
    }

    #[must_use]
    pub const fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }
}
