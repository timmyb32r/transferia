mod config;
mod pipeline;

use crate::{LogExportConfig, RawLogExportConfig};

fn raw_config() -> RawLogExportConfig {
    RawLogExportConfig {
        queue_capacity_records: 16,
        queue_capacity_bytes: 4096,
        max_record_bytes: 256,
        max_batch_records: 8,
        max_batch_bytes: 1024,
        flush_interval_ms: 10,
        write_timeout_ms: 1_000,
        max_write_attempts: 3,
        retry_backoff_min_ms: 20,
        retry_backoff_max_ms: 80,
        shutdown_timeout_ms: 5_000,
    }
}

fn config(adjust: impl FnOnce(&mut RawLogExportConfig)) -> LogExportConfig {
    let mut raw = raw_config();
    adjust(&mut raw);
    LogExportConfig::try_from(raw).expect("test limits are valid")
}
