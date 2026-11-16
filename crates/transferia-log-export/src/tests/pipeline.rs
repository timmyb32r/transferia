use std::io::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::Semaphore;
use tracing_subscriber::layer::SubscriberExt as _;

use super::config;
use crate::{
    start, LogExportConfig, LogExportHandle, LogExportSetup, LogRecord, LogRecordEncoder, LogSink,
    LogValue,
};

/// Renders `LEVEL message name=value ...\n`.
struct LineEncoder;

impl LogRecordEncoder for LineEncoder {
    fn encode(&self, record: &LogRecord<'_>, output: &mut Vec<u8>) -> anyhow::Result<()> {
        anyhow::ensure!(
            record.message != "unencodable",
            "encoder rejected the record"
        );
        write!(output, "{} {}", record.level, record.message)?;
        for field in record.fields {
            match &field.value {
                LogValue::Str(value) => write!(output, " {}={value}", field.name)?,
                LogValue::I64(value) => write!(output, " {}={value}", field.name)?,
                LogValue::U64(value) => write!(output, " {}={value}", field.name)?,
                LogValue::F64(value) => write!(output, " {}={value}", field.name)?,
                LogValue::Bool(value) => write!(output, " {}={value}", field.name)?,
            }
        }
        output.push(b'\n');
        Ok(())
    }
}

#[derive(Clone, Default)]
struct Destination {
    batches: Arc<Mutex<Vec<Vec<String>>>>,
    failures_left: Arc<AtomicUsize>,
    /// When set, every write waits for one permit.
    gate: Option<Arc<Semaphore>>,
    logs_while_writing: bool,
    logs_from_a_spawned_task: bool,
}

impl Destination {
    fn records(&self) -> Vec<String> {
        self.batches
            .lock()
            .expect("destination lock")
            .iter()
            .flatten()
            .cloned()
            .collect()
    }

    fn batch_sizes(&self) -> Vec<usize> {
        self.batches
            .lock()
            .expect("destination lock")
            .iter()
            .map(Vec::len)
            .collect()
    }
}

impl LogSink for Destination {
    fn write<'a>(&'a mut self, batch: &'a [Vec<u8>]) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.logs_while_writing {
                tracing::info!("sink is writing");
            }
            if self.logs_from_a_spawned_task {
                crate::spawn_inheriting_scope(async { tracing::info!("transport is writing") })
                    .await?;
            }
            if let Some(gate) = &self.gate {
                gate.acquire().await?.forget();
            }
            if self
                .failures_left
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                anyhow::bail!("destination is down");
            }
            self.batches.lock().expect("destination lock").push(
                batch
                    .iter()
                    .map(|record| String::from_utf8_lossy(record).into_owned())
                    .collect(),
            );
            Ok(())
        })
    }
}

fn export(
    config: LogExportConfig,
    destination: &Destination,
) -> (tracing::subscriber::DefaultGuard, LogExportHandle) {
    let (layer, handle) = start(LogExportSetup {
        config,
        encoder: Arc::new(LineEncoder),
        sink: Box::new(destination.clone()),
    })
    .expect("export starts inside the test runtime")
    .into_parts();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    (guard, handle)
}

/// Lets the worker run until it waits for records, the gate or a timer.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

#[tokio::test(start_paused = true)]
async fn exports_events_in_order_with_their_fields() {
    let destination = Destination::default();
    let (_guard, handle) = export(config(|_| {}), &destination);

    tracing::info!(answer = 42, name = "value", flag = true, "first");
    tracing::warn!(error = %"broken pipe", "second");
    let stats = handle.shutdown().await;

    assert_eq!(
        destination.records(),
        [
            "INFO first answer=42 name=value flag=true\n",
            "WARN second error=broken pipe\n",
        ]
    );
    assert_eq!(stats.exported_records, 2);
    assert_eq!(stats.failed_writes, 0);
}

#[tokio::test(start_paused = true)]
async fn batches_by_record_count_and_bytes() {
    let destination = Destination::default();
    let by_count = config(|raw| raw.max_batch_records = 2);
    let (guard, handle) = export(by_count, &destination);
    for index in 0..5 {
        tracing::info!("record {index}");
    }
    handle.shutdown().await;
    drop(guard);
    assert_eq!(destination.batch_sizes(), [2, 2, 1]);

    let destination = Destination::default();
    // Each record below is 103 bytes: two fit a 256-byte batch, three do not.
    let by_bytes = config(|raw| raw.max_batch_bytes = 256);
    let (_guard, handle) = export(by_bytes, &destination);
    for index in 0..3 {
        tracing::info!("record {index:090}");
    }
    handle.shutdown().await;
    assert_eq!(destination.batch_sizes(), [2, 1]);
}

#[tokio::test(start_paused = true)]
async fn oversized_and_unencodable_records_are_dropped_and_reported() {
    let destination = Destination::default();
    let (_guard, handle) = export(config(|_| {}), &destination);

    tracing::info!("{}", "x".repeat(300));
    tracing::info!("unencodable");
    tracing::info!("kept");
    let stats = handle.shutdown().await;

    assert_eq!(
        destination.records(),
        [
            "INFO kept\n",
            "WARN log records were dropped before export \
             queue_full=0 oversized=1 encode_failed=1 write_failed=0\n",
        ]
    );
    assert_eq!(stats.dropped_oversized, 1);
    assert_eq!(stats.dropped_encode_failed, 1);
    assert_eq!(stats.exported_records, 1);
}

#[tokio::test(start_paused = true)]
async fn full_queue_drops_new_records_without_blocking() {
    let gate = Arc::new(Semaphore::new(0));
    let destination = Destination {
        gate: Some(Arc::clone(&gate)),
        ..Destination::default()
    };
    let limits = config(|raw| {
        raw.queue_capacity_records = 2;
        raw.max_batch_records = 1;
        raw.write_timeout_ms = 60_000;
    });
    let (_guard, handle) = export(limits, &destination);

    tracing::info!("in flight");
    settle().await;
    tracing::info!("queued 1");
    tracing::info!("queued 2");
    tracing::info!("dropped");
    assert_eq!(handle.stats().dropped_queue_full, 1);

    gate.add_permits(16);
    let stats = handle.shutdown().await;

    assert_eq!(
        destination.records(),
        [
            "INFO in flight\n",
            "WARN log records were dropped before export \
             queue_full=1 oversized=0 encode_failed=0 write_failed=0\n",
            "INFO queued 1\n",
            "INFO queued 2\n",
        ]
    );
    assert_eq!(stats.exported_records, 3);
}

#[tokio::test(start_paused = true)]
async fn queue_byte_budget_drops_new_records() {
    let gate = Arc::new(Semaphore::new(0));
    let destination = Destination {
        gate: Some(Arc::clone(&gate)),
        ..Destination::default()
    };
    // Each record below is 213 bytes; the budget holds two of them.
    let limits = config(|raw| {
        raw.queue_capacity_bytes = 500;
        raw.max_batch_records = 1;
        raw.write_timeout_ms = 60_000;
    });
    let (_guard, handle) = export(limits, &destination);

    tracing::info!("record {:0200}", 0);
    settle().await;
    tracing::info!("record {:0200}", 1);
    tracing::info!("record {:0200}", 2);
    assert_eq!(handle.stats().dropped_queue_full, 1);

    gate.add_permits(16);
    let stats = handle.shutdown().await;
    assert_eq!(stats.exported_records, 2);
}

#[tokio::test(start_paused = true)]
async fn failed_write_is_retried_until_accepted() {
    let destination = Destination::default();
    destination.failures_left.store(2, Ordering::Relaxed);
    let (_guard, handle) = export(config(|_| {}), &destination);

    tracing::info!("survives the outage");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let stats = handle.shutdown().await;

    assert_eq!(destination.records(), ["INFO survives the outage\n"]);
    assert_eq!(stats.failed_writes, 2);
    assert_eq!(stats.exported_records, 1);
    assert_eq!(stats.dropped_write_failed, 0);
}

#[tokio::test(start_paused = true)]
async fn batch_is_dropped_after_the_last_attempt() {
    let destination = Destination::default();
    destination.failures_left.store(3, Ordering::Relaxed);
    let (_guard, handle) = export(config(|_| {}), &destination);

    tracing::info!("lost");
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("delivered");
    let stats = handle.shutdown().await;

    assert_eq!(
        destination.records(),
        [
            "INFO delivered\n",
            "WARN log records were dropped before export \
             queue_full=0 oversized=0 encode_failed=0 write_failed=1\n",
        ]
    );
    assert_eq!(stats.failed_writes, 3);
    assert_eq!(stats.dropped_write_failed, 1);
    assert_eq!(stats.exported_records, 1);
}

#[tokio::test(start_paused = true)]
async fn stuck_write_times_out_and_counts_as_a_failure() {
    let destination = Destination {
        gate: Some(Arc::new(Semaphore::new(0))),
        ..Destination::default()
    };
    let limits = config(|raw| raw.max_write_attempts = 1);
    let (_guard, handle) = export(limits, &destination);

    tracing::info!("never written");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let stats = handle.stats();
    handle.shutdown().await;

    assert!(destination.records().is_empty());
    assert_eq!(stats.failed_writes, 1);
    assert_eq!(stats.dropped_write_failed, 1);
}

#[tokio::test(start_paused = true)]
async fn events_of_the_export_worker_are_not_exported() {
    let destination = Destination {
        logs_while_writing: true,
        ..Destination::default()
    };
    destination.failures_left.store(1, Ordering::Relaxed);
    let (_guard, handle) = export(config(|_| {}), &destination);

    tracing::info!("application event");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let stats = handle.shutdown().await;

    assert_eq!(destination.records(), ["INFO application event\n"]);
    assert_eq!(stats.exported_records, 1);
}

#[tokio::test(start_paused = true)]
async fn events_of_tasks_spawned_for_the_sink_are_not_exported() {
    let destination = Destination {
        logs_from_a_spawned_task: true,
        ..Destination::default()
    };
    let (_guard, handle) = export(config(|_| {}), &destination);

    tracing::info!("application event");
    tokio::time::sleep(Duration::from_secs(1)).await;
    crate::spawn_inheriting_scope(async { tracing::info!("application task event") })
        .await
        .expect("task completes");
    let stats = handle.shutdown().await;

    assert_eq!(
        destination.records(),
        ["INFO application event\n", "INFO application task event\n"]
    );
    assert_eq!(stats.exported_records, 2);
}

#[tokio::test(start_paused = true)]
async fn shutdown_writes_what_is_queued() {
    let destination = Destination::default();
    let limits = config(|raw| raw.flush_interval_ms = 3_600_000);
    let (_guard, handle) = export(limits, &destination);

    for index in 0..3 {
        tracing::info!("record {index}");
    }
    let stats = handle.shutdown().await;

    assert_eq!(destination.records().len(), 3);
    assert_eq!(stats.exported_records, 3);
    assert_eq!(stats.dropped_at_shutdown, 0);
}

#[tokio::test(start_paused = true)]
async fn shutdown_gives_up_after_its_timeout() {
    let destination = Destination {
        gate: Some(Arc::new(Semaphore::new(0))),
        ..Destination::default()
    };
    let limits = config(|raw| {
        raw.write_timeout_ms = 60_000;
        raw.shutdown_timeout_ms = 100;
        raw.flush_interval_ms = 3_600_000;
    });
    let (_guard, handle) = export(limits, &destination);

    tracing::info!("first");
    tracing::info!("second");
    let stats = handle.shutdown().await;

    assert!(destination.records().is_empty());
    assert_eq!(stats.dropped_at_shutdown, 2);
}

#[tokio::test(start_paused = true)]
async fn shutdown_timeout_covers_a_write_in_flight() {
    let destination = Destination {
        gate: Some(Arc::new(Semaphore::new(0))),
        ..Destination::default()
    };
    let limits = config(|raw| {
        raw.write_timeout_ms = 60_000;
        raw.shutdown_timeout_ms = 100;
    });
    let (_guard, handle) = export(limits, &destination);

    tracing::info!("in flight");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let requested = tokio::time::Instant::now();
    let stats = handle.shutdown().await;

    assert_eq!(requested.elapsed(), Duration::from_millis(100));
    assert_eq!(stats.dropped_at_shutdown, 1);
}

#[tokio::test(start_paused = true)]
async fn records_logged_after_shutdown_are_not_exported() {
    let destination = Destination::default();
    let (_guard, handle) = export(config(|_| {}), &destination);

    handle.shutdown().await;
    tracing::info!("late");
    settle().await;

    assert!(destination.records().is_empty());
}

#[tokio::test]
async fn start_rejects_limits_that_cannot_hold_the_drop_report() {
    let result = start(LogExportSetup {
        config: config(|raw| raw.max_record_bytes = 64),
        encoder: Arc::new(LineEncoder),
        sink: Box::new(Destination::default()),
    });
    let error = result.err().expect("the report does not fit 64 bytes");
    assert!(
        error.to_string().contains("report of dropped records"),
        "{error}"
    );
}

#[test]
fn start_fails_outside_a_runtime() {
    let result = start(LogExportSetup {
        config: config(|_| {}),
        encoder: Arc::new(LineEncoder),
        sink: Box::new(Destination::default()),
    });
    assert!(result.is_err());
}
