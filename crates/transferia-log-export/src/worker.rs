use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context as _;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::LogExportConfig;
use crate::layer::LogExportLayer;
use crate::record::{LogField, LogRecord, LogRecordEncoder, LogSink, LogValue};

const DROP_REPORT_MESSAGE: &str = "log records were dropped before export";

tokio::task_local! {
    /// Set for the whole worker task. An event emitted under it is the export
    /// reporting on itself and must not be exported, or every write would
    /// produce another record to write.
    static EXPORT_WORKER: ();
}

pub fn in_export_worker() -> bool {
    EXPORT_WORKER.try_with(|()| ()).is_ok()
}

/// Spawns a task that inherits the export worker's exemption.
///
/// A transport opened by a [`LogSink`] spawns its I/O tasks with it: their
/// events would otherwise be exported and cause the next write. Called from
/// any other task, it is `tokio::spawn`.
pub fn spawn_inheriting_scope<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if in_export_worker() {
        tokio::spawn(EXPORT_WORKER.scope((), future))
    } else {
        tokio::spawn(future)
    }
}

/// Everything an export needs; [`start`] turns it into a running export.
pub struct LogExportSetup {
    pub config: LogExportConfig,

    pub encoder: Arc<dyn LogRecordEncoder>,

    pub sink: Box<dyn LogSink>,
}

/// Running export: the layer to install and the handle that stops it.
pub struct LogExport {
    layer: LogExportLayer,
    handle: LogExportHandle,
}

impl LogExport {
    #[must_use]
    pub fn into_parts(self) -> (LogExportLayer, LogExportHandle) {
        (self.layer, self.handle)
    }
}

/// Starts the export worker on the current Tokio runtime.
///
/// Fails outside a runtime and when the encoder cannot fit the report of
/// dropped records into `max_record_bytes`. Nothing is exported until the
/// returned layer is installed in a subscriber.
pub fn start(setup: LogExportSetup) -> anyhow::Result<LogExport> {
    let runtime = tokio::runtime::Handle::try_current()
        .context("log export must be started inside a Tokio runtime")?;
    let (sender, receiver) = mpsc::channel(setup.config.queue_capacity_records());
    let shared = Arc::new(Shared {
        encoder: setup.encoder,
        max_record_bytes: setup.config.max_record_bytes(),
        queue_capacity_bytes: setup.config.queue_capacity_bytes(),
        queued_bytes: AtomicUsize::new(0),
        counters: Counters::default(),
    });
    anyhow::ensure!(
        shared.encode_drop_report(Dropped::MAX).is_ok(),
        "log export cannot encode its report of dropped records within max_record_bytes {}",
        setup.config.max_record_bytes()
    );
    let shutdown = CancellationToken::new();
    let worker = Worker {
        config: setup.config,
        shared: Arc::clone(&shared),
        receiver,
        sink: setup.sink,
        shutdown: shutdown.clone(),
        carried: None,
        reported: Dropped::default(),
    };
    let worker = runtime.spawn(EXPORT_WORKER.scope((), worker.run()));
    Ok(LogExport {
        layer: LogExportLayer::new(Arc::clone(&shared), sender),
        handle: LogExportHandle {
            shared,
            shutdown,
            worker,
        },
    })
}

/// Stops a running export. Dropping it leaves the export running, and then
/// nothing writes what is queued when the runtime stops.
pub struct LogExportHandle {
    shared: Arc<Shared>,
    shutdown: CancellationToken,
    worker: JoinHandle<()>,
}

impl LogExportHandle {
    #[must_use]
    pub fn stats(&self) -> LogExportStats {
        self.shared.counters.snapshot()
    }

    /// Writes what is queued and stops the worker, both within the configured
    /// shutdown timeout. Records logged afterwards are not exported.
    pub async fn shutdown(self) -> LogExportStats {
        self.shutdown.cancel();
        if let Err(error) = self.worker.await {
            tracing::warn!(%error, "log export worker terminated abnormally");
        }
        self.shared.counters.snapshot()
    }
}

/// Cumulative counters of one export.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogExportStats {
    pub exported_records: u64,

    pub failed_writes: u64,

    pub dropped_queue_full: u64,

    pub dropped_oversized: u64,

    pub dropped_encode_failed: u64,

    /// Records of batches that exhausted their write attempts.
    pub dropped_write_failed: u64,

    /// Records still queued when the shutdown timeout expired.
    pub dropped_at_shutdown: u64,
}

#[derive(Default)]
struct Counters {
    exported_records: AtomicU64,
    failed_writes: AtomicU64,
    dropped_queue_full: AtomicU64,
    dropped_oversized: AtomicU64,
    dropped_encode_failed: AtomicU64,
    dropped_write_failed: AtomicU64,
    dropped_at_shutdown: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> LogExportStats {
        LogExportStats {
            exported_records: self.exported_records.load(Ordering::Relaxed),
            failed_writes: self.failed_writes.load(Ordering::Relaxed),
            dropped_queue_full: self.dropped_queue_full.load(Ordering::Relaxed),
            dropped_oversized: self.dropped_oversized.load(Ordering::Relaxed),
            dropped_encode_failed: self.dropped_encode_failed.load(Ordering::Relaxed),
            dropped_write_failed: self.dropped_write_failed.load(Ordering::Relaxed),
            dropped_at_shutdown: self.dropped_at_shutdown.load(Ordering::Relaxed),
        }
    }
}

/// State shared by the layer, on logging threads, and the worker.
pub struct Shared {
    encoder: Arc<dyn LogRecordEncoder>,
    max_record_bytes: usize,
    queue_capacity_bytes: usize,
    /// Bytes of records that are queued or in the batch being written.
    queued_bytes: AtomicUsize,
    counters: Counters,
}

impl Shared {
    pub(crate) const fn max_record_bytes(&self) -> usize {
        self.max_record_bytes
    }

    pub(crate) fn count_oversized(&self) {
        self.counters
            .dropped_oversized
            .fetch_add(1, Ordering::Relaxed);
    }

    fn encode(&self, record: &LogRecord<'_>) -> Result<Vec<u8>, Rejected> {
        let mut encoded = Vec::new();
        if self.encoder.encode(record, &mut encoded).is_err() || encoded.is_empty() {
            return Err(Rejected::EncodeFailed);
        }
        if encoded.len() > self.max_record_bytes {
            return Err(Rejected::Oversized);
        }
        Ok(encoded)
    }

    /// Encodes and queues one record without blocking; a record that does not
    /// fit is counted and dropped.
    pub(crate) fn submit(&self, record: &LogRecord<'_>, sender: &mpsc::Sender<Vec<u8>>) {
        let encoded = match self.encode(record) {
            Ok(encoded) => encoded,
            Err(Rejected::EncodeFailed) => {
                self.counters
                    .dropped_encode_failed
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(Rejected::Oversized) => {
                self.count_oversized();
                return;
            }
        };
        let size = encoded.len();
        let queued = self.queued_bytes.fetch_add(size, Ordering::Relaxed);
        if queued.saturating_add(size) > self.queue_capacity_bytes {
            self.release(size);
            self.counters
                .dropped_queue_full
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        match sender.try_send(encoded) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.release(size);
                self.counters
                    .dropped_queue_full
                    .fetch_add(1, Ordering::Relaxed);
            }
            // Shut down between the layer's check and here.
            Err(mpsc::error::TrySendError::Closed(_)) => self.release(size),
        }
    }

    fn release(&self, bytes: usize) {
        self.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Encodes the record that tells the destination what was dropped.
    fn encode_drop_report(&self, dropped: Dropped) -> Result<Vec<u8>, Rejected> {
        let fields = [
            ("queue_full", dropped.queue_full),
            ("oversized", dropped.oversized),
            ("encode_failed", dropped.encode_failed),
            ("write_failed", dropped.write_failed),
        ]
        .map(|(name, value)| LogField {
            name,
            value: LogValue::U64(value),
        });
        self.encode(&LogRecord {
            timestamp: SystemTime::now(),
            level: tracing::Level::WARN,
            target: module_path!(),
            file: Some(file!()),
            line: Some(line!()),
            message: DROP_REPORT_MESSAGE,
            fields: &fields,
        })
    }
}

enum Rejected {
    EncodeFailed,
    Oversized,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct Dropped {
    queue_full: u64,
    oversized: u64,
    encode_failed: u64,
    write_failed: u64,
}

impl Dropped {
    /// The longest report there can be.
    const MAX: Self = Self {
        queue_full: u64::MAX,
        oversized: u64::MAX,
        encode_failed: u64::MAX,
        write_failed: u64::MAX,
    };

    const fn of(stats: &LogExportStats) -> Self {
        Self {
            queue_full: stats.dropped_queue_full,
            oversized: stats.dropped_oversized,
            encode_failed: stats.dropped_encode_failed,
            write_failed: stats.dropped_write_failed,
        }
    }

    const fn since(self, earlier: Self) -> Self {
        Self {
            queue_full: self.queue_full.saturating_sub(earlier.queue_full),
            oversized: self.oversized.saturating_sub(earlier.oversized),
            encode_failed: self.encode_failed.saturating_sub(earlier.encode_failed),
            write_failed: self.write_failed.saturating_sub(earlier.write_failed),
        }
    }
}

#[derive(Default)]
struct Batch {
    records: Vec<Vec<u8>>,
    bytes: usize,
}

impl Batch {
    fn push(&mut self, record: Vec<u8>) {
        self.bytes = self.bytes.saturating_add(record.len());
        self.records.push(record);
    }
}

enum Delivery {
    Written,
    Abandoned,
    Interrupted,
}

struct Worker {
    config: LogExportConfig,
    shared: Arc<Shared>,
    receiver: mpsc::Receiver<Vec<u8>>,
    sink: Box<dyn LogSink>,
    shutdown: CancellationToken,
    /// Record received for the current batch that did not fit it.
    carried: Option<Vec<u8>>,
    /// Drop counters already reported to the destination.
    reported: Dropped,
}

impl Worker {
    async fn run(mut self) {
        let mut batch = Batch::default();
        let shutdown = self.shutdown.clone();
        let timeout = self.config.shutdown_timeout();
        // The limit covers everything after the request, a write in flight included.
        let deadline = async move {
            shutdown.cancelled().await;
            tokio::time::sleep(timeout).await;
        };
        let timed_out = tokio::select! {
            biased;
            () = self.work(&mut batch) => false,
            () = deadline => true,
        };
        self.receiver.close();
        if timed_out {
            tracing::warn!(
                timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                "log export shutdown timed out"
            );
        }
        let mut remaining = batch.records.len();
        let mut bytes = batch.bytes;
        if let Some(record) = self.carried.take() {
            remaining = remaining.saturating_add(1);
            bytes = bytes.saturating_add(record.len());
        }
        // After `close`, `recv` ends only when no sender is still publishing a record.
        while let Some(record) = self.receiver.recv().await {
            remaining = remaining.saturating_add(1);
            bytes = bytes.saturating_add(record.len());
        }
        self.shared.release(bytes);
        if remaining > 0 {
            self.shared.counters.dropped_at_shutdown.fetch_add(
                u64::try_from(remaining).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            tracing::warn!(
                records = remaining,
                "log export stopped with unexported records"
            );
        }
    }

    /// Exports until shutdown is requested, then writes what is still queued.
    async fn work(&mut self, batch: &mut Batch) {
        while self.collect(batch).await {
            match self.deliver(&batch.records).await {
                Delivery::Written => {
                    self.finish(batch, &self.shared.counters.exported_records);
                    self.report_dropped().await;
                }
                Delivery::Abandoned => {
                    self.finish(batch, &self.shared.counters.dropped_write_failed);
                }
                Delivery::Interrupted => break,
            }
        }
        self.receiver.close();
        self.drain(batch).await;
    }

    /// Fills `batch`: waits for the first record, then for more until a batch
    /// limit or the flush interval. Returns `false` once shutdown is requested.
    async fn collect(&mut self, batch: &mut Batch) -> bool {
        if batch.records.is_empty() {
            let first = match self.carried.take() {
                Some(record) => record,
                None => tokio::select! {
                    biased;
                    () = self.shutdown.cancelled() => return false,
                    received = self.receiver.recv() => match received {
                        Some(record) => record,
                        None => return false,
                    },
                },
            };
            batch.push(first);
        }
        let flush = tokio::time::sleep(self.config.flush_interval());
        tokio::pin!(flush);
        while batch.records.len() < self.config.max_batch_records() {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return false,
                () = &mut flush => break,
                received = self.receiver.recv() => {
                    let Some(record) = received else { return false };
                    if !self.fits(batch, &record) {
                        self.carried = Some(record);
                        break;
                    }
                    batch.push(record);
                }
            }
        }
        true
    }

    const fn fits(&self, batch: &Batch, record: &[u8]) -> bool {
        batch.bytes.saturating_add(record.len()) <= self.config.max_batch_bytes()
    }

    async fn deliver(&mut self, batch: &[Vec<u8>]) -> Delivery {
        let max_attempts = self.config.max_write_attempts();
        let mut backoff = self.config.retry_backoff_min();
        for attempt in 1..=max_attempts {
            let Err(error) = self.write(batch).await else {
                return Delivery::Written;
            };
            let dropped = Dropped::of(&self.shared.counters.snapshot());
            tracing::warn!(
                attempt,
                max_attempts,
                records = batch.len(),
                dropped_queue_full = dropped.queue_full,
                dropped_write_failed = dropped.write_failed,
                error = %format_args!("{error:#}"),
                "log export write failed"
            );
            if attempt == max_attempts {
                break;
            }
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Delivery::Interrupted,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff
                .saturating_mul(2)
                .min(self.config.retry_backoff_max());
        }
        Delivery::Abandoned
    }

    async fn write(&mut self, batch: &[Vec<u8>]) -> anyhow::Result<()> {
        let timeout = self.config.write_timeout();
        let result = tokio::time::timeout(timeout, self.sink.write(batch))
            .await
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "log sink write timed out after {} ms",
                    timeout.as_millis()
                ))
            });
        if result.is_err() {
            self.shared
                .counters
                .failed_writes
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Settles a batch as exported or dropped and frees its queue budget.
    fn finish(&self, batch: &mut Batch, counter: &AtomicU64) {
        counter.fetch_add(
            u64::try_from(batch.records.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.shared.release(batch.bytes);
        batch.records.clear();
        batch.bytes = 0;
    }

    /// Tells the destination how many records were dropped since the last
    /// report. The counters stay unreported until this record is written.
    async fn report_dropped(&mut self) {
        let current = Dropped::of(&self.shared.counters.snapshot());
        let dropped = current.since(self.reported);
        if dropped == Dropped::default() {
            return;
        }
        tracing::warn!(
            queue_full = dropped.queue_full,
            oversized = dropped.oversized,
            encode_failed = dropped.encode_failed,
            write_failed = dropped.write_failed,
            "{DROP_REPORT_MESSAGE}"
        );
        let Ok(encoded) = self.shared.encode_drop_report(dropped) else {
            return;
        };
        if self.write(std::slice::from_ref(&encoded)).await.is_ok() {
            self.reported = current;
        }
    }

    /// Writes what is still queued, one attempt per batch.
    async fn drain(&mut self, batch: &mut Batch) {
        loop {
            while batch.records.len() < self.config.max_batch_records() {
                let Some(record) = self
                    .carried
                    .take()
                    .or_else(|| self.receiver.try_recv().ok())
                else {
                    break;
                };
                if !batch.records.is_empty() && !self.fits(batch, &record) {
                    self.carried = Some(record);
                    break;
                }
                batch.push(record);
            }
            if batch.records.is_empty() {
                break;
            }
            if let Err(error) = self.write(&batch.records).await {
                tracing::warn!(
                    records = batch.records.len(),
                    error = %format_args!("{error:#}"),
                    "log export write failed during shutdown"
                );
                return;
            }
            self.finish(batch, &self.shared.counters.exported_records);
        }
        self.report_dropped().await;
    }
}
