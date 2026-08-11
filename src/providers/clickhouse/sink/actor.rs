use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use futures_util::future::BoxFuture;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use super::transport::{InsertError, InsertTransport, NativeTransport};
use crate::metrics::SinkCounters;
use crate::pipeline::sink::{Delivery, DeliveryId, Sink, SinkBatch, SinkEvent, SinkIo};
use crate::providers::clickhouse::connection::ReconnectingClient;
use crate::providers::clickhouse::ClickHouseSinkConfig;

struct BufferedBatch {
    delivery_id: DeliveryId,
    batch: SinkBatch,
}

struct TableBuffer {
    table: Arc<str>,
    first_seen: Instant,
    rows: usize,
    bytes: usize,
    batches: Vec<BufferedBatch>,
}

struct DeliveryProgress {
    remaining_outputs: usize,
    source_messages: u64,
}

struct ActiveInsert {
    table: Arc<str>,
    rows: usize,
    bytes: usize,
    batches: Vec<BufferedBatch>,
}

struct InsertFailure {
    error: anyhow::Error,
}

pub struct ClickHouseSink {
    transport: Arc<dyn InsertTransport>,
    config: ClickHouseSinkConfig,
    counters: Arc<SinkCounters>,
    buffers: HashMap<Arc<str>, TableBuffer>,
    progress: BTreeMap<DeliveryId, DeliveryProgress>,
    next_received: DeliveryId,
    next_ack: DeliveryId,
    last_insert_started: Option<Instant>,
}

impl ClickHouseSink {
    pub async fn new(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
    ) -> anyhow::Result<Self> {
        let client = Arc::new(ReconnectingClient::connect(&config).await?);
        tracing::info!(
            "Connected to ClickHouse at {} (one connection per partition)",
            config.connection_string
        );
        Ok(Self::with_transport(
            config,
            counters,
            Arc::new(NativeTransport::new(client)),
        ))
    }

    #[must_use]
    pub fn with_transport(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
        transport: Arc<dyn InsertTransport>,
    ) -> Self {
        Self {
            transport,
            config,
            counters,
            buffers: HashMap::new(),
            progress: BTreeMap::new(),
            next_received: DeliveryId::new(1),
            next_ack: DeliveryId::new(1),
            last_insert_started: None,
        }
    }

    fn accept(&mut self, delivery: Delivery) -> anyhow::Result<()> {
        anyhow::ensure!(
            delivery.id == self.next_received,
            "sink delivery order violation: expected {}, got {}",
            self.next_received.get(),
            delivery.id.get(),
        );
        self.next_received = self.next_received.next();
        let remaining_outputs = delivery
            .outputs
            .iter()
            .filter(|output| output.batch.num_rows() > 0)
            .count();
        self.progress.insert(
            delivery.id,
            DeliveryProgress {
                remaining_outputs,
                source_messages: delivery.meta.source_messages,
            },
        );
        for batch in delivery
            .outputs
            .into_iter()
            .filter(|output| output.batch.num_rows() > 0)
        {
            let table = Arc::clone(&batch.table);
            let rows = batch.rows();
            let bytes = batch.bytes();
            let buffer = self
                .buffers
                .entry(Arc::clone(&table))
                .or_insert_with(|| TableBuffer {
                    table,
                    first_seen: Instant::now(),
                    rows: 0,
                    bytes: 0,
                    batches: Vec::new(),
                });
            buffer.rows = buffer.rows.saturating_add(rows);
            buffer.bytes = buffer.bytes.saturating_add(bytes);
            buffer.batches.push(BufferedBatch {
                delivery_id: delivery.id,
                batch,
            });
        }
        Ok(())
    }

    fn next_flush(&self, input_closed: bool, memory_pressure: bool) -> Option<(Arc<str>, Instant)> {
        let interval = Duration::from_millis(self.config.flush_interval_ms);
        let rate_limit = self
            .last_insert_started
            .map_or_else(Instant::now, |last| last + interval);
        self.buffers
            .values()
            .map(|buffer| {
                let full = memory_pressure
                    || buffer.rows >= self.config.max_insert_rows
                    || buffer.bytes >= self.config.max_insert_bytes;
                let wanted = if input_closed || full {
                    Instant::now()
                } else {
                    buffer.first_seen + interval
                };
                (Arc::clone(&buffer.table), wanted.max(rate_limit))
            })
            .min_by_key(|(_, deadline)| *deadline)
    }

    fn take_insert(&mut self, table: &Arc<str>) -> anyhow::Result<ActiveInsert> {
        let buffer = self
            .buffers
            .remove(table)
            .ok_or_else(|| anyhow::anyhow!("missing ClickHouse buffer for table '{table}'"))?;
        self.last_insert_started = Some(Instant::now());
        Ok(ActiveInsert {
            table: buffer.table,
            rows: buffer.rows,
            bytes: buffer.bytes,
            batches: buffer.batches,
        })
    }

    fn start_insert(
        &self,
        active: ActiveInsert,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> JoinHandle<Result<ActiveInsert, InsertFailure>> {
        let transport = Arc::clone(&self.transport);
        let counters = Arc::clone(&self.counters);
        let config = self.config.clone();
        tokio::spawn(async move {
            let mut attempts = 0_u32;
            let mut backoff = Duration::from_millis(config.retry_initial_ms.max(1));
            loop {
                attempts = attempts.saturating_add(1);
                let batches = active
                    .batches
                    .iter()
                    .map(|buffered| buffered.batch.batch.clone())
                    .collect();
                let started = std::time::Instant::now();
                let result = tokio::select! {
                    () = cancellation.cancelled() => {
                        return Err(InsertFailure { error: anyhow::anyhow!("ClickHouse insert cancelled") });
                    }
                    result = transport.insert(Arc::clone(&active.table), batches) => result,
                };
                counters.add_busy(started.elapsed());
                match result {
                    Ok(()) => {
                        counters.add_rows(active.rows as u64);
                        counters.add_bytes(active.bytes as u64);
                        counters.add_flush();
                        return Ok(active);
                    }
                    Err(InsertError::Permanent(error)) => return Err(InsertFailure { error }),
                    Err(InsertError::Transient(error)) => {
                        if config.retry_max_attempts.is_some_and(|max| attempts >= max) {
                            return Err(InsertFailure {
                                error: error.context("ClickHouse retry limit exhausted"),
                            });
                        }
                        tracing::warn!(
                            attempts,
                            backoff_ms = backoff.as_millis() as u64,
                            "ClickHouse INSERT failed, retrying: {error}"
                        );
                        tokio::select! {
                            () = cancellation.cancelled() => {
                                return Err(InsertFailure { error: anyhow::anyhow!("ClickHouse retry cancelled") });
                            }
                            () = tokio::time::sleep(backoff) => {}
                        }
                        backoff = backoff
                            .saturating_mul(2)
                            .min(Duration::from_millis(config.retry_max_ms.max(1)));
                    }
                }
            }
        })
    }

    fn complete_insert(&mut self, active: ActiveInsert) -> anyhow::Result<()> {
        for buffered in active.batches {
            let progress = self
                .progress
                .get_mut(&buffered.delivery_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("missing delivery progress {}", buffered.delivery_id.get())
                })?;
            progress.remaining_outputs = progress
                .remaining_outputs
                .checked_sub(1)
                .ok_or_else(|| anyhow::anyhow!("delivery output underflow"))?;
            drop(buffered);
        }
        tracing::info!(rows = active.rows, bytes = active.bytes, table = %active.table, "ClickHouse INSERT completed");
        Ok(())
    }

    async fn emit_committed(
        &mut self,
        events: &tokio::sync::mpsc::Sender<SinkEvent>,
    ) -> anyhow::Result<()> {
        let mut committed = None;
        let mut source_messages = 0_u64;
        while self
            .progress
            .get(&self.next_ack)
            .is_some_and(|progress| progress.remaining_outputs == 0)
        {
            let progress = self
                .progress
                .remove(&self.next_ack)
                .ok_or_else(|| anyhow::anyhow!("missing completed delivery"))?;
            source_messages = source_messages.saturating_add(progress.source_messages);
            committed = Some(self.next_ack);
            self.next_ack = self.next_ack.next();
        }
        if let Some(id) = committed {
            self.counters.add_unique_offsets(source_messages);
            events
                .send(SinkEvent::CommittedThrough(id))
                .await
                .map_err(|_| anyhow::anyhow!("sink event receiver closed"))?;
        }
        Ok(())
    }

    async fn run_actor(mut self, mut io: SinkIo) -> anyhow::Result<()> {
        let mut active: Option<JoinHandle<Result<ActiveInsert, InsertFailure>>> = None;
        let mut input_closed = false;
        loop {
            self.emit_committed(&io.events).await?;

            if let Some(mut task) = active.take() {
                let mut completed = None;
                tokio::select! {
                    () = io.cancellation.cancelled() => {
                        task.abort();
                        return Ok(());
                    }
                    result = &mut task => completed = Some(result),
                    delivery = io.deliveries.recv(), if !input_closed => {
                        match delivery {
                            Some(delivery) => self.accept(delivery)?,
                            None => input_closed = true,
                        }
                    }
                }
                if let Some(result) = completed {
                    match result.map_err(|error| {
                        anyhow::anyhow!("ClickHouse insert task failed: {error}")
                    })? {
                        Ok(insert) => self.complete_insert(insert)?,
                        Err(failure) => return Err(failure.error),
                    }
                } else {
                    active = Some(task);
                }
                continue;
            }

            if input_closed && self.buffers.is_empty() {
                self.emit_committed(&io.events).await?;
                anyhow::ensure!(
                    self.progress.is_empty(),
                    "sink input closed with incomplete deliveries"
                );
                return Ok(());
            }

            let memory_pressure = io.memory.used() >= io.memory.limit();
            let Some((table, deadline)) = self.next_flush(input_closed, memory_pressure) else {
                tokio::select! {
                    () = io.cancellation.cancelled() => return Ok(()),
                    delivery = io.deliveries.recv(), if !input_closed => {
                        match delivery {
                            Some(delivery) => self.accept(delivery)?,
                            None => input_closed = true,
                        }
                    }
                }
                continue;
            };

            tokio::select! {
                () = io.cancellation.cancelled() => return Ok(()),
                delivery = io.deliveries.recv(), if !input_closed => {
                    match delivery {
                        Some(delivery) => self.accept(delivery)?,
                        None => input_closed = true,
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    let insert = self.take_insert(&table)?;
                    active = Some(self.start_insert(insert, io.cancellation.clone()));
                }
            }
        }
    }
}

impl Sink for ClickHouseSink {
    fn run(self: Box<Self>, io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move { self.run_actor(io).await })
    }
}
