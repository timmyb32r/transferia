use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use tokio::time::Instant;
use transferia_core::data::changelog::{project_sink_batch, ProjectedSinkBatch};
use transferia_core::delivery::{DeliveryDiscovery, SinkLimits};
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::sink::{Delivery, DeliveryId, Sink, SinkEvent, SinkIo};
use transferia_delivery_contracts::delivery_tracker::DeliveryTracker;
use transferia_delivery_contracts::metrics::SinkCounters;

use super::bulk::{retry_seed, write_bulk_with_retry, BulkTransport};
use super::config::OpenSearchSinkConfig;
use super::document::{encode_batch, BulkAction};

struct PendingAction {
    delivery_id: DeliveryId,

    action: BulkAction,

    _input_memory: Arc<transferia_core::memory::MemoryReservation>,

    _encoded_memory: Arc<transferia_core::memory::MemoryReservation>,
}

struct IndexBuffer {
    first_seen: Instant,

    rows: usize,

    bytes: usize,

    actions: VecDeque<PendingAction>,
}

pub(super) struct OpenSearchSink {
    config: Arc<OpenSearchSinkConfig>,

    transport: Arc<dyn BulkTransport>,

    counters: Arc<SinkCounters>,

    discovery: Arc<DeliveryDiscovery>,

    buffers: HashMap<Arc<str>, IndexBuffer>,

    /// Source primary keys are globally unique by delivery contract. This set
    /// detects violations exactly across the bounded buffered/in-flight window;
    /// retaining every completed key would require unbounded process memory.
    pending_identities: HashSet<(Arc<str>, Arc<str>)>,

    progress: DeliveryTracker,

    partition_id: i64,
}

impl OpenSearchSink {
    pub(super) fn new(
        config: Arc<OpenSearchSinkConfig>,
        transport: Arc<dyn BulkTransport>,
        counters: Arc<SinkCounters>,
        discovery: Arc<DeliveryDiscovery>,
        partition_id: i64,
    ) -> Self {
        Self {
            config,
            transport,
            counters,
            discovery,
            buffers: HashMap::new(),
            pending_identities: HashSet::new(),
            progress: DeliveryTracker::new(),
            partition_id,
        }
    }

    fn accept(
        &mut self,
        delivery: Delivery,
        pipeline_memory: &transferia_core::memory::PipelineMemory,
    ) -> anyhow::Result<()> {
        for output in &delivery.outputs {
            self.config.validate_batch(&self.discovery, output)?;
        }
        let mut prepared = Vec::new();
        let mut ids = HashSet::<(Arc<str>, Arc<str>)>::new();
        for output in delivery.outputs {
            let dataset = self
                .discovery
                .dataset_named(transferia_core::delivery::DatasetRole::from_is_dlq(output.is_dlq), &output.table)?;
            let batch = match project_sink_batch(&self.discovery, &output)? {
                ProjectedSinkBatch::AppendOnly(batch) => batch,
                ProjectedSinkBatch::Changelog(_) => {
                    anyhow::bail!("OpenSearch sink accepts append-only records only")
                }
            };
            let memory = Arc::new(output.memory);
            for action in encode_batch(
                &output.table,
                &dataset.stored_schema,
                &batch,
                self.config.routed_identity,
            )? {
                let key = (Arc::clone(&output.table), Arc::clone(&action.id));
                anyhow::ensure!(
                    ids.insert(key),
                    "OpenSearch delivery {} repeats document _id '{}' in index '{}'; refusing ambiguous last-write-wins",
                    delivery.id.get(),
                    action.id,
                    output.table
                );
                anyhow::ensure!(
                    !self
                        .pending_identities
                        .contains(&(Arc::clone(&output.table), Arc::clone(&action.id))),
                    "OpenSearch delivery {} repeats buffered document identity '{}' in index '{}'; source primary keys must be globally unique",
                    delivery.id.get(),
                    action.id,
                    output.table
                );
                prepared.push((Arc::clone(&output.table), action, Arc::clone(&memory)));
            }
        }
        let encoded_bytes = prepared.iter().try_fold(0_usize, |total, (_, action, _)| {
            total
                .checked_add(action.bytes)
                .ok_or_else(|| anyhow::anyhow!("OpenSearch encoded delivery size overflow"))
        })?;
        self.progress
            .accept(delivery.id, prepared.len(), delivery.meta.source_messages)?;
        if prepared.is_empty() {
            return Ok(());
        }
        let encoded_memory = Arc::new(pipeline_memory.reserve_transform(encoded_bytes));
        self.pending_identities.extend(ids);
        for (index, action, memory) in prepared {
            let buffer = self.buffers.entry(index).or_insert_with(|| IndexBuffer {
                first_seen: Instant::now(),
                rows: 0,
                bytes: 0,
                actions: VecDeque::new(),
            });
            buffer.rows = buffer.rows.saturating_add(1);
            buffer.bytes = buffer.bytes.saturating_add(action.bytes);
            buffer.actions.push_back(PendingAction {
                delivery_id: delivery.id,
                action,
                _input_memory: memory,
                _encoded_memory: Arc::clone(&encoded_memory),
            });
        }
        Ok(())
    }

    fn next_flush(&self, input_closed: bool, memory_pressure: bool) -> Option<(Arc<str>, Instant)> {
        self.buffers
            .iter()
            .map(|(index, buffer)| {
                let full = memory_pressure
                    || buffer.rows >= self.config.bulk_target_rows
                    || buffer.bytes >= self.config.bulk_target_bytes;
                let deadline = if input_closed || full {
                    Instant::now()
                } else {
                    buffer.first_seen + self.config.flush_interval()
                };
                (Arc::clone(index), deadline)
            })
            .min_by_key(|(_, deadline)| *deadline)
    }

    fn take_flush(&mut self, index: &str) -> anyhow::Result<Vec<PendingAction>> {
        let buffer = self
            .buffers
            .get_mut(index)
            .ok_or_else(|| anyhow::anyhow!("missing OpenSearch buffer for index '{index}'"))?;
        let max_rows = self
            .config
            .bulk_target_rows
            .saturating_mul(self.config.bulk_concurrency);
        let max_bytes = self
            .config
            .bulk_target_bytes
            .saturating_mul(self.config.bulk_concurrency);
        let mut rows = 0_usize;
        let mut bytes = 0_usize;
        let mut actions = Vec::new();
        while let Some(front) = buffer.actions.front() {
            if !actions.is_empty()
                && (rows >= max_rows || bytes.saturating_add(front.action.bytes) > max_bytes)
            {
                break;
            }
            let action = buffer
                .actions
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("OpenSearch buffer changed while flushing"))?;
            rows += 1;
            bytes = bytes.saturating_add(action.action.bytes);
            actions.push(action);
        }
        buffer.rows = buffer.rows.saturating_sub(rows);
        buffer.bytes = buffer.bytes.saturating_sub(bytes);
        if buffer.actions.is_empty() {
            self.buffers.remove(index);
        } else {
            buffer.first_seen = Instant::now();
        }
        Ok(actions)
    }

    async fn flush(
        &mut self,
        index: &str,
        memory: &transferia_core::memory::PipelineMemory,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> DataPlaneResult<()> {
        let pending = self
            .take_flush(index)
            .map_err(DataPlaneFailure::fatal_or_passthrough)?;
        let completed = write_parallel(
            Arc::clone(&self.transport),
            Arc::clone(&self.config),
            Arc::clone(&self.counters),
            memory.clone(),
            cancellation.clone(),
            self.partition_id,
            index,
            pending,
        )
        .await?;
        let mut per_delivery = BTreeMap::<DeliveryId, usize>::new();
        let index_identity: Arc<str> = Arc::from(index);
        for action in completed {
            self.pending_identities.remove(&(
                Arc::clone(&index_identity),
                Arc::clone(&action.action.id),
            ));
            *per_delivery.entry(action.delivery_id).or_default() += 1;
        }
        for (delivery_id, count) in per_delivery {
            self.progress
                .complete(delivery_id, count)
                .map_err(DataPlaneFailure::fatal)?;
        }
        Ok(())
    }

    async fn emit_committed(
        &mut self,
        events: &tokio::sync::mpsc::Sender<SinkEvent>,
    ) -> DataPlaneResult<()> {
        if let Some(committed) = self.progress.take_committed() {
            self.counters.add_source_messages(committed.source_messages);
            events
                .send(SinkEvent::CommittedThrough(committed.through))
                .await
                .map_err(|_| {
                    DataPlaneFailure::fatal(anyhow::anyhow!(
                        "OpenSearch sink event receiver closed"
                    ))
                })?;
        }
        Ok(())
    }

    async fn run_actor(mut self, mut io: SinkIo) -> DataPlaneResult<()> {
        let mut input_closed = false;
        loop {
            self.emit_committed(&io.events).await?;
            let memory_pressure = io.memory.is_transform_pressured();
            if let Some((index, deadline)) = self.next_flush(input_closed, memory_pressure) {
                if deadline <= Instant::now() {
                    self.flush(&index, &io.memory, &io.cancellation).await?;
                    continue;
                }
            }
            if input_closed && self.buffers.is_empty() {
                self.emit_committed(&io.events).await?;
                if !self.progress.is_empty() {
                    return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                        "OpenSearch sink input closed with incomplete deliveries"
                    )));
                }
                return Ok(());
            }
            let deadline = self
                .next_flush(input_closed, memory_pressure)
                .map(|(_, deadline)| deadline);
            tokio::select! {
                biased;
                () = io.cancellation.cancelled() => return Ok(()),
                delivery = io.deliveries.recv(), if !input_closed => {
                    match delivery {
                        Some(delivery) => self
                            .accept(delivery, &io.memory)
                            .map_err(DataPlaneFailure::fatal_or_passthrough)?,
                        None => input_closed = true,
                    }
                }
                () = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {}
            }
        }
    }
}

async fn write_parallel(
    transport: Arc<dyn BulkTransport>,
    config: Arc<OpenSearchSinkConfig>,
    counters: Arc<SinkCounters>,
    memory: transferia_core::memory::PipelineMemory,
    cancellation: tokio_util::sync::CancellationToken,
    partition_id: i64,
    index: &str,
    pending: Vec<PendingAction>,
) -> DataPlaneResult<Vec<PendingAction>> {
    let mut by_id = BTreeMap::<Arc<str>, Vec<PendingAction>>::new();
    for action in pending {
        by_id.entry(Arc::clone(&action.action.id)).or_default().push(action);
    }
    let lane_count = config.bulk_concurrency.min(by_id.len().max(1));
    let mut lanes = (0..lane_count)
        .map(|_| (0_usize, VecDeque::new()))
        .collect::<Vec<_>>();
    for group in by_id.into_values() {
        let lane = lanes
            .iter_mut()
            .min_by_key(|(bytes, _)| *bytes)
            .ok_or_else(|| DataPlaneFailure::fatal(anyhow::anyhow!("no OpenSearch bulk lane")))?;
        lane.0 = lane
            .0
            .saturating_add(group.iter().map(|action| action.action.bytes).sum());
        lane.1.push_back(VecDeque::from(group));
    }
    let index: Arc<str> = Arc::from(index);
    let mut tasks = FuturesUnordered::new();
    for (lane, (_, mut groups)) in lanes.into_iter().enumerate() {
        let transport = Arc::clone(&transport);
        let config = Arc::clone(&config);
        let counters = Arc::clone(&counters);
        let memory = memory.clone();
        let cancellation = cancellation.clone();
        let index = Arc::clone(&index);
        tasks.push(async move {
            let mut completed = Vec::new();
            let mut ordinal = 0_usize;
            while !groups.is_empty() {
                let mut chunk = Vec::new();
                let mut bytes = 0_usize;
                let available_ids = groups.len();
                for _ in 0..available_ids {
                    let mut group = groups.pop_front().ok_or_else(|| {
                        DataPlaneFailure::fatal(anyhow::anyhow!(
                            "OpenSearch lane group disappeared"
                        ))
                    })?;
                    let front = group.front().ok_or_else(|| {
                        DataPlaneFailure::fatal(anyhow::anyhow!(
                            "OpenSearch lane contains an empty ID group"
                        ))
                    })?;
                    if !chunk.is_empty()
                        && (chunk.len() >= config.bulk_target_rows
                            || bytes.saturating_add(front.action.bytes)
                                > config.bulk_target_bytes)
                    {
                        groups.push_front(group);
                        break;
                    }
                    let action = group.pop_front().ok_or_else(|| {
                        DataPlaneFailure::fatal(anyhow::anyhow!(
                            "OpenSearch lane group changed"
                        ))
                    })?;
                    bytes = bytes.saturating_add(action.action.bytes);
                    chunk.push(action);
                    if !group.is_empty() {
                        groups.push_back(group);
                    }
                }
                debug_assert!(!chunk.is_empty());
                let wire = chunk
                    .iter()
                    .map(|pending| pending.action.clone())
                    .collect();
                let (rows, bytes) = write_bulk_with_retry(
                    Arc::clone(&transport),
                    &config,
                    &counters,
                    &memory,
                    &cancellation,
                    wire,
                    retry_seed(partition_id, &index, lane ^ ordinal),
                )
                .await?;
                counters.add_rows(rows as u64);
                counters.add_bytes(bytes as u64);
                counters.add_flush();
                completed.extend(chunk);
                ordinal = ordinal.saturating_add(1);
            }
            Ok::<_, DataPlaneFailure>(completed)
        });
    }
    let mut completed = Vec::new();
    while let Some(result) = tasks.next().await {
        completed.extend(result?);
    }
    Ok(completed)
}

impl Sink for OpenSearchSink {
    fn run(self: Box<Self>, io: SinkIo) -> BoxFuture<'static, DataPlaneResult<()>> {
        Box::pin(self.run_actor(io))
    }
}
