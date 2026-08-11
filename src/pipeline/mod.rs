pub(crate) mod delivery_tracker;
pub mod memory;
pub mod middleware;
pub mod sink;
pub mod source;

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::thread;

use anyhow::Context as _;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::metrics::ParseCounters;
use crate::parsers::{Parser, ParserWorkspace};
use crate::pipeline::memory::{MemoryReservation, PipelineMemory};
use crate::pipeline::middleware::Middleware;
use crate::pipeline::sink::{
    Delivery, DeliveryId, DeliveryMeta, Sink, SinkBatch, SinkEvent, SinkIo,
};
use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::types::message::Message;
use crate::types::table_data::TableData;

const CHANNEL_CAPACITY: usize = 8;
const MAX_OUTSTANDING_DELIVERIES: usize = CHANNEL_CAPACITY * 2;
const INITIAL_BACKOFF_MS: u64 = 10;
const MAX_BACKOFF_MS: u64 = 30_000;
const SINK_SHUTDOWN_GRACE: Duration = Duration::from_secs(6);

/// A pipeline failure with an explicit restart contract.
///
/// Sources use fatal failures for deterministic data/protocol violations (for
/// example corrupt compressed data), while transport and actor failures remain
/// retryable by default.
#[derive(Debug)]
pub struct PipelineFailure {
    retryable: bool,
    error: anyhow::Error,
}

impl PipelineFailure {
    #[must_use]
    pub const fn fatal(error: anyhow::Error) -> Self {
        Self {
            retryable: false,
            error,
        }
    }

    #[must_use]
    pub const fn retryable(error: anyhow::Error) -> Self {
        Self {
            retryable: true,
            error,
        }
    }

    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl fmt::Display for PipelineFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for PipelineFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

struct ReadEnvelope {
    id: DeliveryId,
    messages: Vec<Message>,
    partition_id: i64,
    memory: Vec<MemoryReservation>,
    meta: DeliveryMeta,
}

struct CommitEntry {
    id: DeliveryId,
    marker: Option<CommitMarker>,
}

fn arrow_batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size())
        .sum()
}

fn delivery_meta(messages: &[Message]) -> DeliveryMeta {
    DeliveryMeta {
        source_messages: messages.len() as u64,
        source_bytes: messages
            .iter()
            .map(|message| message.value.len() as u64)
            .sum(),
        first_offset: messages
            .iter()
            .filter_map(|message| message.meta.offset)
            .min(),
        last_offset: messages
            .iter()
            .filter_map(|message| message.meta.offset)
            .max(),
    }
}

fn apply_middlewares(
    mut data: TableData,
    middlewares: &[Box<dyn Middleware>],
) -> anyhow::Result<TableData> {
    if data.is_dlq {
        return Ok(data);
    }
    for middleware in middlewares {
        data = middleware.process(data)?;
    }
    Ok(data)
}

fn make_sink_batch(
    data: TableData,
    byte_size: usize,
    reservation: MemoryReservation,
) -> Option<SinkBatch> {
    if data.batch.num_rows() == 0 {
        return None;
    }
    Some(SinkBatch {
        table: data.table,
        is_dlq: data.is_dlq,
        batch: data.batch,
        byte_size,
        memory: reservation,
        system_columns: data.system_columns,
    })
}

fn parser_loop(
    mut input: mpsc::Receiver<ReadEnvelope>,
    output: &mpsc::Sender<Delivery>,
    parser: &dyn Parser,
    middlewares: &[Box<dyn Middleware>],
    memory: &PipelineMemory,
    counters: &ParseCounters,
    cancellation: &CancellationToken,
    runtime: &tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let mut workspace = ParserWorkspace::new();
    let mut downstream_pressured = false;
    while !cancellation.is_cancelled() {
        if downstream_pressured {
            let capacity_available = runtime.block_on(async {
                tokio::select! {
                    () = memory.wait_transform_below_limit() => true,
                    () = cancellation.cancelled() => false,
                }
            });
            if !capacity_available {
                return Ok(());
            }
        }
        let Some(envelope) = input.blocking_recv() else {
            return Ok(());
        };
        let ReadEnvelope {
            id,
            messages,
            partition_id,
            memory: source_memory,
            meta,
        } = envelope;
        let started = std::time::Instant::now();
        let (valid, dlq) = parser
            .parse_into(messages, partition_id, &mut workspace)
            .map_err(|error| anyhow::anyhow!("parser failed for delivery {}: {error}", id.get()))?;
        let valid = apply_middlewares(valid, middlewares).map_err(|error| {
            anyhow::anyhow!("middleware failed for delivery {}: {error}", id.get())
        })?;
        counters.add_parse_busy(started.elapsed());
        counters.add_rows(valid.batch.num_rows() as u64);
        let valid_bytes = arrow_batch_bytes(&valid.batch);
        let dlq_bytes = dlq
            .as_ref()
            .map_or(0, |batch| arrow_batch_bytes(&batch.batch));
        counters.add_arrow_bytes(valid_bytes as u64);
        counters.add_unique_offsets(meta.source_messages);
        if let Some(dlq) = &dlq {
            counters.add_dlq_rows(dlq.batch.num_rows() as u64);
        }

        // Source buffers are no longer needed after parse. Release them before
        // waiting for Arrow capacity, avoiding a transform-stage deadlock.
        drop(source_memory);
        let output_bytes = valid_bytes.saturating_add(dlq_bytes);
        let output_memory = memory.reserve_transform(output_bytes);
        let mut outputs = Vec::with_capacity(2);
        if let Some(batch) = make_sink_batch(valid, valid_bytes, output_memory.clone()) {
            outputs.push(batch);
        }
        if let Some(dlq) = dlq {
            if let Some(batch) = make_sink_batch(dlq, dlq_bytes, output_memory.clone()) {
                outputs.push(batch);
            }
        }
        drop(output_memory);
        if output
            .blocking_send(Delivery { id, outputs, meta })
            .is_err()
        {
            return Ok(());
        }
        downstream_pressured = memory.transform_used() >= memory.limit();
    }
    Ok(())
}

async fn commit_through(
    source: &mut Box<dyn Source>,
    ledger: &mut VecDeque<CommitEntry>,
    committed: DeliveryId,
) -> anyhow::Result<()> {
    while ledger.front().is_some_and(|entry| entry.id <= committed) {
        let entry = ledger
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("commit ledger underflow"))?;
        if let Some(marker) = entry.marker {
            source
                .commit_offsets(&marker)
                .await
                .with_context(|| format!("PQv1 commit failed at delivery {}", entry.id.get()))?;
        }
    }
    Ok(())
}

async fn reserve_source_memory_with_events(
    source: &mut Box<dyn Source>,
    ledger: &mut VecDeque<CommitEntry>,
    events: &mut mpsc::Receiver<SinkEvent>,
    memory: &PipelineMemory,
    bytes: usize,
    cancellation: &CancellationToken,
) -> anyhow::Result<Option<MemoryReservation>> {
    let reservation = memory.reserve(bytes);
    tokio::pin!(reservation);
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(None),
            event = events.recv() => {
                let event = event.ok_or_else(|| anyhow::anyhow!(
                    "sink event stream closed while reserving source memory"
                ))?;
                let SinkEvent::CommittedThrough(id) = event;
                commit_through(source, ledger, id).await?;
            }
            reservation = &mut reservation => return Ok(Some(reservation)),
        }
    }
}

async fn reader_loop(
    mut source: Box<dyn Source>,
    output: mpsc::Sender<ReadEnvelope>,
    mut events: mpsc::Receiver<SinkEvent>,
    memory: PipelineMemory,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    let mut ledger = VecDeque::new();
    let mut next_id = DeliveryId::new(1);
    let mut backoff_ms = INITIAL_BACKOFF_MS;
    loop {
        if ledger.len() >= MAX_OUTSTANDING_DELIVERIES {
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                event = events.recv() => {
                    let event = event.ok_or_else(|| anyhow::anyhow!(
                        "sink event stream closed while delivery admission was paused"
                    ))?;
                    let SinkEvent::CommittedThrough(id) = event;
                    commit_through(&mut source, &mut ledger, id).await?;
                }
            }
            continue;
        }
        let read = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            event = events.recv() => {
                let event = event.ok_or_else(|| anyhow::anyhow!("sink event stream closed"))?;
                let SinkEvent::CommittedThrough(id) = event;
                commit_through(&mut source, &mut ledger, id).await?;
                continue;
            }
            read = source.read_batch() => read,
        };

        let mut batch = match read? {
            ReadResult::Batch(batch)
                if batch.messages.is_empty() && batch.commit_marker.is_none() =>
            {
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(()),
                    event = events.recv() => {
                        if let Some(SinkEvent::CommittedThrough(id)) = event {
                            commit_through(&mut source, &mut ledger, id).await?;
                        }
                    }
                    () = sleep(Duration::from_millis(backoff_ms)) => {}
                }
                backoff_ms = backoff_ms.saturating_mul(2).min(MAX_BACKOFF_MS);
                continue;
            }
            ReadResult::Batch(batch) => batch,
            ReadResult::Failed(error) => {
                return Err(PipelineFailure::fatal(error).into());
            }
            ReadResult::Exhausted => {
                return Err(anyhow::anyhow!("PQv1 source exhausted unexpectedly"))
            }
            ReadResult::Arrow(_) => {
                return Err(anyhow::anyhow!(
                    "Arrow source is disabled in the PQv1 pipeline"
                ))
            }
        };
        backoff_ms = INITIAL_BACKOFF_MS;
        let meta = delivery_meta(&batch.messages);
        if batch.memory.is_empty() && meta.source_bytes > 0 {
            let Some(reservation) = reserve_source_memory_with_events(
                &mut source,
                &mut ledger,
                &mut events,
                &memory,
                meta.source_bytes as usize,
                &cancellation,
            )
            .await?
            else {
                return Ok(());
            };
            batch.memory.push(reservation);
        }
        let marker = batch.commit_marker.take();
        ledger.push_back(CommitEntry {
            id: next_id,
            marker,
        });
        let envelope = ReadEnvelope {
            id: next_id,
            messages: batch.messages,
            partition_id: batch.partition_id,
            memory: batch.memory,
            meta,
        };
        next_id = next_id.next();

        let mut pending = Some(envelope);
        while pending.is_some() {
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                event = events.recv() => {
                    let event = event.ok_or_else(|| anyhow::anyhow!("sink event stream closed"))?;
                    let SinkEvent::CommittedThrough(id) = event;
                    commit_through(&mut source, &mut ledger, id).await?;
                }
                permit = output.reserve() => {
                    let permit = permit.map_err(|_| anyhow::anyhow!("parser input closed"))?;
                    permit.send(pending.take().ok_or_else(|| anyhow::anyhow!("missing pending read"))?);
                }
            }
        }
    }
}

struct ComponentOutcome {
    result: anyhow::Result<()>,
    infrastructure_failure: bool,
}

#[derive(Clone, Copy)]
enum FirstComponent {
    Reader,
    Sink,
    Parser,
}

impl ComponentOutcome {
    fn timeout(message: &'static str) -> Self {
        Self {
            result: Err(anyhow::anyhow!(message)),
            infrastructure_failure: true,
        }
    }

    fn fatal_timeout(message: &'static str) -> Self {
        Self {
            result: Err(PipelineFailure::fatal(anyhow::anyhow!(message)).into()),
            infrastructure_failure: true,
        }
    }
}

fn async_component_outcome(
    name: &'static str,
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> ComponentOutcome {
    match result {
        Ok(outcome) => ComponentOutcome {
            result: outcome.with_context(|| format!("{name} failed")),
            infrastructure_failure: false,
        },
        Err(error) => ComponentOutcome {
            result: Err(anyhow::anyhow!("{name} task panicked: {error}")),
            infrastructure_failure: true,
        },
    }
}

fn parser_component_outcome(
    result: Result<anyhow::Result<()>, tokio::sync::oneshot::error::RecvError>,
) -> ComponentOutcome {
    match result {
        Ok(Ok(())) => ComponentOutcome {
            result: Ok(()),
            infrastructure_failure: false,
        },
        Ok(Err(error)) => ComponentOutcome {
            result: Err(PipelineFailure::fatal(error).into()),
            infrastructure_failure: false,
        },
        Err(error) => ComponentOutcome {
            result: Err(anyhow::anyhow!("parser completion channel closed: {error}")),
            infrastructure_failure: true,
        },
    }
}

fn prefer_component_failure(
    outcomes: impl IntoIterator<Item = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let mut first_error = None;
    for outcome in outcomes {
        let Err(error) = outcome else {
            continue;
        };
        if error
            .downcast_ref::<PipelineFailure>()
            .is_some_and(|failure| !failure.is_retryable())
        {
            return Err(error);
        }
        first_error.get_or_insert(error);
    }
    first_error.map_or(Ok(()), Err)
}

pub async fn run_partition_pipeline(
    source: Box<dyn Source>,
    parser: Arc<dyn Parser>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    sink: Box<dyn Sink>,
    memory: PipelineMemory,
    cancel_token: CancellationToken,
    partition_id: i64,
    parse_counters: Arc<ParseCounters>,
) -> anyhow::Result<()> {
    let local = cancel_token.child_token();
    let (read_tx, read_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (delivery_tx, delivery_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (parser_done_tx, mut parser_done_rx) = tokio::sync::oneshot::channel();

    let parser_token = local.clone();
    let parser_memory = memory.clone();
    let parser_runtime = tokio::runtime::Handle::current();
    let parser_thread = thread::Builder::new()
        .name(format!("parser-{partition_id}"))
        .spawn(move || {
            let result = parser_loop(
                read_rx,
                &delivery_tx,
                parser.as_ref(),
                middlewares.as_slice(),
                &parser_memory,
                parse_counters.as_ref(),
                &parser_token,
                &parser_runtime,
            );
            drop(parser_done_tx.send(result));
        })?;

    let reader_token = local.clone();
    let reader_memory = memory.clone();
    let mut reader_task = tokio::spawn(async move {
        reader_loop(source, read_tx, event_rx, reader_memory, reader_token).await
    });
    let sink_token = local.clone();
    let sink_io = SinkIo {
        deliveries: delivery_rx,
        events: event_tx,
        memory,
        cancellation: sink_token,
    };
    let mut sink_task = tokio::spawn(sink.run(sink_io));

    let mut reader_outcome = None;
    let mut sink_outcome = None;
    let mut parser_outcome = None;
    let mut parser_monitor_finished = false;
    let mut first_component = None;
    let mut external_cancelled = false;
    tokio::select! {
        biased;
        () = cancel_token.cancelled() => external_cancelled = true,
        result = &mut reader_task => {
            reader_outcome = Some(async_component_outcome("reader", result));
            first_component = Some(FirstComponent::Reader);
        }
        result = &mut sink_task => {
            sink_outcome = Some(async_component_outcome("sink", result));
            first_component = Some(FirstComponent::Sink);
        }
        result = &mut parser_done_rx => {
            parser_monitor_finished = true;
            parser_outcome = Some(parser_component_outcome(result));
            first_component = Some(FirstComponent::Parser);
        }
    }

    local.cancel();
    if reader_outcome.is_none() {
        reader_outcome = Some(
            match tokio::time::timeout(Duration::from_secs(1), &mut reader_task).await {
                Ok(result) => async_component_outcome("reader", result),
                Err(_) => {
                    reader_task.abort();
                    drop(reader_task.await);
                    ComponentOutcome::timeout("reader task did not stop within 1s")
                }
            },
        );
    }
    // Give the sink a chance to clean up owned I/O (including multipart abort)
    // and release reservations before resorting to task abortion.
    if sink_outcome.is_none() {
        sink_outcome = Some(
            match tokio::time::timeout(SINK_SHUTDOWN_GRACE, &mut sink_task).await {
                Ok(result) => async_component_outcome("sink", result),
                Err(_) => {
                    sink_task.abort();
                    drop(sink_task.await);
                    ComponentOutcome::timeout("sink task did not stop within 6s")
                }
            },
        );
    }
    if parser_outcome.is_none() {
        parser_outcome = Some(
            tokio::time::timeout(Duration::from_secs(1), &mut parser_done_rx)
                .await
                .map_or_else(
                    |_| ComponentOutcome::fatal_timeout("parser thread did not stop within 1s"),
                    |result| {
                        parser_monitor_finished = true;
                        parser_component_outcome(result)
                    },
                ),
        );
    }
    let parser_thread_outcome = if parser_monitor_finished {
        match parser_thread.join() {
            Ok(()) => ComponentOutcome {
                result: Ok(()),
                infrastructure_failure: false,
            },
            Err(_) => ComponentOutcome {
                result: Err(anyhow::anyhow!("parser thread panicked")),
                infrastructure_failure: true,
            },
        }
    } else {
        drop(parser_thread);
        ComponentOutcome {
            result: Ok(()),
            infrastructure_failure: false,
        }
    };

    let missing_outcome = || ComponentOutcome {
        result: Err(anyhow::anyhow!("missing component outcome")),
        infrastructure_failure: true,
    };
    let mut reader_outcome = Some(reader_outcome.unwrap_or_else(missing_outcome));
    let mut sink_outcome = Some(sink_outcome.unwrap_or_else(missing_outcome));
    let mut parser_outcome = Some(parser_outcome.unwrap_or_else(missing_outcome));
    let mut outcomes = Vec::with_capacity(4);
    match first_component {
        Some(FirstComponent::Reader) => {
            outcomes.push(reader_outcome.take().expect("reader outcome"));
        }
        Some(FirstComponent::Sink) => outcomes.push(sink_outcome.take().expect("sink outcome")),
        Some(FirstComponent::Parser) => {
            outcomes.push(parser_outcome.take().expect("parser outcome"));
        }
        None => {}
    }
    outcomes.extend(reader_outcome);
    outcomes.extend(sink_outcome);
    outcomes.extend(parser_outcome);
    outcomes.push(parser_thread_outcome);

    if external_cancelled {
        return outcomes
            .into_iter()
            .find(|outcome| outcome.infrastructure_failure)
            .map_or(Ok(()), |outcome| outcome.result);
    }
    prefer_component_failure(outcomes.into_iter().map(|outcome| outcome.result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_shutdown_timeout_cannot_restart_over_a_live_parser_thread() {
        let error = ComponentOutcome::fatal_timeout("parser timeout")
            .result
            .expect_err("timeout must fail the pipeline");
        let failure = error
            .downcast_ref::<PipelineFailure>()
            .expect("parser timeout must preserve its restart contract");
        assert!(!failure.is_retryable());
    }
}
