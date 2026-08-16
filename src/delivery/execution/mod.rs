pub(crate) mod delivery_tracker;
pub mod middleware;
pub mod retry;
pub mod runner;

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context as _;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::core::data::message::{Message, SourceBatch};
use crate::core::data::table_data::TableData;
use crate::core::failure::{DataPlaneFailure, DataPlaneResult};
use crate::core::memory::{MemoryReservation, PipelineMemory};
use crate::core::sink::{Delivery, DeliveryId, DeliveryMeta, Sink, SinkBatch, SinkEvent, SinkIo};
use crate::core::source::{CommitMarker, Source};
use crate::delivery::execution::middleware::Middleware;
use crate::metrics::ParseCounters;
use crate::parsers::{ParserFactory, ParserSession};

const CHANNEL_CAPACITY: usize = 8;
const INITIAL_BACKOFF_MS: u64 = 10;
const MAX_BACKOFF_MS: u64 = 30_000;
const SINK_SHUTDOWN_GRACE: Duration = Duration::from_secs(6);

/// Monotonic source-commit progress shared across retries of one partition.
#[derive(Default)]
pub struct PipelineProgress {
    committed_groups: AtomicU64,
}

impl PipelineProgress {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            committed_groups: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn checkpoint(&self) -> u64 {
        self.committed_groups.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn advanced_since(&self, checkpoint: u64) -> bool {
        self.checkpoint() > checkpoint
    }

    fn record_source_commit(&self) {
        self.committed_groups.fetch_add(1, Ordering::AcqRel);
    }
}

struct ReadEnvelope {
    id: DeliveryId,
    payload: ReadPayload,
    memory: Vec<MemoryReservation>,
    meta: DeliveryMeta,
}

enum ReadPayload {
    Raw(Vec<Message>),
    Typed(Vec<TableData>),
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

const fn delivery_meta(source_messages: u64) -> DeliveryMeta {
    DeliveryMeta { source_messages }
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

#[expect(
    clippy::unreachable,
    reason = "typed and raw variants are separated before parser dispatch"
)]
async fn parser_loop(
    mut input: mpsc::Receiver<ReadEnvelope>,
    output: mpsc::Sender<Delivery>,
    parser_factory: Arc<dyn ParserFactory>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    memory: PipelineMemory,
    counters: Arc<ParseCounters>,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    let mut parser: Option<Box<dyn ParserSession>> = None;
    let mut downstream_pressured = false;
    let memory_limit_bytes = memory.limit();
    while !cancellation.is_cancelled() {
        if downstream_pressured {
            let capacity_available = tokio::select! {
                () = memory.wait_transform_below_limit() => true,
                () = cancellation.cancelled() => false,
            };
            if !capacity_available {
                return Ok(());
            }
        }
        let envelope = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            envelope = input.recv() => envelope,
        };
        let Some(envelope) = envelope else {
            return Ok(());
        };

        if let ReadPayload::Typed(tables) = envelope.payload {
            let mut outputs = Vec::with_capacity(tables.len());
            let mut output_bytes = 0_usize;
            let mut output_rows = 0_u64;
            for table in tables {
                let table = apply_middlewares(table, middlewares.as_slice())?;
                let bytes = arrow_batch_bytes(&table.batch);
                output_bytes = output_bytes.saturating_add(bytes);
                output_rows = output_rows.saturating_add(table.batch.num_rows() as u64);
                outputs.push((table, bytes));
            }
            let output_memory = if outputs.is_empty() {
                None
            } else {
                let reservation = tokio::select! {
                    reservation = memory.admit_active_transform(output_bytes) => Some(reservation),
                    () = cancellation.cancelled() => None,
                };
                let Some(reservation) = reservation else {
                    return Ok(());
                };
                drop(envelope.memory);
                Some(reservation.finish(output_bytes))
            };
            counters.add_rows(output_rows);
            counters.add_arrow_bytes(output_bytes as u64);
            counters.add_source_messages(envelope.meta.source_messages);
            let outputs = outputs
                .into_iter()
                .filter_map(|(table, bytes)| {
                    output_memory
                        .as_ref()
                        .and_then(|memory| make_sink_batch(table, bytes, memory.clone()))
                })
                .collect();
            if output
                .send(Delivery {
                    id: envelope.id,
                    outputs,
                    meta: envelope.meta,
                })
                .await
                .is_err()
            {
                return Ok(());
            }
            downstream_pressured = memory.is_transform_pressured();
            continue;
        }
        let ReadPayload::Raw(messages) = envelope.payload else {
            unreachable!();
        };
        let envelope = ReadEnvelope {
            id: envelope.id,
            payload: ReadPayload::Raw(messages),
            memory: envelope.memory,
            meta: envelope.meta,
        };

        // Keep each partition's mutable parser session logically affine while
        // lending it to Tokio's bounded blocking pool only for CPU work. Idle
        // partitions retain no OS thread (and do not even construct a session
        // until their first delivery).
        let parser_factory = Arc::clone(&parser_factory);
        let estimate_task = tokio::task::spawn_blocking(move || {
            let parser =
                parser.unwrap_or_else(|| parser_factory.create_session(memory_limit_bytes));
            let ReadPayload::Raw(messages) = &envelope.payload else {
                unreachable!();
            };
            let output_bound = parser.output_memory_bound(messages);
            (parser, envelope, output_bound)
        });
        let (active_parser, envelope, output_bound) =
            parser_worker_result("estimate", estimate_task.await)?;
        let ReadEnvelope {
            id,
            payload,
            memory: source_memory,
            meta,
        } = envelope;
        let ReadPayload::Raw(messages) = payload else {
            unreachable!();
        };
        let admission_bound = output_bound.min(memory.limit());
        let parse_memory = if messages.is_empty() {
            None
        } else {
            tokio::select! {
                reservation = memory.admit_active_transform(admission_bound) => Some(reservation),
                () = cancellation.cancelled() => None,
            }
        };
        if !messages.is_empty() && parse_memory.is_none() {
            return Ok(());
        }
        let middlewares = Arc::clone(&middlewares);
        let parse_task = tokio::task::spawn_blocking(move || {
            let mut parser = active_parser;
            let started = std::time::Instant::now();
            let parsed = parser
                .parse_into(messages)
                .map_err(|error| {
                    anyhow::anyhow!("parser failed for delivery {}: {error}", id.get())
                })
                .and_then(|(valid, dlq)| {
                    // Parsing materializes owned Arrow arrays, so the source
                    // buffers can be released before middleware allocates any
                    // additional output.
                    drop(source_memory);
                    apply_middlewares(valid, middlewares.as_slice())
                        .map(|valid| (valid, dlq, started.elapsed()))
                        .map_err(|error| {
                            anyhow::anyhow!("middleware failed for delivery {}: {error}", id.get())
                        })
                });
            // Keep admission live with detached blocking work if pipeline
            // shutdown times out and aborts only the async supervisor task.
            (parser, parse_memory, parsed)
        });
        let (active_parser, parse_memory, parsed) =
            parser_worker_result("parse", parse_task.await)?;
        parser = Some(active_parser);
        let (valid, dlq, parse_busy) = parsed?;
        counters.add_parse_busy(parse_busy);
        counters.add_rows(valid.batch.num_rows() as u64);
        let valid_bytes = arrow_batch_bytes(&valid.batch);
        let dlq_bytes = dlq
            .as_ref()
            .map_or(0, |batch| arrow_batch_bytes(&batch.batch));
        counters.add_arrow_bytes(valid_bytes as u64);
        counters.add_source_messages(meta.source_messages);
        if let Some(dlq) = &dlq {
            counters.add_dlq_rows(dlq.batch.num_rows() as u64);
        }

        let output_bytes = valid_bytes.saturating_add(dlq_bytes);
        let has_output = valid.batch.num_rows() > 0
            || dlq.as_ref().is_some_and(|batch| batch.batch.num_rows() > 0);
        anyhow::ensure!(
            !has_output || output_bytes <= memory.limit(),
            "parser delivery {} materialized {output_bytes} bytes, exceeding the configured pipeline memory limit of {} bytes; raise pipeline_memory_limit_bytes or reduce the source read size",
            id.get(),
            memory.limit(),
        );
        if has_output && output_bytes > output_bound {
            tracing::warn!(
                delivery_id = id.get(),
                actual_bytes = output_bytes,
                estimated_bytes = output_bound,
                "parser output exceeded its admission estimate; accounting exact output",
            );
        }
        let output_memory = match (has_output, parse_memory) {
            (true, Some(reservation)) => Some(reservation.finish(output_bytes)),
            (false, Some(reservation)) => {
                drop(reservation);
                None
            }
            (false, None) => None,
            (true, None) => Some(memory.reserve_transform(output_bytes)),
        };
        let mut outputs = Vec::with_capacity(2);
        if let Some(batch) = output_memory
            .as_ref()
            .and_then(|memory| make_sink_batch(valid, valid_bytes, memory.clone()))
        {
            outputs.push(batch);
        }
        if let Some(dlq) = dlq {
            if let Some(batch) = output_memory
                .as_ref()
                .and_then(|memory| make_sink_batch(dlq, dlq_bytes, memory.clone()))
            {
                outputs.push(batch);
            }
        }
        drop(output_memory);
        if output.send(Delivery { id, outputs, meta }).await.is_err() {
            return Ok(());
        }
        downstream_pressured = memory.is_transform_pressured();
    }
    Ok(())
}

#[derive(Debug)]
struct ParserInfrastructureFailure(anyhow::Error);

impl fmt::Display for ParserInfrastructureFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ParserInfrastructureFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

fn parser_worker_result<T>(
    operation: &'static str,
    result: Result<T, tokio::task::JoinError>,
) -> anyhow::Result<T> {
    result.map_err(|error| {
        ParserInfrastructureFailure(anyhow::anyhow!(
            "parser {operation} blocking task failed: {error}"
        ))
        .into()
    })
}

async fn commit_through(
    source: &mut Box<dyn Source>,
    ledger: &mut VecDeque<CommitEntry>,
    committed: DeliveryId,
    progress: &PipelineProgress,
) -> anyhow::Result<()> {
    let valid_range = ledger
        .front()
        .zip(ledger.back())
        .is_some_and(|(first, last)| first.id <= committed && committed <= last.id);
    if !valid_range {
        return Err(DataPlaneFailure::fatal(anyhow::anyhow!(
            "sink committed unknown delivery {}; outstanding range is {:?}..={:?}",
            committed.get(),
            ledger.front().map(|entry| entry.id.get()),
            ledger.back().map(|entry| entry.id.get())
        ))
        .into());
    }
    let committed_entries = ledger
        .iter()
        .take_while(|entry| entry.id <= committed)
        .count();
    let markers = ledger
        .iter()
        .take(committed_entries)
        .filter_map(|entry| entry.marker.clone())
        .collect::<Vec<_>>();
    if !markers.is_empty() {
        source.commit_offsets(&markers).await.with_context(|| {
            format!("source commit failed through delivery {}", committed.get())
        })?;
        progress.record_source_commit();
    }
    for _ in 0..committed_entries {
        ledger
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("commit ledger underflow"))?;
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
    progress: &PipelineProgress,
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
                commit_through(source, ledger, id, progress).await?;
            }
            reservation = &mut reservation => return Ok(Some(reservation)),
        }
    }
}

#[expect(
    clippy::unreachable,
    reason = "Finished is handled immediately before exhaustive payload extraction"
)]
async fn reader_loop(
    mut source: Box<dyn Source>,
    output: mpsc::Sender<ReadEnvelope>,
    mut events: mpsc::Receiver<SinkEvent>,
    memory: PipelineMemory,
    cancellation: CancellationToken,
    progress: Arc<PipelineProgress>,
) -> anyhow::Result<()> {
    let mut ledger = VecDeque::new();
    let mut next_id = DeliveryId::new(1);
    let mut backoff_ms = INITIAL_BACKOFF_MS;
    loop {
        let read = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            event = events.recv() => {
                let event = event.ok_or_else(|| anyhow::anyhow!("sink event stream closed"))?;
                let SinkEvent::CommittedThrough(id) = event;
                commit_through(&mut source, &mut ledger, id, progress.as_ref()).await?;
                continue;
            }
            read = source.read_batch() => read,
        };

        let batch = read?;
        if matches!(batch, SourceBatch::Finished) {
            // Close the parser input before waiting for the durability ledger.
            // Finite sources need the parser and sink to observe EOF so that
            // open sink buffers can be flushed and committed.
            drop(output);
            while !ledger.is_empty() {
                let event = events.recv().await.ok_or_else(|| {
                    anyhow::anyhow!("sink event stream closed before source completion")
                })?;
                let SinkEvent::CommittedThrough(id) = event;
                commit_through(&mut source, &mut ledger, id, progress.as_ref()).await?;
            }
            return Ok(());
        }
        let (payload, mut batch_memory, mut marker, source_payload_bytes, source_messages) =
            match batch {
                SourceBatch::Raw {
                    messages,
                    commit_marker,
                    memory,
                } => {
                    let bytes = messages
                        .iter()
                        .map(|message| message.value.len() as u64)
                        .sum::<u64>();
                    let source_messages = messages.len() as u64;
                    (
                        ReadPayload::Raw(messages),
                        memory,
                        commit_marker,
                        bytes,
                        source_messages,
                    )
                }
                SourceBatch::Typed {
                    tables,
                    source_rows,
                    commit_marker,
                    memory,
                } => {
                    let bytes = tables
                        .iter()
                        .map(|table| arrow_batch_bytes(&table.batch) as u64)
                        .sum();
                    (
                        ReadPayload::Typed(tables),
                        memory,
                        commit_marker,
                        bytes,
                        source_rows,
                    )
                }
                SourceBatch::Finished => unreachable!(),
            };
        let payload_is_empty = match &payload {
            ReadPayload::Raw(messages) => messages.is_empty(),
            ReadPayload::Typed(tables) => tables.iter().all(|table| table.batch.num_rows() == 0),
        };
        if payload_is_empty && marker.is_none() {
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                event = events.recv() => {
                    if let Some(SinkEvent::CommittedThrough(id)) = event {
                        commit_through(&mut source, &mut ledger, id, progress.as_ref()).await?;
                    }
                }
                () = sleep(Duration::from_millis(backoff_ms)) => {}
            }
            backoff_ms = backoff_ms.saturating_mul(2).min(MAX_BACKOFF_MS);
            continue;
        }
        backoff_ms = INITIAL_BACKOFF_MS;
        let meta = delivery_meta(source_messages);
        if batch_memory.is_empty() && source_payload_bytes > 0 {
            let Some(reservation) = reserve_source_memory_with_events(
                &mut source,
                &mut ledger,
                &mut events,
                &memory,
                source_payload_bytes as usize,
                &cancellation,
                progress.as_ref(),
            )
            .await?
            else {
                return Ok(());
            };
            batch_memory.push(reservation);
        }
        ledger.push_back(CommitEntry {
            id: next_id,
            marker: marker.take(),
        });
        let envelope = ReadEnvelope {
            id: next_id,
            payload,
            memory: batch_memory,
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
                    commit_through(&mut source, &mut ledger, id, progress.as_ref()).await?;
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
    result: DataPlaneResult<()>,
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
            result: Err(DataPlaneFailure::retryable(anyhow::anyhow!(message))),
            infrastructure_failure: true,
        }
    }

    fn fatal_timeout(message: &'static str) -> Self {
        Self {
            result: Err(DataPlaneFailure::fatal(anyhow::anyhow!(message))),
            infrastructure_failure: true,
        }
    }
}

fn data_plane_component_outcome(
    name: &'static str,
    result: Result<DataPlaneResult<()>, tokio::task::JoinError>,
) -> ComponentOutcome {
    match result {
        Ok(outcome) => ComponentOutcome {
            result: outcome.map_err(|error| error.context(format!("{name} failed"))),
            infrastructure_failure: false,
        },
        Err(error) => ComponentOutcome {
            result: Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                "{name} task panicked: {error}"
            ))),
            infrastructure_failure: true,
        },
    }
}

fn anyhow_component_outcome(
    name: &'static str,
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> ComponentOutcome {
    data_plane_component_outcome(
        name,
        result.map(|outcome| {
            outcome.map_err(|error| {
                DataPlaneFailure::retryable_or_passthrough(error).context(format!("{name} failed"))
            })
        }),
    )
}

fn parser_component_outcome(
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> ComponentOutcome {
    match result {
        Ok(Ok(())) => ComponentOutcome {
            result: Ok(()),
            infrastructure_failure: false,
        },
        Ok(Err(error))
            if error
                .downcast_ref::<ParserInfrastructureFailure>()
                .is_some() =>
        {
            ComponentOutcome {
                result: Err(DataPlaneFailure::retryable(error)),
                infrastructure_failure: true,
            }
        }
        Ok(Err(error)) => ComponentOutcome {
            result: Err(DataPlaneFailure::fatal(error)),
            infrastructure_failure: false,
        },
        Err(error) => ComponentOutcome {
            result: Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                "parser task panicked: {error}"
            ))),
            infrastructure_failure: true,
        },
    }
}

fn prefer_component_failure(
    outcomes: impl IntoIterator<Item = DataPlaneResult<()>>,
) -> DataPlaneResult<()> {
    let mut first_retryable = None;
    for outcome in outcomes {
        let Err(error) = outcome else {
            continue;
        };
        if !error.is_retryable() {
            return Err(error);
        }
        first_retryable.get_or_insert(error);
    }
    first_retryable.map_or(Ok(()), Err)
}

pub async fn run_partition_pipeline(
    source: Box<dyn Source>,
    parser: Arc<dyn ParserFactory>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    sink: Box<dyn Sink>,
    memory: PipelineMemory,
    cancel_token: CancellationToken,
    partition_id: i64,
    parse_counters: Arc<ParseCounters>,
) -> DataPlaneResult<()> {
    run_partition_pipeline_with_progress(
        source,
        parser,
        middlewares,
        sink,
        memory,
        cancel_token,
        partition_id,
        parse_counters,
        Arc::new(PipelineProgress::new()),
    )
    .await
}

pub async fn run_partition_pipeline_with_progress(
    source: Box<dyn Source>,
    parser: Arc<dyn ParserFactory>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    sink: Box<dyn Sink>,
    memory: PipelineMemory,
    cancel_token: CancellationToken,
    _partition_id: i64,
    parse_counters: Arc<ParseCounters>,
    progress: Arc<PipelineProgress>,
) -> DataPlaneResult<()> {
    let local = cancel_token.child_token();
    let (read_tx, read_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (delivery_tx, delivery_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(CHANNEL_CAPACITY);

    let parser_token = local.clone();
    let parser_memory = memory.clone();
    let mut parser_task = tokio::spawn(parser_loop(
        read_rx,
        delivery_tx,
        parser,
        middlewares,
        parser_memory,
        parse_counters,
        parser_token,
    ));

    let reader_token = local.clone();
    let reader_memory = memory.clone();
    let mut reader_task = tokio::spawn(async move {
        reader_loop(
            source,
            read_tx,
            event_rx,
            reader_memory,
            reader_token,
            progress,
        )
        .await
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
    let mut first_component = None;
    let mut external_cancelled = false;
    while first_component.is_none() && !external_cancelled {
        tokio::select! {
            biased;
            () = cancel_token.cancelled() => external_cancelled = true,
            result = &mut reader_task, if reader_outcome.is_none() => {
                let outcome = anyhow_component_outcome("reader", result);
                first_component = Some(FirstComponent::Reader);
                reader_outcome = Some(outcome);
            }
            result = &mut sink_task, if sink_outcome.is_none() => {
                let outcome = data_plane_component_outcome("sink", result);
                if outcome.result.is_err() {
                    first_component = Some(FirstComponent::Sink);
                }
                sink_outcome = Some(outcome);
            }
            result = &mut parser_task, if parser_outcome.is_none() => {
                let outcome = parser_component_outcome(result);
                if outcome.result.is_err() {
                    first_component = Some(FirstComponent::Parser);
                }
                parser_outcome = Some(outcome);
            }
        }
    }

    local.cancel();
    if reader_outcome.is_none() {
        reader_outcome = Some(
            match tokio::time::timeout(Duration::from_secs(1), &mut reader_task).await {
                Ok(result) => anyhow_component_outcome("reader", result),
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
                Ok(result) => data_plane_component_outcome("sink", result),
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
            match tokio::time::timeout(Duration::from_secs(1), &mut parser_task).await {
                Ok(result) => parser_component_outcome(result),
                Err(_) => {
                    parser_task.abort();
                    drop(parser_task.await);
                    ComponentOutcome::fatal_timeout("parser task did not stop within 1s")
                }
            },
        );
    }

    let missing_outcome = || ComponentOutcome {
        result: Err(DataPlaneFailure::retryable(anyhow::anyhow!(
            "missing component outcome"
        ))),
        infrastructure_failure: true,
    };
    let mut reader_outcome = Some(reader_outcome.unwrap_or_else(missing_outcome));
    let mut sink_outcome = Some(sink_outcome.unwrap_or_else(missing_outcome));
    let mut parser_outcome = Some(parser_outcome.unwrap_or_else(missing_outcome));
    let mut outcomes = Vec::with_capacity(3);
    match first_component {
        Some(FirstComponent::Reader) => {
            outcomes.push(reader_outcome.take().unwrap_or_else(missing_outcome));
        }
        Some(FirstComponent::Sink) => {
            outcomes.push(sink_outcome.take().unwrap_or_else(missing_outcome));
        }
        Some(FirstComponent::Parser) => {
            outcomes.push(parser_outcome.take().unwrap_or_else(missing_outcome));
        }
        None => {}
    }
    outcomes.extend(reader_outcome);
    outcomes.extend(sink_outcome);
    outcomes.extend(parser_outcome);

    if external_cancelled {
        return outcomes
            .into_iter()
            .find(|outcome| outcome.infrastructure_failure)
            .map_or(Ok(()), |outcome| outcome.result);
    }
    prefer_component_failure(outcomes.into_iter().map(|outcome| outcome.result))
}

#[cfg(test)]
mod tests;
