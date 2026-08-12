pub(crate) mod delivery_tracker;
pub mod memory;
pub mod middleware;
pub mod retry;
pub mod sink;
pub mod source;

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context as _;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::metrics::ParseCounters;
use crate::parsers::{ParserFactory, ParserSession};
use crate::pipeline::memory::{MemoryReservation, PipelineMemory};
use crate::pipeline::middleware::Middleware;
use crate::pipeline::sink::{
    Delivery, DeliveryId, DeliveryMeta, Sink, SinkBatch, SinkEvent, SinkIo,
};
use crate::pipeline::source::{CommitMarker, Source};
use crate::types::message::Message;
use crate::types::table_data::TableData;

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

const fn delivery_meta(messages: &[Message]) -> DeliveryMeta {
    DeliveryMeta {
        source_messages: messages.len() as u64,
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

        // Keep each partition's mutable parser session logically affine while
        // lending it to Tokio's bounded blocking pool only for CPU work. Idle
        // partitions retain no OS thread (and do not even construct a session
        // until their first delivery).
        let parser_factory = Arc::clone(&parser_factory);
        let estimate_task = tokio::task::spawn_blocking(move || {
            let parser = parser.unwrap_or_else(|| parser_factory.create_session());
            let output_bound = parser.output_memory_bound(&envelope.messages);
            let hard_output_limit = parser.hard_output_limit();
            (parser, envelope, output_bound, hard_output_limit)
        });
        let (active_parser, envelope, output_bound, hard_output_limit) =
            parser_worker_result("estimate", estimate_task.await)?;
        let ReadEnvelope {
            id,
            messages,
            memory: source_memory,
            meta,
        } = envelope;
        let admission_bound =
            hard_output_limit.map_or(output_bound, |limit| output_bound.min(limit));
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
        if let Some(limit) = hard_output_limit {
            anyhow::ensure!(
                !has_output || output_bytes <= limit,
                "parser delivery {} materialized {output_bytes} bytes, exceeding its hard output limit of {limit} bytes; reduce source read size, mapping width, or record size",
                id.get(),
            );
        }
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
        return Err(PipelineFailure::fatal(anyhow::anyhow!(
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

        let mut batch = read?;
        if batch.messages.is_empty() && batch.commit_marker.is_none() {
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
        let source_payload_bytes = batch
            .messages
            .iter()
            .map(|message| message.value.len() as u64)
            .sum::<u64>();
        let meta = delivery_meta(&batch.messages);
        if batch.memory.is_empty() && source_payload_bytes > 0 {
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
                result: Err(error),
                infrastructure_failure: true,
            }
        }
        Ok(Err(error)) => ComponentOutcome {
            result: Err(PipelineFailure::fatal(error).into()),
            infrastructure_failure: false,
        },
        Err(error) => ComponentOutcome {
            result: Err(anyhow::anyhow!("parser task panicked: {error}")),
            infrastructure_failure: true,
        },
    }
}

fn prefer_component_failure(
    outcomes: impl IntoIterator<Item = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let mut first_error = None;
    let mut first_pipeline_failure = None;
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
        if error.downcast_ref::<PipelineFailure>().is_some() {
            first_pipeline_failure.get_or_insert(error);
        } else {
            first_error.get_or_insert(error);
        }
    }
    first_pipeline_failure.or(first_error).map_or(Ok(()), Err)
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
) -> anyhow::Result<()> {
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
) -> anyhow::Result<()> {
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
        result = &mut parser_task => {
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
        result: Err(anyhow::anyhow!("missing component outcome")),
        infrastructure_failure: true,
    };
    let mut reader_outcome = Some(reader_outcome.unwrap_or_else(missing_outcome));
    let mut sink_outcome = Some(sink_outcome.unwrap_or_else(missing_outcome));
    let mut parser_outcome = Some(parser_outcome.unwrap_or_else(missing_outcome));
    let mut outcomes = Vec::with_capacity(3);
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
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};

    struct RecordingSource {
        groups: Arc<std::sync::Mutex<Vec<Vec<i64>>>>,
        fail_commit: bool,
    }

    struct OverestimatedSession;

    struct OverestimatedFactory;

    impl ParserFactory for OverestimatedFactory {
        fn create_session(self: Arc<Self>) -> Box<dyn ParserSession> {
            Box::new(OverestimatedSession)
        }
    }

    struct StatefulFactory {
        created: Arc<AtomicU64>,
    }

    impl ParserFactory for StatefulFactory {
        fn create_session(self: Arc<Self>) -> Box<dyn ParserSession> {
            self.created.fetch_add(1, Ordering::Relaxed);
            Box::new(StatefulSession { calls: 0 })
        }
    }

    struct StatefulSession {
        calls: i64,
    }

    impl ParserSession for StatefulSession {
        fn output_memory_bound(&self, _messages: &[Message]) -> usize {
            1024
        }

        fn hard_output_limit(&self) -> Option<usize> {
            Some(1024)
        }

        fn parse_into(
            &mut self,
            _messages: Vec<Message>,
        ) -> anyhow::Result<(TableData, Option<TableData>)> {
            self.calls += 1;
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    arrow::datatypes::DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![self.calls]))],
            )?;
            Ok((
                TableData::new(
                    "events".into(),
                    false,
                    batch,
                    crate::types::system_columns::SystemColumns::default(),
                ),
                None,
            ))
        }
    }

    impl ParserSession for OverestimatedSession {
        fn output_memory_bound(&self, _messages: &[Message]) -> usize {
            2048
        }

        fn hard_output_limit(&self) -> Option<usize> {
            Some(1024)
        }

        fn parse_into(
            &mut self,
            _messages: Vec<Message>,
        ) -> anyhow::Result<(TableData, Option<TableData>)> {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    arrow::datatypes::DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1_i64]))],
            )?;
            Ok((
                TableData::new(
                    "events".into(),
                    false,
                    batch,
                    crate::types::system_columns::SystemColumns::default(),
                ),
                None,
            ))
        }
    }

    impl Source for RecordingSource {
        fn read_batch(
            &mut self,
        ) -> futures_util::future::BoxFuture<'_, anyhow::Result<crate::types::message::MessageBatch>>
        {
            Box::pin(async { anyhow::bail!("recording source is commit-only") })
        }

        fn commit_offsets<'ctx>(
            &'ctx mut self,
            markers: &'ctx [CommitMarker],
        ) -> futures_util::future::BoxFuture<'ctx, anyhow::Result<()>> {
            Box::pin(async move {
                if self.fail_commit {
                    anyhow::bail!("injected grouped commit failure");
                }
                let group = markers
                    .iter()
                    .map(|marker| {
                        marker
                            .downcast_ref::<i64>()
                            .copied()
                            .ok_or_else(|| anyhow::anyhow!("unexpected marker"))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                self.groups
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(group);
                Ok(())
            })
        }
    }

    #[test]
    fn parser_shutdown_timeout_cannot_restart_over_live_blocking_work() {
        let error = ComponentOutcome::fatal_timeout("parser timeout")
            .result
            .expect_err("timeout must fail the pipeline");
        let failure = error
            .downcast_ref::<PipelineFailure>()
            .expect("parser timeout must preserve its restart contract");
        assert!(!failure.is_retryable());
    }

    #[tokio::test]
    async fn conservative_parser_estimate_is_not_a_correctness_rejection() {
        let memory = PipelineMemory::new(1024);
        let cancellation = CancellationToken::new();
        let (input_tx, input_rx) = mpsc::channel(1);
        let (output_tx, mut output_rx) = mpsc::channel(1);
        input_tx
            .send(ReadEnvelope {
                id: DeliveryId::new(1),
                messages: vec![Message::new(bytes::Bytes::from_static(b"{}"))],
                memory: Vec::new(),
                meta: DeliveryMeta { source_messages: 1 },
            })
            .await
            .unwrap();
        drop(input_tx);

        parser_loop(
            input_rx,
            output_tx,
            Arc::new(OverestimatedFactory),
            Arc::new(Vec::new()),
            memory,
            Arc::new(ParseCounters::new()),
            cancellation,
        )
        .await
        .unwrap();

        let delivery = output_rx.recv().await.expect("delivery must be produced");
        assert_eq!(delivery.outputs[0].rows(), 1);
    }

    #[tokio::test]
    async fn idle_parser_task_does_not_construct_a_session_or_hold_a_worker() {
        let created = Arc::new(AtomicU64::new(0));
        let factory = Arc::new(StatefulFactory {
            created: Arc::clone(&created),
        });
        let cancellation = CancellationToken::new();
        let (input_tx, input_rx) = mpsc::channel(1);
        let (output_tx, _output_rx) = mpsc::channel(1);
        let task = tokio::spawn(parser_loop(
            input_rx,
            output_tx,
            factory,
            Arc::new(Vec::new()),
            PipelineMemory::new(1024),
            Arc::new(ParseCounters::new()),
            cancellation.clone(),
        ));

        tokio::task::yield_now().await;
        assert_eq!(created.load(Ordering::Relaxed), 0);

        cancellation.cancel();
        drop(input_tx);
        task.await.unwrap().unwrap();
        assert_eq!(created.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn parser_session_state_is_preserved_across_blocking_workers() {
        let created = Arc::new(AtomicU64::new(0));
        let factory = Arc::new(StatefulFactory {
            created: Arc::clone(&created),
        });
        let cancellation = CancellationToken::new();
        let (input_tx, input_rx) = mpsc::channel(2);
        let (output_tx, mut output_rx) = mpsc::channel(2);
        for id in 1..=2 {
            input_tx
                .send(ReadEnvelope {
                    id: DeliveryId::new(id),
                    messages: vec![Message::new(bytes::Bytes::from_static(b"{}"))],
                    memory: Vec::new(),
                    meta: DeliveryMeta { source_messages: 1 },
                })
                .await
                .unwrap();
        }
        drop(input_tx);

        parser_loop(
            input_rx,
            output_tx,
            factory,
            Arc::new(Vec::new()),
            PipelineMemory::new(1 << 20),
            Arc::new(ParseCounters::new()),
            cancellation,
        )
        .await
        .unwrap();

        assert_eq!(created.load(Ordering::Relaxed), 1);
        for expected in 1..=2 {
            let delivery = output_rx.recv().await.expect("delivery must be produced");
            let values = delivery.outputs[0].batch.column(0);
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("state column must be Int64");
            assert_eq!(values.value(0), expected);
        }
    }

    #[tokio::test]
    async fn commit_through_submits_the_contiguous_prefix_as_one_source_group() {
        let groups = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut source: Box<dyn Source> = Box::new(RecordingSource {
            groups: Arc::clone(&groups),
            fail_commit: false,
        });
        let mut ledger = VecDeque::from([
            CommitEntry {
                id: DeliveryId::new(1),
                marker: Some(CommitMarker::new(11_i64)),
            },
            CommitEntry {
                id: DeliveryId::new(2),
                marker: None,
            },
            CommitEntry {
                id: DeliveryId::new(3),
                marker: Some(CommitMarker::new(33_i64)),
            },
        ]);

        let progress = PipelineProgress::new();
        commit_through(&mut source, &mut ledger, DeliveryId::new(3), &progress)
            .await
            .unwrap();

        assert!(ledger.is_empty());
        assert!(progress.advanced_since(0));
        assert_eq!(
            *groups
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![vec![11, 33]]
        );
    }

    #[tokio::test]
    async fn commit_through_rejects_an_unknown_sink_delivery_as_fatal() {
        let mut source: Box<dyn Source> = Box::new(RecordingSource {
            groups: Arc::new(std::sync::Mutex::new(Vec::new())),
            fail_commit: false,
        });
        let mut ledger = VecDeque::from([CommitEntry {
            id: DeliveryId::new(1),
            marker: Some(CommitMarker::new(11_i64)),
        }]);

        let error = commit_through(
            &mut source,
            &mut ledger,
            DeliveryId::new(2),
            &PipelineProgress::new(),
        )
        .await
        .expect_err("sink cannot commit a delivery the source never issued");

        let failure = error
            .downcast_ref::<PipelineFailure>()
            .expect("delivery protocol violations must keep their fatal disposition");
        assert!(!failure.is_retryable());
        assert_eq!(ledger.len(), 1);
    }

    #[tokio::test]
    async fn failed_grouped_commit_keeps_the_ledger_for_pipeline_recovery() {
        let mut source: Box<dyn Source> = Box::new(RecordingSource {
            groups: Arc::new(std::sync::Mutex::new(Vec::new())),
            fail_commit: true,
        });
        let mut ledger = VecDeque::from([
            CommitEntry {
                id: DeliveryId::new(1),
                marker: Some(CommitMarker::new(11_i64)),
            },
            CommitEntry {
                id: DeliveryId::new(2),
                marker: Some(CommitMarker::new(22_i64)),
            },
        ]);

        let progress = PipelineProgress::new();
        let error = commit_through(&mut source, &mut ledger, DeliveryId::new(2), &progress)
            .await
            .expect_err("injected source commit failure must propagate");

        assert!(error
            .to_string()
            .contains("source commit failed through delivery 2"));
        assert_eq!(ledger.len(), 2);
        assert!(!progress.advanced_since(0));
    }
}
