pub mod memory;
pub mod middleware;
pub mod sink;
pub mod source;

use std::collections::VecDeque;
use std::sync::Arc;
use std::thread;

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
const INITIAL_BACKOFF_MS: u64 = 10;
const MAX_BACKOFF_MS: u64 = 30_000;

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

fn make_sink_batch(data: TableData, reservation: MemoryReservation) -> Option<SinkBatch> {
    if data.batch.num_rows() == 0 {
        return None;
    }
    let byte_size = arrow_batch_bytes(&data.batch);
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
    let mut throttle_before_next = false;
    while !cancellation.is_cancelled() {
        if throttle_before_next {
            runtime.block_on(memory.wait_below_limit());
            if cancellation.is_cancelled() {
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
        counters.add_arrow_bytes(arrow_batch_bytes(&valid.batch) as u64);
        counters.add_unique_offsets(meta.source_messages);
        if let Some(dlq) = &dlq {
            counters.add_dlq_rows(dlq.batch.num_rows() as u64);
        }

        // Source buffers are no longer needed after parse. Release them before
        // waiting for Arrow capacity, avoiding a transform-stage deadlock.
        drop(source_memory);
        let output_bytes = arrow_batch_bytes(&valid.batch).saturating_add(
            dlq.as_ref()
                .map_or(0, |batch| arrow_batch_bytes(&batch.batch)),
        );
        let output_memory = memory.reserve_transform(output_bytes);
        let mut outputs = Vec::with_capacity(2);
        if let Some(batch) = make_sink_batch(valid, output_memory.clone()) {
            outputs.push(batch);
        }
        if let Some(dlq) = dlq {
            if let Some(batch) = make_sink_batch(dlq, output_memory.clone()) {
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
        throttle_before_next = memory.used() >= memory.limit();
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
            source.commit_offsets(&marker).await.map_err(|error| {
                anyhow::anyhow!("PQv1 commit failed at delivery {}: {error}", entry.id.get())
            })?;
        }
    }
    Ok(())
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
        let read = tokio::select! {
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
            ReadResult::Batch(batch) if batch.messages.is_empty() => {
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
            ReadResult::Failed(error) => return Err(error),
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
        if batch.memory.is_empty() {
            batch
                .memory
                .push(memory.reserve(meta.source_bytes as usize).await);
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
    let (parser_done_tx, parser_done_rx) = std::sync::mpsc::sync_channel(1);

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

    let result = tokio::select! {
        () = cancel_token.cancelled() => Ok(()),
        result = &mut reader_task => {
            result.map_err(|error| anyhow::anyhow!("reader task panicked: {error}"))?
        }
        result = &mut sink_task => {
            result.map_err(|error| anyhow::anyhow!("sink task panicked: {error}"))?
        }
        parser_result = async move {
            tokio::task::spawn_blocking(move || parser_done_rx.recv()).await
        } => {
            parser_result
                .map_err(|error| anyhow::anyhow!("parser monitor panicked: {error}"))?
                .map_err(|_| anyhow::anyhow!("parser completion channel closed"))?
        }
    };

    local.cancel();
    // Give the actor a chance to abort its owned INSERT task and release the
    // corresponding memory reservation. Aborting the actor first would detach
    // the INSERT JoinHandle and could strand the parser on the byte budget.
    if !sink_task.is_finished() {
        drop(tokio::time::timeout(Duration::from_secs(1), &mut sink_task).await);
    }
    sink_task.abort();
    if !reader_task.is_finished() {
        drop(tokio::time::timeout(Duration::from_secs(1), &mut reader_task).await);
    }
    reader_task.abort();
    parser_thread
        .join()
        .map_err(|_| anyhow::anyhow!("parser thread panicked"))?;
    result
}
