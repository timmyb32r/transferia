// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    validate_data_partition, ActiveAssignment, SessionFailure, DECODED_MESSAGE_METADATA_BYTES,
    DECODED_PART_METADATA_BYTES, DECODE_READ_CHUNK_SIZE, MAX_DECOMPRESSED_BATCH_SIZE,
    MAX_DECOMPRESSED_MESSAGE_SIZE, MAX_READ_BATCH_COUNT, MAX_READ_EXTRA_FIELD_COUNT,
    MAX_READ_MESSAGES_COUNT, MAX_READ_SIZE, MAX_ZSTD_WINDOW_LOG, MIN_VEC_ALLOCATION_CAPACITY,
    OUTPUT_MESSAGE_METADATA_BYTES,
};
use crate::connectors::logbroker::proto::pers_queue::v1::{
    migration_streaming_read_server_message, Codec, CommitCookie,
};
use crate::metrics::SourceCounters;
use transferia_core::memory::MemoryReservation;

/// One decompressed message within a partition part.
pub struct DecodedMessage {
    pub data: Bytes,
    /// Stable source offset within the `PQv1` partition.
    pub offset: u64,
    pub write_timestamp_ms: u64,
}

pub struct PqV1CommitMarker {
    pub partition_id: i64,
    pub cookies: Vec<CommitCookie>,
}

/// Raw (still-compressed) message handed off to the decompress pool.
pub(super) struct RawMsg {
    pub(super) data: Vec<u8>,
    pub(super) codec: i32,
    pub(super) uncompressed_size: u64,
    pub(super) offset: u64,
    pub(super) write_timestamp_ms: u64,
}

/// One partition's worth of raw messages within a `DataBatch`.
pub(super) struct RawPart {
    pub(super) pid: i64,
    pub(super) cookie: Option<CommitCookie>,
    pub(super) msgs: Vec<RawMsg>,
}

/// One partition's decompressed messages.
pub struct DecodedPart {
    pub(super) pid: i64,
    pub(super) cookie: Option<CommitCookie>,
    pub(super) msgs: Vec<DecodedMessage>,
    pub(super) memory: MemoryReservation,
}

/// A server data response after protocol validation, but before downstream admission.
/// The response task only enqueues this bounded work item; all waits happen in the data task so
/// commit acknowledgements and partition releases remain observable under backpressure.
pub(super) enum PendingDataKind {
    Decode { parts: Vec<RawPart> },
    Discard { parts: Vec<(i64, CommitCookie)> },
}

pub(super) struct PendingDataBatch {
    pub(super) kind: PendingDataKind,
    /// Source-stage credit acquired before the corresponding `Read` request.
    /// It accounts for this raw batch while it waits for transform admission.
    pub(super) raw_memory: MemoryReservation,
}

pub(super) fn prepare_data_batch(
    batch: migration_streaming_read_server_message::DataBatch,
    active_assignments: &mut HashMap<i64, ActiveAssignment>,
    discard_payload: bool,
    allow_ttl_rewind: bool,
) -> Result<(PendingDataKind, u64, u64), SessionFailure> {
    let result = (|| {
        let mut compressed_bytes = 0_u64;
        let mut message_count = 0_u64;
        if discard_payload {
            let mut parts = Vec::with_capacity(batch.partition_data.len());
            for partition in batch.partition_data {
                let (pid, cookie) = validate_data_partition(&partition, active_assignments)
                    .map_err(|failure| failure.error)?;
                let assignment = active_assignments
                    .get_mut(&pid)
                    .ok_or_else(|| anyhow!("PQv1 discarded data for inactive partition {pid}"))?;
                for message_batch in partition.batches {
                    for message in message_batch.message_data {
                        super::observe_offset(assignment, pid, message.offset, allow_ttl_rewind)?;
                        compressed_bytes = compressed_bytes
                            .checked_add(u64::try_from(message.data.len())?)
                            .ok_or_else(|| anyhow!("PQv1 compressed byte count overflow"))?;
                        message_count = message_count
                            .checked_add(1)
                            .ok_or_else(|| anyhow!("PQv1 message count overflow"))?;
                    }
                }
                parts.push((pid, cookie));
            }
            validate_message_count(message_count)?;
            return Ok((
                PendingDataKind::Discard { parts },
                compressed_bytes,
                message_count,
            ));
        }

        let mut parts = Vec::with_capacity(batch.partition_data.len());
        for partition in batch.partition_data {
            let (pid, cookie) = validate_data_partition(&partition, active_assignments)
                .map_err(|failure| failure.error)?;
            let assignment = active_assignments
                .get_mut(&pid)
                .ok_or_else(|| anyhow!("PQv1 data for inactive partition {pid}"))?;
            let mut messages = Vec::new();
            for message_batch in partition.batches {
                let write_timestamp_ms = message_batch.write_timestamp_ms;
                for message in message_batch.message_data {
                    super::observe_offset(assignment, pid, message.offset, allow_ttl_rewind)?;
                    compressed_bytes = compressed_bytes
                        .checked_add(u64::try_from(message.data.len())?)
                        .ok_or_else(|| anyhow!("PQv1 compressed byte count overflow"))?;
                    message_count = message_count
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("PQv1 message count overflow"))?;
                    messages.push(RawMsg {
                        data: message.data,
                        codec: message.codec,
                        uncompressed_size: message.uncompressed_size,
                        offset: message.offset,
                        write_timestamp_ms,
                    });
                }
            }
            parts.push(RawPart {
                pid,
                cookie: Some(cookie),
                msgs: messages,
            });
        }
        validate_message_count(message_count)?;
        Ok((
            PendingDataKind::Decode { parts },
            compressed_bytes,
            message_count,
        ))
    })();
    result.map_err(SessionFailure::fatal)
}

fn checked_raw_add(total: usize, value: usize) -> anyhow::Result<usize> {
    total
        .checked_add(value)
        .ok_or_else(|| anyhow!("PQv1 raw batch memory estimate overflow"))
}

fn checked_raw_capacity<T>(capacity: usize) -> anyhow::Result<usize> {
    capacity
        .checked_mul(core::mem::size_of::<T>())
        .ok_or_else(|| anyhow!("PQv1 raw batch memory estimate overflow"))
}

/// Maximum source credit for one advertised read. Repeated protobuf fields can retain nearly
/// twice their element count in `Vec` capacity, so fixed metadata is budgeted at 2x. Dynamic
/// strings/bytes are likewise budgeted above the wire-size limit; the received object is checked
/// against this credit before it can enter the admission queue.
pub(super) fn raw_read_credit_bytes(max_partitions: usize) -> anyhow::Result<usize> {
    use migration_streaming_read_server_message::data_batch::{Batch, MessageData, PartitionData};

    let max_messages = usize::try_from(MAX_READ_MESSAGES_COUNT)?;
    let dynamic_container_count = max_partitions
        .checked_mul(3)
        .and_then(|count| count.checked_add(MAX_READ_BATCH_COUNT.checked_mul(2)?))
        .and_then(|count| count.checked_add(MAX_READ_EXTRA_FIELD_COUNT.checked_mul(2)?))
        .and_then(|count| count.checked_add(max_messages.checked_mul(3)?))
        .ok_or_else(|| anyhow!("PQv1 raw read credit overflow"))?;
    let dynamic_capacity_slack = dynamic_container_count
        .checked_mul(MIN_VEC_ALLOCATION_CAPACITY)
        .ok_or_else(|| anyhow!("PQv1 raw read credit overflow"))?;
    let max_dynamic = usize::try_from(MAX_READ_SIZE)?
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(dynamic_capacity_slack))
        .ok_or_else(|| anyhow!("PQv1 raw read credit overflow"))?;
    let repeated_capacity = |elements: usize, containers: usize| {
        elements
            .checked_mul(2)
            .and_then(|capacity| {
                capacity.checked_add(containers.checked_mul(MIN_VEC_ALLOCATION_CAPACITY)?)
            })
            .ok_or_else(|| anyhow!("PQv1 raw read credit overflow"))
    };
    let mut bytes = core::mem::size_of::<migration_streaming_read_server_message::DataBatch>();
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<PartitionData>(repeated_capacity(max_partitions, 1)?)?,
    )?;
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<Batch>(repeated_capacity(MAX_READ_BATCH_COUNT, max_partitions)?)?,
    )?;
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<MessageData>(repeated_capacity(
            max_messages,
            MAX_READ_BATCH_COUNT,
        )?)?,
    )?;
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<crate::connectors::logbroker::proto::pers_queue::v1::KeyValue>(
            repeated_capacity(MAX_READ_EXTRA_FIELD_COUNT, MAX_READ_BATCH_COUNT)?,
        )?,
    )?;
    checked_raw_add(bytes, max_dynamic)
}

pub(super) fn validate_raw_data_batch(
    batch: &migration_streaming_read_server_message::DataBatch,
    max_partitions: usize,
    reserved_bytes: usize,
) -> anyhow::Result<usize> {
    use migration_streaming_read_server_message::data_batch::{Batch, MessageData, PartitionData};

    anyhow::ensure!(
        batch.partition_data.len() <= max_partitions,
        "PQv1 DataBatch contains {} partition parts, exceeding active partition count {max_partitions}",
        batch.partition_data.len()
    );

    let mut seen_partitions = HashSet::with_capacity(batch.partition_data.len());
    let mut batch_count = 0_usize;
    let mut message_count = 0_usize;
    let mut extra_field_count = 0_usize;
    let mut raw_payload_bytes = 0_usize;
    let mut bytes = core::mem::size_of::<migration_streaming_read_server_message::DataBatch>();
    bytes = checked_raw_add(
        bytes,
        checked_raw_capacity::<PartitionData>(batch.partition_data.capacity())?,
    )?;
    for partition in &batch.partition_data {
        anyhow::ensure!(
            seen_partitions.insert(partition.partition),
            "PQv1 DataBatch repeats partition {}",
            partition.partition
        );
        bytes = checked_raw_add(bytes, partition.cluster.capacity())?;
        bytes = checked_raw_add(bytes, partition.deprecated_topic.capacity())?;
        if let Some(topic) = &partition.topic {
            bytes = checked_raw_add(bytes, topic.path.capacity())?;
        }
        batch_count = batch_count
            .checked_add(partition.batches.len())
            .ok_or_else(|| anyhow!("PQv1 DataBatch batch count overflow"))?;
        bytes = checked_raw_add(
            bytes,
            checked_raw_capacity::<Batch>(partition.batches.capacity())?,
        )?;
        for message_batch in &partition.batches {
            bytes = checked_raw_add(bytes, message_batch.source_id.capacity())?;
            bytes = checked_raw_add(bytes, message_batch.ip.capacity())?;
            extra_field_count = extra_field_count
                .checked_add(message_batch.extra_fields.len())
                .ok_or_else(|| anyhow!("PQv1 DataBatch extra-field count overflow"))?;
            bytes = checked_raw_add(
                bytes,
                checked_raw_capacity::<
                    crate::connectors::logbroker::proto::pers_queue::v1::KeyValue,
                >(message_batch.extra_fields.capacity())?,
            )?;
            for field in &message_batch.extra_fields {
                bytes = checked_raw_add(bytes, field.key.capacity())?;
                bytes = checked_raw_add(bytes, field.value.capacity())?;
            }
            message_count = message_count
                .checked_add(message_batch.message_data.len())
                .ok_or_else(|| anyhow!("PQv1 DataBatch message count overflow"))?;
            bytes = checked_raw_add(
                bytes,
                checked_raw_capacity::<MessageData>(message_batch.message_data.capacity())?,
            )?;
            for message in &message_batch.message_data {
                raw_payload_bytes = raw_payload_bytes
                    .checked_add(message.data.len())
                    .ok_or_else(|| anyhow!("PQv1 DataBatch raw payload size overflow"))?;
                bytes = checked_raw_add(bytes, message.data.capacity())?;
                bytes = checked_raw_add(bytes, message.partition_key.capacity())?;
                bytes = checked_raw_add(bytes, message.explicit_hash.capacity())?;
            }
        }
    }
    anyhow::ensure!(
        batch_count <= MAX_READ_BATCH_COUNT,
        "PQv1 DataBatch contains {batch_count} batches, exceeding limit {MAX_READ_BATCH_COUNT}"
    );
    anyhow::ensure!(
        extra_field_count <= MAX_READ_EXTRA_FIELD_COUNT,
        "PQv1 DataBatch contains {extra_field_count} extra fields, exceeding limit {MAX_READ_EXTRA_FIELD_COUNT}"
    );
    anyhow::ensure!(
        raw_payload_bytes <= usize::try_from(MAX_READ_SIZE)?,
        "PQv1 DataBatch raw payload size {raw_payload_bytes} exceeds requested limit {MAX_READ_SIZE}"
    );
    validate_message_count(u64::try_from(message_count)?)?;
    anyhow::ensure!(
        bytes <= reserved_bytes,
        "PQv1 DataBatch retained size {bytes} exceeds pre-reserved read credit {reserved_bytes}"
    );
    Ok(bytes.max(1))
}

pub(super) fn pending_raw_bytes(kind: &PendingDataKind) -> anyhow::Result<usize> {
    let mut bytes = core::mem::size_of::<PendingDataKind>();
    match kind {
        PendingDataKind::Decode { parts } => {
            bytes = checked_raw_add(bytes, checked_raw_capacity::<RawPart>(parts.capacity())?)?;
            for part in parts {
                bytes =
                    checked_raw_add(bytes, checked_raw_capacity::<RawMsg>(part.msgs.capacity())?)?;
                for message in &part.msgs {
                    bytes = checked_raw_add(bytes, message.data.capacity())?;
                }
            }
        }
        PendingDataKind::Discard { parts } => {
            bytes = checked_raw_add(
                bytes,
                checked_raw_capacity::<(i64, CommitCookie)>(parts.capacity())?,
            )?;
        }
    }
    Ok(bytes.max(1))
}

pub(super) fn validate_message_count(message_count: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        message_count <= u64::from(MAX_READ_MESSAGES_COUNT),
        "PQv1 DataBatch contains {message_count} messages, exceeding requested limit {MAX_READ_MESSAGES_COUNT}"
    );
    Ok(())
}

pub(super) fn enqueue_pending_data(
    sender: &mpsc::Sender<PendingDataBatch>,
    batch: PendingDataBatch,
) -> Result<(), SessionFailure> {
    match sender.try_send(batch) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => Err(SessionFailure::fatal(anyhow!(
            "PQv1 protocol violation: received DataBatch without available read credit"
        ))),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(SessionFailure::retryable(anyhow!(
            "PQv1 data admission channel closed"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Decompression
// ---------------------------------------------------------------------------

fn declared_uncompressed_size(uncompressed_size: u64) -> anyhow::Result<usize> {
    let size = usize::try_from(uncompressed_size)
        .map_err(|_| anyhow!("declared uncompressed size does not fit in usize"))?;
    anyhow::ensure!(
        size <= MAX_DECOMPRESSED_MESSAGE_SIZE,
        "declared uncompressed size {size} exceeds limit {MAX_DECOMPRESSED_MESSAGE_SIZE}"
    );
    Ok(size)
}

pub(super) const fn decoded_part_retained_bytes(message_count: usize) -> usize {
    DECODED_PART_METADATA_BYTES.saturating_add(message_count.saturating_mul(
        DECODED_MESSAGE_METADATA_BYTES.saturating_add(OUTPUT_MESSAGE_METADATA_BYTES),
    ))
}

pub(super) fn decoded_batch_retained_bytes(parts: &[RawPart]) -> anyhow::Result<usize> {
    decoded_batch_bytes(parts, true)
}

/// Extra allocation required while the raw protobuf batch is still alive.
/// RAW payload buffers are moved into `Bytes`, so counting their capacity a
/// second time would manufacture pressure without representing real memory.
pub(super) fn decoded_batch_additional_bytes(parts: &[RawPart]) -> anyhow::Result<usize> {
    decoded_batch_bytes(parts, false)
}

fn decoded_batch_bytes(parts: &[RawPart], include_raw_payload: bool) -> anyhow::Result<usize> {
    let mut retained = 0_usize;
    let mut decoded_total = 0_usize;
    for part in parts {
        retained = retained
            .checked_add(DECODED_PART_METADATA_BYTES)
            .and_then(|total| {
                total.checked_add(part.msgs.len().checked_mul(
                    DECODED_MESSAGE_METADATA_BYTES.checked_add(OUTPUT_MESSAGE_METADATA_BYTES)?,
                )?)
            })
            .ok_or_else(|| anyhow!("PQv1 decoded batch metadata estimate overflow"))?;
        for message in &part.msgs {
            let decoded = declared_uncompressed_size(message.uncompressed_size)?;
            let retained_payload = if message.codec == Codec::Raw as i32 {
                anyhow::ensure!(
                    message.data.len() == decoded,
                    "RAW decoded size mismatch: declared={decoded}, actual={}",
                    message.data.len()
                );
                if include_raw_payload {
                    message.data.capacity()
                } else {
                    0
                }
            } else {
                decoded
            };
            decoded_total = decoded_total
                .checked_add(decoded)
                .ok_or_else(|| anyhow!("PQv1 decoded batch size overflow"))?;
            anyhow::ensure!(
                decoded_total <= MAX_DECOMPRESSED_BATCH_SIZE,
                "declared uncompressed batch size {decoded_total} exceeds limit {MAX_DECOMPRESSED_BATCH_SIZE}"
            );
            retained = retained
                .checked_add(retained_payload)
                .ok_or_else(|| anyhow!("PQv1 decoded batch memory estimate overflow"))?;
        }
    }
    Ok(retained.max(1))
}

pub(super) fn batch_uses_only_raw_codec(parts: &[RawPart]) -> bool {
    parts
        .iter()
        .flat_map(|part| &part.msgs)
        .all(|message| message.codec == Codec::Raw as i32)
}

#[derive(Debug)]
pub(super) struct DecodeCancelled;

impl core::fmt::Display for DecodeCancelled {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("PQv1 decompression cancelled")
    }
}

impl std::error::Error for DecodeCancelled {}

fn ensure_decode_active(cancellation: &CancellationToken) -> anyhow::Result<()> {
    if cancellation.is_cancelled() {
        return Err(DecodeCancelled.into());
    }
    Ok(())
}

pub(super) fn decode_parts_with_cancellation(
    parts: Vec<RawPart>,
    reservation: &MemoryReservation,
    counters: &SourceCounters,
    cancellation: &CancellationToken,
) -> anyhow::Result<Vec<DecodedPart>> {
    let mut decoded_parts = Vec::with_capacity(parts.len());
    let mut retained_bytes = 0_usize;
    for RawPart { pid, cookie, msgs } in parts {
        retained_bytes = retained_bytes
            .checked_add(decoded_part_retained_bytes(msgs.len()))
            .ok_or_else(|| anyhow!("PQv1 decoded batch metadata size overflow"))?;
        let mut decoded = Vec::with_capacity(msgs.len());
        let mut decomp_busy = core::time::Duration::ZERO;
        let mut decompressed_bytes = 0_u64;
        for message in msgs {
            ensure_decode_active(cancellation)?;
            let codec = message.codec;
            let offset = message.offset;
            let raw_capacity = message.data.capacity();
            let started = std::time::Instant::now();
            let data = match decompress_with_cancellation(
                message.data,
                codec,
                message.uncompressed_size,
                cancellation,
            ) {
                Ok(data) => data,
                Err(error) if error.downcast_ref::<DecodeCancelled>().is_some() => {
                    return Err(error);
                }
                Err(error) => {
                    decomp_busy += started.elapsed();
                    counters.add_network_decode_busy(decomp_busy);
                    counters.add_network_decoded_bytes(decompressed_bytes);
                    return Err(anyhow!(
                        "PQv1 decompress failed: codec={codec} offset={offset}: {error}"
                    ));
                }
            };
            let retained_payload = if codec == Codec::Raw as i32 {
                raw_capacity
            } else {
                data.len()
            };
            decomp_busy += started.elapsed();
            decompressed_bytes = decompressed_bytes.saturating_add(data.len() as u64);
            retained_bytes = retained_bytes
                .checked_add(retained_payload)
                .ok_or_else(|| anyhow!("PQv1 decoded batch size overflow"))?;
            decoded.push(DecodedMessage {
                data,
                offset,
                write_timestamp_ms: message.write_timestamp_ms,
            });
        }
        counters.add_network_decode_busy(decomp_busy);
        counters.add_network_decoded_bytes(decompressed_bytes);
        decoded_parts.push(DecodedPart {
            pid,
            cookie,
            msgs: decoded,
            memory: reservation.clone(),
        });
    }
    // Compressed inputs have been dropped. Keep accounting only for the decoded
    // buffers retained by `DecodedPart` clones of this reservation.
    let _shrunk = reservation.shrink_to(retained_bytes);
    Ok(decoded_parts)
}

/// Decompress a message body. RAW (codec 1) reuses the input buffer (zero-copy).
pub(super) fn decompress_with_cancellation(
    data: Vec<u8>,
    codec: i32,
    uncompressed_size: u64,
    cancellation: &CancellationToken,
) -> anyhow::Result<Bytes> {
    ensure_decode_active(cancellation)?;
    let expected_size = declared_uncompressed_size(uncompressed_size)?;
    let decoded = match Codec::try_from(codec).ok() {
        Some(Codec::Raw) => Bytes::from(data), // RAW — move, no copy
        Some(Codec::Gzip) => {
            let decoder = flate2::read::GzDecoder::new(&*data);
            read_exact_decoded(decoder, expected_size, cancellation)?
        }
        Some(Codec::Zstd) => {
            let mut decoder = zstd::stream::read::Decoder::new(&*data)?;
            decoder.window_log_max(MAX_ZSTD_WINDOW_LOG)?;
            read_exact_decoded(decoder, expected_size, cancellation)?
        }
        Some(Codec::Unspecified | Codec::Lzop) | None => {
            return Err(anyhow!("Unsupported codec: {codec}"));
        }
    };
    anyhow::ensure!(
        decoded.len() == expected_size,
        "decoded size mismatch: declared={expected_size}, actual={}",
        decoded.len()
    );
    Ok(decoded)
}

/// Decode into an exactly-sized buffer, then probe one extra byte without growing it.
/// This keeps the actual allocation within the pre-accounted decoded size even when a malformed
/// stream expands beyond its declaration.
pub(super) fn read_exact_decoded(
    mut decoder: impl std::io::Read,
    expected_size: usize,
    cancellation: &CancellationToken,
) -> anyhow::Result<Bytes> {
    ensure_decode_active(cancellation)?;
    let mut decoded = vec![0_u8; expected_size];
    let mut actual_size = 0_usize;
    while actual_size < expected_size {
        ensure_decode_active(cancellation)?;
        let chunk_end = actual_size
            .saturating_add(DECODE_READ_CHUNK_SIZE)
            .min(expected_size);
        match decoder.read(&mut decoded[actual_size..chunk_end]) {
            Ok(0) => break,
            Ok(read) => actual_size += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::ensure!(
        actual_size == expected_size,
        "decoded size mismatch: declared={expected_size}, actual={actual_size}"
    );
    ensure_decode_active(cancellation)?;
    let mut extra = [0_u8; 1];
    let extra_size = decoder.read(&mut extra)?;
    anyhow::ensure!(
        extra_size == 0,
        "decoded size mismatch: declared={expected_size}, actual_at_least={}",
        expected_size.saturating_add(extra_size)
    );
    Ok(Bytes::from(decoded))
}
