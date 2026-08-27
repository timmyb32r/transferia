use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use arrow::record_batch::RecordBatch;
use prost::Message as _;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use transferia_core::data::schema::DatasetSchema;

use super::columnar_chunk::{
    ChunkMeta, ColumnarChunkDecoder, DataBlockMetaExt, EXTENSION_COLUMN_META,
    EXTENSION_DATA_BLOCK_META, EXTENSION_MISC, data_block_meta, has_extension,
};
use super::config::YTsaurusPartitionTablesConfig;
use super::native_rpc::{
    DataNodeRpcClient, PARTITION_MODE_UNORDERED, crc64, receive_read_worker_item,
    partition_tables,
};

const WORKLOAD_CATEGORY_USER_BATCH: i32 = 3;
const NODE_ID_MASK: u64 = (1 << 24) - 1;
const MEDIUM_INDEX_SHIFT: u32 = 29;
const MEDIUM_INDEX_MASK: u64 = (1 << 7) - 1;
const YSON_BINARY_STRING: u8 = 0x01;

pub(super) struct NativeDirectPartitionedReadStream {
    receiver: mpsc::Receiver<anyhow::Result<DirectReadBlock>>,
    tasks: JoinSet<()>,
    service_ticket_refresh: JoinHandle<()>,
    queued: Option<DirectReadBlock>,
}

pub(super) struct DirectReadBlock {
    pub(super) network_raw_bytes: u64,
    pub(super) network_decoded_bytes: u64,
    pub(super) network_decode_duration: Duration,
    pub(super) payload: DirectReadPayload,
    pub(super) stream_id: usize,
    pub(super) end_of_stream: bool,
    pub(super) cumulative_rows: Option<u64>,
}

pub(super) enum DirectReadPayload {
    Count,
    End,
    RecordBatch(RecordBatch),
}

#[derive(Clone, Copy, Default)]
struct ChunkReadStats {
    network_bytes: u64,
    decoded_rows: u64,
}

type DecodedDirectBlock = (u64, u64, u64, DirectReadPayload, Duration);

impl NativeDirectPartitionedReadStream {
    pub(super) async fn open(
        rpc_proxy_endpoints: &[String],
        token: &str,
        path: &str,
        config: YTsaurusPartitionTablesConfig,
        service_ticket_file: &str,
        schema: Option<DatasetSchema>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            config.direct_data_node_access,
            "direct YTsaurus partition reader requires direct_data_node_access=true"
        );
        let partitions = partition_tables(rpc_proxy_endpoints, token, path, config).await?;
        anyhow::ensure!(
            !partitions.is_empty(),
            "YTsaurus PartitionTables returned no partitions for '{path}'"
        );
        let concurrency = config.concurrency.min(partitions.len());
        tracing::info!(
            table_path = path,
            partition_count = partitions.len(),
            concurrency,
            "YTsaurus direct data-node partition plan created"
        );

        let partitions = Arc::new(partitions);
        let (service_ticket, service_ticket_refresh) =
            rotating_service_ticket(service_ticket_file).await?;
        let failed = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::channel(concurrency.saturating_mul(2).max(1));
        let mut tasks = JoinSet::new();
        for worker in 0..concurrency {
            let partitions = Arc::clone(&partitions);
            let service_ticket = service_ticket.clone();
            let failed = Arc::clone(&failed);
            let sender = sender.clone();
            let schema = schema.clone();
            tasks.spawn(async move {
                let mut clients = HashMap::new();
                for partition_index in (worker..partitions.len()).step_by(concurrency) {
                    if failed.load(Ordering::Acquire) {
                        return;
                    }
                    let partition = &partitions[partition_index];
                    let result = read_partition(
                        partition_index,
                        &partition.cookie,
                        partition.row_count,
                        config.direct_blocks_per_request,
                        &service_ticket,
                        &sender,
                        &mut clients,
                        schema.as_ref(),
                    )
                    .await;
                    if let Err(error) = result {
                        publish_failure(&failed, &sender, error).await;
                        return;
                    }
                }
            });
        }
        drop(sender);
        let mut stream = Self {
            receiver,
            tasks,
            service_ticket_refresh,
            queued: None,
        };
        stream.queued = Some(stream.receive_block().await?.ok_or_else(|| {
            anyhow::anyhow!(
                "YTsaurus direct data-node readers ended before returning a block"
            )
        })?);
        Ok(stream)
    }

    pub(super) async fn next_block(&mut self) -> anyhow::Result<Option<DirectReadBlock>> {
        if let Some(block) = self.queued.take() {
            return Ok(Some(block));
        }
        self.receive_block().await
    }

    async fn receive_block(&mut self) -> anyhow::Result<Option<DirectReadBlock>> {
        receive_read_worker_item(
            &mut self.receiver,
            &mut self.tasks,
            "YTsaurus direct data-node reader",
        )
        .await
    }
}

impl Drop for NativeDirectPartitionedReadStream {
    fn drop(&mut self) {
        self.tasks.abort_all();
        self.service_ticket_refresh.abort();
    }
}

async fn rotating_service_ticket(
    configured_path: &str,
) -> anyhow::Result<(watch::Receiver<Arc<str>>, JoinHandle<()>)> {
    let expanded = shellexpand::full(configured_path)?;
    let path = PathBuf::from(expanded.as_ref());
    let initial = read_service_ticket(&path).await?;
    let (sender, receiver) = watch::channel(initial);
    let task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            match read_service_ticket(&path).await {
                Ok(ticket) => {
                    if ticket.as_ref() != sender.borrow().as_ref() {
                        sender.send_replace(ticket);
                        tracing::info!(
                            service_ticket_file = %path.display(),
                            "reloaded rotated YTsaurus native RPC service ticket"
                        );
                    }
                }
                Err(error) => tracing::warn!(
                    service_ticket_file = %path.display(),
                    error = %error,
                    "could not reload YTsaurus native RPC service ticket; retaining the last valid ticket"
                ),
            }
        }
    });
    Ok((receiver, task))
}

async fn read_service_ticket(path: &Path) -> anyhow::Result<Arc<str>> {
    let value = tokio::fs::read_to_string(path).await.map_err(|error| {
        anyhow::anyhow!(
            "failed to read YTsaurus native RPC service-ticket file '{}': {error}",
            path.display()
        )
    })?;
    let value = value.trim();
    anyhow::ensure!(
        !value.is_empty(),
        "YTsaurus native RPC service-ticket file '{}' is empty",
        path.display()
    );
    Ok(Arc::from(value))
}

async fn publish_failure(
    failed: &AtomicBool,
    sender: &mpsc::Sender<anyhow::Result<DirectReadBlock>>,
    error: anyhow::Error,
) {
    if failed
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        drop(sender.send(Err(error)).await);
    }
}

async fn read_partition(
    partition_index: usize,
    signed_cookie: &Bytes,
    expected_rows: Option<u64>,
    blocks_per_request: usize,
    service_ticket: &watch::Receiver<Arc<str>>,
    sender: &mpsc::Sender<anyhow::Result<DirectReadBlock>>,
    clients: &mut HashMap<String, DataNodeRpcClient>,
    schema: Option<&DatasetSchema>,
) -> anyhow::Result<()> {
    let row_count = expected_rows.ok_or_else(|| {
        anyhow::anyhow!("YTsaurus partition {partition_index} has no aggregate row count")
    })?;
    let payload = signature_payload(signed_cookie)?;
    let cookie = TablePartitionCookie::decode(payload)?;
    anyhow::ensure!(
        cookie.partition_mode == Some(PARTITION_MODE_UNORDERED),
        "YTsaurus direct data-node reader expected an unordered PartitionTables cookie, got {:?}",
        cookie.partition_mode
    );
    let directory = cookie
        .node_directory
        .ok_or_else(|| anyhow::anyhow!(
            "YTsaurus partition {partition_index} cookie has no data-node directory"
        ))?;
    let nodes = node_endpoints(directory)?;
    let chunks = cookie
        .table_input_specs
        .into_iter()
        .flat_map(|input| input.chunk_specs)
        .collect::<Vec<_>>();
    let mut prepared = Vec::with_capacity(chunks.len());
    let mut planned_rows = 0_u64;
    for chunk in &chunks {
        let plan = prepare_chunk(chunk, &nodes, service_ticket, clients, schema).await?;
        planned_rows = planned_rows
            .checked_add(plan.selected_rows)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus partition row count overflow"))?;
        prepared.push(plan);
    }
    anyhow::ensure!(
        planned_rows == row_count,
        "YTsaurus direct data-node partition {partition_index} selects {planned_rows} rows, but PartitionTables declared {row_count}; refusing to emit a partial or duplicated partition"
    );

    let mut network_bytes = 0_u64;
    let mut decoded_rows = 0_u64;
    for chunk in &prepared {
        let stats = read_prepared_chunk(
            chunk,
            blocks_per_request,
            decoded_rows,
            service_ticket,
            clients,
            schema,
            sender,
            partition_index,
        )
        .await?;
        network_bytes = network_bytes
            .checked_add(stats.network_bytes)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus partition network byte count overflow"))?;
        decoded_rows = decoded_rows
            .checked_add(stats.decoded_rows)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus partition row count overflow"))?;
    }
    anyhow::ensure!(
        decoded_rows == row_count,
        "YTsaurus direct data-node partition {partition_index} read {decoded_rows} rows, but PartitionTables declared {row_count}"
    );
    tracing::debug!(
        partition_index,
        chunk_specs = prepared.len(),
        row_count,
        network_bytes,
        "YTsaurus direct data-node partition completed"
    );
    drop(
        sender
            .send(Ok(DirectReadBlock {
                network_raw_bytes: 0,
                network_decoded_bytes: 0,
                network_decode_duration: Duration::ZERO,
                payload: DirectReadPayload::End,
                stream_id: partition_index,
                end_of_stream: true,
                cumulative_rows: Some(row_count),
            }))
            .await,
    );
    Ok(())
}

struct PreparedChunk {
    chunk_id: ProtoGuid,
    replicas: Vec<Replica>,
    meta: ChunkMeta,
    selections: Vec<BlockSelection>,
    selected_rows: u64,
}

async fn prepare_chunk(
    chunk: &ChunkSpec,
    nodes: &HashMap<u32, String>,
    service_ticket: &watch::Receiver<Arc<str>>,
    clients: &mut HashMap<String, DataNodeRpcClient>,
    schema: Option<&DatasetSchema>,
) -> anyhow::Result<PreparedChunk> {
    let include_columns = schema.is_some();
    anyhow::ensure!(
        chunk.erasure_codec.unwrap_or(0) == 0 && !chunk.striped_erasure.unwrap_or(false),
        "direct YTsaurus data-node reads do not support erasure-coded chunks"
    );
    anyhow::ensure!(
        !chunk.use_proxying_data_node_service.unwrap_or(false),
        "YTsaurus chunk requires proxying DataNodeService; direct mode refuses proxy fallback"
    );
    let chunk_id = chunk
        .chunk_id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("YTsaurus chunk spec has no chunk id"))?;
    let replicas = chunk_replicas(chunk, nodes)?;
    anyhow::ensure!(!replicas.is_empty(), "YTsaurus chunk has no reachable data-node replica");
    let embedded_meta = chunk.chunk_meta.as_ref().filter(|meta| {
        has_extension(meta, EXTENSION_DATA_BLOCK_META)
            && (!include_columns
                || {
                    has_extension(meta, EXTENSION_MISC)
                        && has_extension(meta, EXTENSION_COLUMN_META)
                })
    });
    let meta = if let Some(meta) = embedded_meta {
        meta.clone()
    } else {
        fetch_chunk_meta_from_replicas(
            &chunk_id,
            &replicas,
            service_ticket,
            clients,
            include_columns,
        )
        .await?
    };
    let block_meta = data_block_meta(&meta)?;
    let selections = selected_block_ranges(chunk, &block_meta)?;
    let selected_rows = selections.iter().try_fold(0_u64, |total, selection| {
        let rows = u64::try_from(selection.upper_row_index - selection.lower_row_index)?;
        total
            .checked_add(rows)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus selected row count overflow"))
    })?;
    schema
        .map(|schema| ColumnarChunkDecoder::from_meta(&meta, schema))
        .transpose()?;
    Ok(PreparedChunk {
        chunk_id,
        replicas,
        meta,
        selections,
        selected_rows,
    })
}

async fn read_prepared_chunk(
    chunk: &PreparedChunk,
    blocks_per_request: usize,
    partition_rows_before: u64,
    service_ticket: &watch::Receiver<Arc<str>>,
    clients: &mut HashMap<String, DataNodeRpcClient>,
    schema: Option<&DatasetSchema>,
    sender: &mpsc::Sender<anyhow::Result<DirectReadBlock>>,
    partition_index: usize,
) -> anyhow::Result<ChunkReadStats> {
    let decoder = schema
        .map(|schema| ColumnarChunkDecoder::from_meta(&chunk.meta, schema))
        .transpose()?
        .map(Arc::new);
    let mut network_bytes = 0_u64;
    let mut decoded_rows = 0_u64;
    let mut selection_groups = chunk.selections.chunks(blocks_per_request);
    let Some(first_selections) = selection_groups.next() else {
        return Ok(ChunkReadStats::default());
    };
    let mut current_selections = first_selections.to_vec();
    let mut current_blocks = fetch_block_group(
        &chunk.chunk_id,
        &chunk.replicas,
        &current_selections,
        service_ticket,
        clients,
    )
    .await?;
    loop {
        let decode = decode_direct_blocks(
            decoder.clone(),
            current_blocks,
            current_selections,
        );
        let next = if let Some(next_selections) = selection_groups.next() {
            let fetch = fetch_block_group(
                &chunk.chunk_id,
                &chunk.replicas,
                next_selections,
                service_ticket,
                clients,
            );
            let (decoded, blocks) = tokio::join!(decode, fetch);
            Some((decoded?, blocks?, next_selections.to_vec()))
        } else {
            let decoded = decode.await?;
            if !emit_direct_blocks(
                decoded,
                &mut network_bytes,
                &mut decoded_rows,
                partition_rows_before,
                schema.is_none(),
                sender,
                partition_index,
            )
            .await?
            {
                return Ok(ChunkReadStats {
                    network_bytes,
                    decoded_rows,
                });
            }
            break;
        };
        let (decoded, blocks, selections) = next.expect("next block group exists");
        if !emit_direct_blocks(
            decoded,
            &mut network_bytes,
            &mut decoded_rows,
            partition_rows_before,
            schema.is_none(),
            sender,
            partition_index,
        )
        .await?
        {
            return Ok(ChunkReadStats {
                network_bytes,
                decoded_rows,
            });
        }
        current_blocks = blocks;
        current_selections = selections;
    }
    Ok(ChunkReadStats {
        network_bytes,
        decoded_rows,
    })
}

async fn decode_direct_blocks(
    decoder: Option<Arc<ColumnarChunkDecoder>>,
    blocks: Vec<Bytes>,
    selections: Vec<BlockSelection>,
) -> anyhow::Result<Vec<DecodedDirectBlock>> {
    let Some(decoder) = decoder else {
        return blocks
            .into_iter()
            .zip(selections)
            .map(|(block, selection)| {
                Ok((
                    u64::try_from(block.len())?,
                    0,
                    u64::try_from(selection.upper_row_index - selection.lower_row_index)?,
                    DirectReadPayload::Count,
                    Duration::ZERO,
                ))
            })
            .collect();
    };
    tokio::task::spawn_blocking(move || {
        blocks
            .into_iter()
            .zip(selections)
            .map(|(block, selection)| {
                let block_bytes = u64::try_from(block.len())?;
                let started = Instant::now();
                let (batch, decoded_bytes) = decoder.decode_block(
                    selection.block_index,
                    &block,
                    selection.lower_row_index,
                    selection.upper_row_index,
                )?;
                Ok((
                    block_bytes,
                    u64::try_from(decoded_bytes)?,
                    u64::try_from(batch.num_rows())?,
                    DirectReadPayload::RecordBatch(batch),
                    started.elapsed(),
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()
    })
    .await
    .map_err(|error| anyhow::anyhow!("YTsaurus block decoder task failed: {error}"))?
}

async fn emit_direct_blocks(
    decoded: Vec<DecodedDirectBlock>,
    network_bytes: &mut u64,
    decoded_rows: &mut u64,
    partition_rows_before: u64,
    count_only: bool,
    sender: &mpsc::Sender<anyhow::Result<DirectReadBlock>>,
    partition_index: usize,
) -> anyhow::Result<bool> {
    for (
        block_bytes,
        network_decoded_bytes,
        block_rows,
        payload,
        network_decode_duration,
    ) in decoded
    {
        *network_bytes = (*network_bytes)
            .checked_add(block_bytes)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus chunk network byte count overflow"))?;
        *decoded_rows = (*decoded_rows)
            .checked_add(block_rows)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus chunk row count overflow"))?;
        let cumulative_rows = if count_only {
            Some(
                partition_rows_before
                    .checked_add(*decoded_rows)
                    .ok_or_else(|| anyhow::anyhow!("YTsaurus partition row count overflow"))?,
            )
        } else {
            None
        };
        if sender
            .send(Ok(DirectReadBlock {
                network_raw_bytes: block_bytes,
                network_decoded_bytes,
                network_decode_duration,
                payload,
                stream_id: partition_index,
                end_of_stream: false,
                cumulative_rows,
            }))
            .await
            .is_err()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn fetch_chunk_meta_from_replicas(
    chunk_id: &ProtoGuid,
    replicas: &[Replica],
    service_ticket: &watch::Receiver<Arc<str>>,
    clients: &mut HashMap<String, DataNodeRpcClient>,
    include_columns: bool,
) -> anyhow::Result<ChunkMeta> {
    let mut failures = Vec::new();
    for replica in replicas {
        let endpoint = replica.endpoint.clone();
        let result = async {
            let client = data_node_client(clients, replica, service_ticket).await?;
            fetch_chunk_meta(
                client,
                chunk_id.clone(),
                replica.encoded,
                include_columns,
            )
            .await
        }
        .await;
        match result {
            Ok(meta) => return Ok(meta),
            Err(error) => {
                clients.remove(&endpoint);
                failures.push(format!("{endpoint}: {error:#}"));
            }
        }
    }
    anyhow::bail!(
        "all direct YTsaurus data-node replicas failed to return chunk metadata: {}",
        failures.join("; ")
    )
}

async fn fetch_block_group(
    chunk_id: &ProtoGuid,
    replicas: &[Replica],
    selections: &[BlockSelection],
    service_ticket: &watch::Receiver<Arc<str>>,
    clients: &mut HashMap<String, DataNodeRpcClient>,
) -> anyhow::Result<Vec<Bytes>> {
    let mut failures = Vec::new();
    for replica in replicas {
        let endpoint = replica.endpoint.clone();
        let result = async {
            let client = data_node_client(clients, replica, service_ticket).await?;
            let request = GetBlockSetRequest {
                chunk_id: Some(chunk_id.clone()),
                block_indexes: selections
                    .iter()
                    .map(|selection| selection.block_index)
                    .collect(),
                populate_cache: Some(false),
                workload_descriptor: Some(user_batch_workload()),
                fetch_from_cache: Some(true),
                fetch_from_disk: Some(true),
                enable_p2p: Some(false),
                replica_spec: Some(ChunkReplicaSpec {
                    encoded_chunk_replica_with_medium: replica.encoded,
                }),
            };
            let (response, attachments): (GetBlockSetResponse, _) = client
                .invoke("GetBlockSet", Bytes::from(request.encode_to_vec()))
                .await?;
            anyhow::ensure!(
                response.has_complete_chunk,
                "data node does not have the complete chunk"
            );
            anyhow::ensure!(
                attachments.len() == response.block_checksums.len(),
                "data node returned {} blocks but {} checksums",
                attachments.len(),
                response.block_checksums.len()
            );
            anyhow::ensure!(
                attachments.len() == selections.len(),
                "data node returned {} blocks for {} requested indexes",
                attachments.len(),
                selections.len()
            );
            attachments
                .into_iter()
                .zip(response.block_checksums)
                .enumerate()
                .map(|(index, (block, checksum))| {
                    let block = block.ok_or_else(|| {
                        anyhow::anyhow!("data node omitted requested block {index}")
                    })?;
                    anyhow::ensure!(
                        checksum == 0 || crc64(&block) == checksum,
                        "YTsaurus data-node block {index} checksum mismatch"
                    );
                    Ok(block)
                })
                .collect::<anyhow::Result<Vec<_>>>()
        }
        .await;
        match result {
            Ok(blocks) => return Ok(blocks),
            Err(error) => {
                clients.remove(&endpoint);
                failures.push(format!("{endpoint}: {error:#}"));
            }
        }
    }
    anyhow::bail!(
        "all direct YTsaurus data-node replicas failed to return a block group: {}",
        failures.join("; ")
    )
}

async fn data_node_client<'a>(
    clients: &'a mut HashMap<String, DataNodeRpcClient>,
    replica: &Replica,
    service_ticket: &watch::Receiver<Arc<str>>,
) -> anyhow::Result<&'a mut DataNodeRpcClient> {
    if !clients.contains_key(&replica.endpoint) {
        let client = DataNodeRpcClient::connect(&replica.endpoint, service_ticket.clone()).await?;
        clients.insert(replica.endpoint.clone(), client);
    }
    Ok(clients
        .get_mut(&replica.endpoint)
        .expect("data-node client was inserted"))
}

async fn fetch_chunk_meta(
    client: &mut DataNodeRpcClient,
    chunk_id: ProtoGuid,
    encoded_replica: u64,
    include_columns: bool,
) -> anyhow::Result<ChunkMeta> {
    let mut extension_tags = vec![EXTENSION_MISC, EXTENSION_DATA_BLOCK_META];
    if include_columns {
        extension_tags.push(EXTENSION_COLUMN_META);
    }
    let request = GetChunkMetaRequest {
        chunk_id: Some(chunk_id),
        medium_index: Some(medium_index(encoded_replica)),
        extension_tags,
        all_extension_tags: Some(false),
        workload_descriptor: Some(user_batch_workload()),
        enable_throttling: Some(true),
        replica_spec: Some(ChunkReplicaSpec {
            encoded_chunk_replica_with_medium: encoded_replica,
        }),
    };
    let (response, attachments): (GetChunkMetaResponse, _) = client
        .invoke("GetChunkMeta", Bytes::from(request.encode_to_vec()))
        .await?;
    anyhow::ensure!(attachments.is_empty(), "GetChunkMeta unexpectedly returned attachments");
    response
        .chunk_meta
        .ok_or_else(|| anyhow::anyhow!("data node returned no chunk metadata"))
}

#[derive(Clone, Copy)]
struct BlockSelection {
    block_index: i32,
    lower_row_index: i64,
    upper_row_index: i64,
}

fn selected_block_ranges(
    chunk: &ChunkSpec,
    meta: &DataBlockMetaExt,
) -> anyhow::Result<Vec<BlockSelection>> {
    validate_row_only_limit(chunk.lower_limit.as_ref(), "lower")?;
    validate_row_only_limit(chunk.upper_limit.as_ref(), "upper")?;
    let total_rows = meta
        .data_blocks
        .iter()
        .map(|block| block.chunk_row_count)
        .max()
        .unwrap_or(0);
    let absolute = chunk.row_index_is_absolute.unwrap_or(false);
    let table_row_index = chunk.table_row_index.unwrap_or(0);
    let relative = |value: i64| -> anyhow::Result<i64> {
        if absolute {
            value.checked_sub(table_row_index).ok_or_else(|| {
                anyhow::anyhow!("absolute YTsaurus row limit precedes chunk table row index")
            })
        } else {
            Ok(value)
        }
    };
    let lower = chunk
        .lower_limit
        .as_ref()
        .and_then(|limit| limit.row_index)
        .map(relative)
        .transpose()?
        .unwrap_or(0);
    let upper = chunk
        .upper_limit
        .as_ref()
        .and_then(|limit| limit.row_index)
        .map(relative)
        .transpose()?
        .or_else(|| chunk.row_count_override.and_then(|count| lower.checked_add(count)))
        .unwrap_or(total_rows);
    anyhow::ensure!(
        0 <= lower && lower <= upper && upper <= total_rows,
        "invalid YTsaurus chunk row range [{lower}, {upper}) for {total_rows} rows"
    );

    let mut selections = Vec::new();
    for block in &meta.data_blocks {
        let start = block
            .chunk_row_count
            .checked_sub(i64::from(block.row_count))
            .ok_or_else(|| anyhow::anyhow!("YTsaurus data block row range underflow"))?;
        if block.chunk_row_count > lower && start < upper {
            selections.push(BlockSelection {
                block_index: block.block_index,
                lower_row_index: start.max(lower),
                upper_row_index: block.chunk_row_count.min(upper),
            });
        }
    }
    selections.sort_unstable_by_key(|selection| {
        (selection.lower_row_index, selection.block_index)
    });
    selections.dedup_by_key(|selection| selection.block_index);
    let covered_until = selections.iter().try_fold(lower, |covered_until, selection| {
        anyhow::ensure!(
            selection.lower_row_index == covered_until,
            "YTsaurus data-block metadata has a gap or overlap at row {covered_until}"
        );
        Ok::<_, anyhow::Error>(selection.upper_row_index)
    })?;
    anyhow::ensure!(
        covered_until == upper,
        "YTsaurus data-block metadata covers rows [{lower}, {covered_until}), expected [{lower}, {upper})"
    );
    Ok(selections)
}

pub(super) fn validate_row_only_limit(
    limit: Option<&ReadLimit>,
    boundary: &str,
) -> anyhow::Result<()> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let mut unsupported = Vec::new();
    if limit.chunk_index.is_some() {
        unsupported.push("chunk_index");
    }
    if limit.offset.is_some() {
        unsupported.push("offset");
    }
    if limit.legacy_key.is_some() {
        unsupported.push("legacy_key");
    }
    if limit.key_index.is_some() {
        unsupported.push("key_index");
    }
    if limit.tablet_index.is_some() {
        unsupported.push("tablet_index");
    }
    if limit.key_bound_prefix.is_some() {
        unsupported.push("key_bound_prefix");
    }
    if limit.key_bound_is_inclusive.is_some() {
        unsupported.push("key_bound_is_inclusive");
    }
    anyhow::ensure!(
        unsupported.is_empty(),
        "direct YTsaurus data-node reader does not support {boundary} read-limit fields {}; refusing to ignore a sorted-table boundary",
        unsupported.join(", ")
    );
    Ok(())
}

#[derive(Clone)]
struct Replica {
    encoded: u64,
    endpoint: String,
}

fn chunk_replicas(chunk: &ChunkSpec, nodes: &HashMap<u32, String>) -> anyhow::Result<Vec<Replica>> {
    let encoded = chunk
        .replica_specs
        .iter()
        .map(|replica| replica.encoded_chunk_replica_with_medium)
        .chain(chunk.replicas.iter().copied());
    let mut seen = HashSet::new();
    let replicas = encoded
        .filter_map(|encoded| {
            let node_id = u32::try_from(encoded & NODE_ID_MASK).ok()?;
            let endpoint = nodes.get(&node_id)?.clone();
            seen.insert((node_id, medium_index(encoded))).then_some(Replica {
                encoded,
                endpoint,
            })
        })
        .collect::<Vec<_>>();
    Ok(replicas)
}

fn medium_index(encoded_replica: u64) -> i32 {
    ((encoded_replica >> MEDIUM_INDEX_SHIFT) & MEDIUM_INDEX_MASK) as i32
}

fn node_endpoints(directory: NodeDirectory) -> anyhow::Result<HashMap<u32, String>> {
    directory
        .items
        .into_iter()
        .map(|item| {
            let descriptor = item.node_descriptor.ok_or_else(|| {
                anyhow::anyhow!("YTsaurus node {} has no descriptor", item.node_id)
            })?;
            let addresses = descriptor.addresses.ok_or_else(|| {
                anyhow::anyhow!("YTsaurus node {} has no addresses", item.node_id)
            })?;
            let endpoint = addresses
                .entries
                .iter()
                .find(|entry| entry.network == "default")
                .or_else(|| addresses.entries.first())
                .map(|entry| entry.address.clone())
                .filter(|address| !address.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("YTsaurus node {} has no usable RPC address", item.node_id)
                })?;
            Ok((item.node_id, endpoint))
        })
        .collect()
}

fn user_batch_workload() -> WorkloadDescriptor {
    WorkloadDescriptor {
        category: WORKLOAD_CATEGORY_USER_BATCH,
        band: 0,
    }
}

pub(super) fn signature_payload(cookie: &[u8]) -> anyhow::Result<Bytes> {
    let mut reader = BinaryYsonMap::new(cookie);
    let mut payload = None;
    while let Some((key, value)) = reader.next_entry()? {
        if key == b"payload" {
            payload = Some(Bytes::from(value));
        }
    }
    payload.ok_or_else(|| anyhow::anyhow!("YTsaurus partition cookie signature has no payload"))
}

struct BinaryYsonMap<'a> {
    input: &'a [u8],
    position: usize,
    started: bool,
    finished: bool,
}

impl<'a> BinaryYsonMap<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            position: 0,
            started: false,
            finished: false,
        }
    }

    fn next_entry(&mut self) -> anyhow::Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.finished {
            return Ok(None);
        }
        if !self.started {
            self.expect(b'{')?;
            self.started = true;
        }
        self.skip_space();
        if self.peek() == Some(b'}') {
            self.position += 1;
            self.skip_space();
            anyhow::ensure!(self.position == self.input.len(), "trailing bytes after YSON map");
            self.finished = true;
            return Ok(None);
        }
        let key = self.string()?;
        self.expect(b'=')?;
        let value = self.string()?;
        self.expect(b';')?;
        Ok(Some((key, value)))
    }

    fn string(&mut self) -> anyhow::Result<Vec<u8>> {
        self.skip_space();
        match self.peek() {
            Some(YSON_BINARY_STRING) => {
                self.position += 1;
                let encoded = self.var_uint()?;
                anyhow::ensure!(encoded & 1 == 0, "negative YSON string length");
                let length = usize::try_from(encoded >> 1)?;
                let end = self
                    .position
                    .checked_add(length)
                    .ok_or_else(|| anyhow::anyhow!("YSON string length overflow"))?;
                anyhow::ensure!(end <= self.input.len(), "truncated binary YSON string");
                let value = self.input[self.position..end].to_vec();
                self.position = end;
                Ok(value)
            }
            Some(b'\"') => self.quoted_string(),
            other => anyhow::bail!("expected YSON string, found {other:?}"),
        }
    }

    fn quoted_string(&mut self) -> anyhow::Result<Vec<u8>> {
        self.position += 1;
        let mut value = Vec::new();
        while let Some(byte) = self.peek() {
            self.position += 1;
            match byte {
                b'\"' => return Ok(value),
                b'\\' => {
                    let escaped = self.peek().ok_or_else(|| anyhow::anyhow!("truncated YSON escape"))?;
                    self.position += 1;
                    value.push(match escaped {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'\\' => b'\\',
                        b'\"' => b'\"',
                        other => other,
                    });
                }
                other => value.push(other),
            }
        }
        anyhow::bail!("unterminated quoted YSON string")
    }

    fn var_uint(&mut self) -> anyhow::Result<u64> {
        let mut value = 0_u64;
        for shift in (0..=63).step_by(7) {
            let byte = self.peek().ok_or_else(|| anyhow::anyhow!("truncated YSON varint"))?;
            self.position += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        anyhow::bail!("YSON varint is too long")
    }

    fn expect(&mut self, expected: u8) -> anyhow::Result<()> {
        self.skip_space();
        let actual = self.peek();
        anyhow::ensure!(actual == Some(expected), "expected YSON byte {expected:#x}, found {actual:?}");
        self.position += 1;
        Ok(())
    }

    fn skip_space(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.position += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.position).copied()
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProtoGuid {
    #[prost(fixed64, required, tag = "1")]
    first: u64,

    #[prost(fixed64, required, tag = "2")]
    second: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct ReadLimit {
    #[prost(int64, optional, tag = "1")]
    pub(super) row_index: Option<i64>,

    #[prost(int32, optional, tag = "2")]
    pub(super) chunk_index: Option<i32>,

    #[prost(int64, optional, tag = "3")]
    pub(super) offset: Option<i64>,

    #[prost(bytes = "vec", optional, tag = "4")]
    pub(super) legacy_key: Option<Vec<u8>>,

    #[prost(int32, optional, tag = "5")]
    pub(super) key_index: Option<i32>,

    #[prost(int32, optional, tag = "6")]
    pub(super) tablet_index: Option<i32>,

    #[prost(bytes = "vec", optional, tag = "7")]
    pub(super) key_bound_prefix: Option<Vec<u8>>,

    #[prost(bool, optional, tag = "8")]
    pub(super) key_bound_is_inclusive: Option<bool>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ChunkReplicaSpec {
    #[prost(fixed64, required, tag = "1")]
    encoded_chunk_replica_with_medium: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ChunkSpec {
    #[prost(message, optional, tag = "1")]
    chunk_id: Option<ProtoGuid>,

    #[prost(message, optional, tag = "2")]
    lower_limit: Option<ReadLimit>,

    #[prost(message, optional, tag = "3")]
    upper_limit: Option<ReadLimit>,

    #[prost(fixed64, repeated, tag = "25")]
    replicas: Vec<u64>,

    #[prost(int32, optional, tag = "9")]
    erasure_codec: Option<i32>,

    #[prost(int64, optional, tag = "10")]
    table_row_index: Option<i64>,

    #[prost(message, optional, tag = "11")]
    chunk_meta: Option<ChunkMeta>,

    #[prost(int64, optional, tag = "14")]
    row_count_override: Option<i64>,

    #[prost(bool, optional, tag = "21")]
    row_index_is_absolute: Option<bool>,

    #[prost(bool, optional, tag = "24")]
    striped_erasure: Option<bool>,

    #[prost(bool, optional, tag = "26")]
    use_proxying_data_node_service: Option<bool>,

    #[prost(message, repeated, tag = "30")]
    replica_specs: Vec<ChunkReplicaSpec>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TableInputSpec {
    #[prost(message, repeated, tag = "2")]
    chunk_specs: Vec<ChunkSpec>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TablePartitionCookie {
    #[prost(message, repeated, tag = "2")]
    table_input_specs: Vec<TableInputSpec>,

    #[prost(string, optional, tag = "3")]
    user: Option<String>,

    #[prost(int32, optional, tag = "4")]
    partition_mode: Option<i32>,

    #[prost(message, optional, tag = "5")]
    node_directory: Option<NodeDirectory>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AddressEntry {
    #[prost(string, required, tag = "1")]
    network: String,

    #[prost(string, required, tag = "2")]
    address: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AddressMap {
    #[prost(message, repeated, tag = "3")]
    entries: Vec<AddressEntry>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct NodeDescriptor {
    #[prost(message, optional, tag = "1")]
    addresses: Option<AddressMap>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct NodeDirectoryItem {
    #[prost(uint32, required, tag = "1")]
    node_id: u32,

    #[prost(message, optional, tag = "2")]
    node_descriptor: Option<NodeDescriptor>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct NodeDirectory {
    #[prost(message, repeated, tag = "1")]
    items: Vec<NodeDirectoryItem>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WorkloadDescriptor {
    #[prost(int32, required, tag = "1")]
    category: i32,

    #[prost(int32, required, tag = "2")]
    band: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct GetBlockSetRequest {
    #[prost(message, optional, tag = "1")]
    chunk_id: Option<ProtoGuid>,

    #[prost(int32, repeated, tag = "2")]
    block_indexes: Vec<i32>,

    #[prost(bool, optional, tag = "5")]
    populate_cache: Option<bool>,

    #[prost(message, optional, tag = "6")]
    workload_descriptor: Option<WorkloadDescriptor>,

    #[prost(bool, optional, tag = "7")]
    fetch_from_cache: Option<bool>,

    #[prost(bool, optional, tag = "8")]
    fetch_from_disk: Option<bool>,

    #[prost(bool, optional, tag = "14")]
    enable_p2p: Option<bool>,

    #[prost(message, optional, tag = "16")]
    replica_spec: Option<ChunkReplicaSpec>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct GetBlockSetResponse {
    #[prost(bool, required, tag = "3")]
    has_complete_chunk: bool,

    #[prost(fixed64, repeated, tag = "9")]
    block_checksums: Vec<u64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct GetChunkMetaRequest {
    #[prost(message, optional, tag = "1")]
    chunk_id: Option<ProtoGuid>,

    #[prost(int32, optional, tag = "7")]
    medium_index: Option<i32>,

    #[prost(int32, repeated, tag = "2")]
    extension_tags: Vec<i32>,

    #[prost(bool, optional, tag = "3")]
    all_extension_tags: Option<bool>,

    #[prost(message, optional, tag = "5")]
    workload_descriptor: Option<WorkloadDescriptor>,

    #[prost(bool, optional, tag = "6")]
    enable_throttling: Option<bool>,

    #[prost(message, optional, tag = "10")]
    replica_spec: Option<ChunkReplicaSpec>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct GetChunkMetaResponse {
    #[prost(message, optional, tag = "1")]
    chunk_meta: Option<ChunkMeta>,
}
