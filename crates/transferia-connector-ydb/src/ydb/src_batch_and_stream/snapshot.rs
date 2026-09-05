use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow::array::{new_null_array, ArrayRef, BinaryArray, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{
    META_OLD_VALUE_OF, META_SYSTEM_ROLE, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_TABLE,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TRANSACTION_ID, SYSTEM_ROLE_SOURCE_VERSION,
};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::memory::{MemoryReservation, PipelineMemory};
use transferia_core::source::{CommitMarker, Source};
use transferia_core::ChangeOperation;

use super::super::config::YdbSourceConfig;
use super::super::source::{DiscoveredTable, YdbSource};
use super::super::src_stream::{build_table_schema, schema_materialization_admission_bytes};
use super::super::transport::YdbClient;
use super::state::OverlapExecution;
use crate::metrics::SourceCounters;

pub(in crate::ydb) struct OverlapSnapshot {
    config: YdbSourceConfig,
    execution: Arc<OverlapExecution>,
    cancellation: CancellationToken,
    counters: Arc<SourceCounters>,
    table_index: usize,
    reader: Option<YdbSource>,
    previous_key: Option<Vec<u8>>,
    memory: PipelineMemory,
    key_memory: Option<MemoryReservation>,
}

impl OverlapSnapshot {
    pub(in crate::ydb) fn new(
        config: YdbSourceConfig,
        execution: Arc<OverlapExecution>,
        cancellation: CancellationToken,
        counters: Arc<SourceCounters>,
        memory: PipelineMemory,
    ) -> anyhow::Result<Self> {
        execution.claim_snapshot()?;
        Ok(Self {
            config,
            execution,
            cancellation,
            counters,
            table_index: 0,
            reader: None,
            previous_key: None,
            memory,
            key_memory: None,
        })
    }

    async fn next(&mut self) -> anyhow::Result<SourceBatch> {
        loop {
            self.execution.check_fence()?;
            let Some(table) = self
                .execution
                .prepared
                .resources
                .tables
                .get(self.table_index)
            else {
                self.execution
                    .snapshot_finished
                    .store(true, Ordering::Release);
                return Ok(SourceBatch::Finished);
            };
            if self.reader.is_none() {
                self.reader = Some(
                    YdbSource::new(
                        transferia_connector_support::external_request::observe_external_request(
                            "ydb",
                            "overlap_snapshot_connect",
                            YdbClient::connect(&self.config.connection),
                        )
                        .await?,
                        table.clone(),
                        i64::try_from(self.table_index)?,
                        self.config.batch_rows,
                        self.config.session_shutdown_timeout(),
                        self.config.session_shutdown_retry_initial(),
                        Arc::clone(&self.counters),
                        true,
                    )
                    .await?,
                );
            }
            let reader = self
                .reader
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("YDB snapshot reader missing"))?;
            let reservation = self
                .memory
                .reserve_progress_source(self.config.connection.max_rpc_message_bytes)
                .await;
            match reader.read_batch().await? {
                SourceBatch::Finished => {
                    reader.shutdown().await?;
                    self.reader = None;
                    self.previous_key = None;
                    self.key_memory = None;
                    self.table_index += 1;
                }
                SourceBatch::Typed {
                    tables,
                    source_rows,
                    commit_marker,
                    mut memory,
                } => {
                    let schema_bytes = schema_materialization_admission_bytes(table)?;
                    let admission = tables
                        .iter()
                        .try_fold(schema_bytes, |bytes, batch| {
                            bytes
                                .checked_add(batch.batch.get_array_memory_size())
                                .and_then(|bytes| {
                                    bytes.checked_add(batch.batch.num_rows().checked_mul(256)?)
                                })
                                .ok_or_else(|| {
                                    anyhow::anyhow!("YDB snapshot memory accounting overflow")
                                })
                        })?
                        .checked_mul(8)
                        .ok_or_else(|| {
                            anyhow::anyhow!("YDB snapshot materialization admission overflow")
                        })?;
                    reservation.grow_progress_source_to(admission)?;
                    let table = table.clone();
                    let database = self.config.connection.database.clone();
                    let mut previous = self.previous_key.take();
                    let retained_reservation = reservation.clone();
                    let (tables, previous) = tokio::task::spawn_blocking(move || {
                        let _reservation = retained_reservation;
                        let tables = tables
                            .into_iter()
                            .map(|batch| {
                                validate_snapshot_keys(&table, &batch.batch, &mut previous)?;
                                materialize_snapshot(&table, &database, batch)
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?;
                        Ok::<_, anyhow::Error>((tables, previous))
                    })
                    .await??;
                    self.key_memory = Some(
                        reservation
                            .reserve_source_companion(previous.as_ref().map_or(0, Vec::capacity))?,
                    );
                    self.previous_key = previous;
                    let output_bytes = tables.iter().try_fold(0usize, |bytes, table| {
                        bytes
                            .checked_add(table.batch.get_array_memory_size())
                            .and_then(|bytes| bytes.checked_add(schema_bytes))
                            .ok_or_else(|| {
                                anyhow::anyhow!("YDB snapshot output accounting overflow")
                            })
                    })?;
                    memory.push(reservation.reserve_source_companion(output_bytes)?);
                    return Ok(SourceBatch::Typed {
                        tables,
                        source_rows,
                        commit_marker,
                        memory,
                    });
                }
                SourceBatch::Raw { .. } => anyhow::bail!("YDB snapshot returned an unexpected untyped batch"),
            }
        }
    }
}

impl Source for OverlapSnapshot {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            let cancellation = self.cancellation.clone();
            let fence = self.execution.prepared.fence_lost.clone();
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(DataPlaneFailure::fatal(anyhow::anyhow!("YDB overlapping snapshot cancelled; manual recovery required"))),
                () = fence.cancelled() => Err(DataPlaneFailure::fatal(anyhow::anyhow!("YDB overlapping snapshot fence lost; manual recovery required"))),
                result = self.next() => result.map_err(|error| DataPlaneFailure::fatal(error.context("YDB overlapping snapshot failed; manual recovery with a clean destination is required"))),
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, DataPlaneResult<()>> {
        Box::pin(async move {
            self.execution
                .check_fence()
                .map_err(DataPlaneFailure::fatal)
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, DataPlaneResult<()>> {
        Box::pin(async move {
            if let Some(reader) = self.reader.as_mut() {
                reader.shutdown().await?;
            }
            Ok(())
        })
    }
}

fn validate_snapshot_keys(
    table: &DiscoveredTable,
    batch: &RecordBatch,
    previous: &mut Option<Vec<u8>>,
) -> anyhow::Result<()> {
    let mut keys = table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key_ordinal.map(|ordinal| (ordinal, index)))
        .collect::<Vec<_>>();
    keys.sort_unstable();
    anyhow::ensure!(
        !keys.is_empty(),
        "YDB overlap snapshot requires a primary key"
    );
    let arrays = keys
        .iter()
        .map(|(_, index)| Arc::clone(batch.column(*index)))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        arrays.iter().all(|array| array.null_count() == 0),
        "YDB overlap snapshot has a null primary key"
    );
    let converter = RowConverter::new(
        arrays
            .iter()
            .map(|array| SortField::new(array.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(&arrays)?;
    for (index, row) in rows.iter().enumerate() {
        // ReadTable is ordered by the complete primary key. Equal keys are
        // therefore adjacent, including across response boundaries.
        let ordered = if index == 0 {
            previous.as_deref().is_none_or(|key| key < row.as_ref())
        } else {
            rows.row(index - 1).as_ref() < row.as_ref()
        };
        anyhow::ensure!(
            ordered,
            "YDB snapshot contains a duplicate or out-of-order primary key in table '{}'",
            table.config.path
        );
    }
    if let Some(row) = rows.iter().next_back() {
        *previous = Some(row.as_ref().to_vec());
    }
    Ok(())
}

fn materialize_snapshot(
    table: &DiscoveredTable,
    database: &str,
    input: TableData,
) -> anyhow::Result<TableData> {
    let schema = build_table_schema(table)?;
    let len = input.batch.num_rows();
    let mut arrays = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    let mut systems = Vec::new();
    let mut mask = vec![0_u8; table.columns.len().div_ceil(8)];
    for index in 0..table.columns.len() {
        mask[index / 8] |= 1 << (index % 8);
    }
    for (index, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = if index < table.columns.len() {
            Arc::clone(input.batch.column(index))
        } else if field.metadata().contains_key(META_OLD_VALUE_OF) {
            new_null_array(field.data_type(), len)
        } else if let Some(role) = field.metadata().get(META_SYSTEM_ROLE) {
            match role.as_str() {
                SYSTEM_ROLE_SOURCE_VERSION => Arc::new(UInt64Array::from(vec![0; len])),
                SYSTEM_ROLE_SOURCE_DATABASE => Arc::new(StringArray::from(vec![database; len])),
                SYSTEM_ROLE_SOURCE_TABLE => {
                    Arc::new(StringArray::from(vec![table.config.path.as_str(); len]))
                }
                SYSTEM_ROLE_SOURCE_TRANSACTION_ID | SYSTEM_ROLE_SOURCE_TIMESTAMP_MS => {
                    new_null_array(field.data_type(), len)
                }
                _ => anyhow::bail!("unsupported YDB snapshot metadata role"),
            }
        } else {
            let kind = [
                SystemColumnKind::Topic,
                SystemColumnKind::Partition,
                SystemColumnKind::Offset,
                SystemColumnKind::MessageIndex,
                SystemColumnKind::WriteTimestampMs,
                SystemColumnKind::ChangeOperation,
                SystemColumnKind::ChangedColumns,
            ]
            .into_iter()
            .find(|kind| kind.default_name() == field.name())
            .ok_or_else(|| anyhow::anyhow!("unknown YDB snapshot system column"))?;
            systems.push(SystemColumn {
                kind,
                index,
                name: Arc::from(field.name().as_str()),
            });
            match kind {
                SystemColumnKind::ChangeOperation => {
                    Arc::new(StringArray::from(vec![
                        ChangeOperation::SnapshotRead.code();
                        len
                    ]))
                }
                SystemColumnKind::ChangedColumns => Arc::new(BinaryArray::from_iter_values(
                    std::iter::repeat_n(mask.as_slice(), len),
                )),
                SystemColumnKind::WriteTimestampMs => new_null_array(field.data_type(), len),
                _ => Arc::clone(
                    input.batch.column(
                        input
                            .system_columns
                            .get(kind)
                            .ok_or_else(|| anyhow::anyhow!("YDB snapshot routing column missing"))?
                            .index,
                    ),
                ),
            }
        };
        arrays.push(array);
    }
    Ok(TableData::new(
        input.table,
        false,
        RecordBatch::try_new(schema, arrays)?,
        SystemColumns::new(systems),
    ))
}

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
