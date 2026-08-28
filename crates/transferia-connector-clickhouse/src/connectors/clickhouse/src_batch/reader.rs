use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::Stream;
use futures_util::StreamExt as _;
use std::pin::Pin;

use super::connector::DiscoveredTable;
use crate::metrics::SourceCounters;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::source::{CommitMarker, Source};

pub(super) struct ClickHouseSource {
    table: DiscoveredTable,
    partition_id: i64,
    stream: SnapshotStream,
    pending: Option<(RecordBatch, usize)>,
    batch_rows: usize,
    request_timeout: Duration,
    offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
}

pub(super) type SnapshotStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<RecordBatch>> + Send + 'static>>;

impl ClickHouseSource {
    pub(super) const fn new(
        table: DiscoveredTable,
        partition_id: i64,
        stream: SnapshotStream,
        batch_rows: usize,
        request_timeout: Duration,
        counters: Arc<SourceCounters>,
    ) -> Self {
        Self {
            table,
            partition_id,
            stream,
            pending: None,
            batch_rows,
            request_timeout,
            offset: 0,
            finished: false,
            counters,
        }
    }

    fn output(&mut self, batch: &RecordBatch) -> anyhow::Result<SourceBatch> {
        let batch = normalize_snapshot_schema(batch, &self.table)?;
        validate_snapshot_schema(&batch, &self.table)?;
        let rows = batch.num_rows();
        let rows_i64 = i64::try_from(rows)?;
        let base = batch.num_columns();
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut arrays = batch.columns().to_vec();
        let system_columns = if self.table.physical_system_columns.is_empty() {
            fields.extend([
                Arc::new(Field::new(
                    SystemColumnKind::Topic.default_name(),
                    DataType::Utf8,
                    false,
                )),
                Arc::new(Field::new(
                    SystemColumnKind::Partition.default_name(),
                    DataType::Int64,
                    false,
                )),
                Arc::new(Field::new(
                    SystemColumnKind::Offset.default_name(),
                    DataType::Int64,
                    false,
                )),
                Arc::new(Field::new(
                    SystemColumnKind::MessageIndex.default_name(),
                    DataType::UInt64,
                    false,
                )),
            ]);
            arrays.extend([
                Arc::new(StringArray::from(vec![
                    format!(
                        "{}.{}",
                        self.table.config.database, self.table.config.name
                    );
                    rows
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![self.partition_id; rows])) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(
                    self.offset
                        ..self
                            .offset
                            .checked_add(rows_i64)
                            .ok_or_else(|| anyhow::anyhow!("ClickHouse source offset overflow"))?,
                )) as ArrayRef,
                Arc::new(UInt64Array::from(vec![0_u64; rows])) as ArrayRef,
            ]);
            SystemColumns::new(vec![
                SystemColumn {
                    kind: SystemColumnKind::Topic,
                    name: Arc::from(SystemColumnKind::Topic.default_name()),
                    index: base,
                },
                SystemColumn {
                    kind: SystemColumnKind::Partition,
                    name: Arc::from(SystemColumnKind::Partition.default_name()),
                    index: base + 1,
                },
                SystemColumn {
                    kind: SystemColumnKind::Offset,
                    name: Arc::from(SystemColumnKind::Offset.default_name()),
                    index: base + 2,
                },
                SystemColumn {
                    kind: SystemColumnKind::MessageIndex,
                    name: Arc::from(SystemColumnKind::MessageIndex.default_name()),
                    index: base + 3,
                },
            ])
        } else {
            self.table.physical_system_columns.clone()
        };
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        self.offset = self
            .offset
            .checked_add(rows_i64)
            .ok_or_else(|| anyhow::anyhow!("ClickHouse source offset overflow"))?;
        self.counters.add_records(rows as u64);
        Ok(SourceBatch::Typed {
            tables: vec![TableData::new(
                Arc::from(self.table.config.name.as_str()),
                false,
                batch,
                system_columns,
            )],
            source_rows: rows as u64,
            commit_marker: Some(CommitMarker::new(self.offset)),
            memory: Vec::new(),
        })
    }
}

impl Source for ClickHouseSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            loop {
                if let Some((batch, offset)) = self.pending.take() {
                    let take = self.batch_rows.min(batch.num_rows() - offset);
                    let output = batch.slice(offset, take);
                    if offset + take < batch.num_rows() {
                        self.pending = Some((batch, offset + take));
                    }
                    return self.output(&output).map_err(DataPlaneFailure::fatal);
                }
                if self.finished {
                    tracing::info!(
                        table = %format!("{}.{}", self.table.config.database, self.table.config.name),
                        emitted_rows = self.offset,
                        "ClickHouse snapshot source completed"
                    );
                    return Ok(SourceBatch::Finished);
                }
                let next = tokio::time::timeout(self.request_timeout, self.stream.next())
                    .await
                    .map_err(|_| {
                        DataPlaneFailure::retryable(anyhow::anyhow!(
                            "ClickHouse snapshot response timed out after {} ms",
                            self.request_timeout.as_millis()
                        ))
                    })?;
                match next {
                    Some(Ok(batch)) if batch.num_rows() > 0 => {
                        // This is the payload after transport decompression and
                        // ClickHouse-to-Arrow decoding, before Transferia adds synthetic
                        // system columns. Transports that expose compressed response
                        // sizes account for network-raw at their own boundary.
                        self.counters.add_network_decoded_bytes(
                            u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
                        );
                        self.pending = Some((batch, 0));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                            "ClickHouse snapshot response failed: {error}"
                        )))
                    }
                    None => self.finished = true,
                }
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn validate_snapshot_schema(batch: &RecordBatch, table: &DiscoveredTable) -> anyhow::Result<()> {
    anyhow::ensure!(
        batch.num_columns() == table.schema.columns.len(),
        "ClickHouse snapshot query for '{}.{}' returned {} columns, discovery declared {}",
        table.config.database,
        table.config.name,
        batch.num_columns(),
        table.schema.columns.len()
    );
    for (actual, expected) in batch.schema().fields().iter().zip(&table.schema.columns) {
        anyhow::ensure!(actual.name() == &expected.name && actual.data_type() == &expected.data_type && actual.is_nullable() == expected.nullable, "ClickHouse snapshot schema drifted at '{}.{}': discovered '{} {:?} nullable={}', query returned '{} {:?} nullable={}'", table.config.database, table.config.name, expected.name, expected.data_type, expected.nullable, actual.name(), actual.data_type(), actual.is_nullable());
    }
    Ok(())
}

pub(super) fn normalize_snapshot_schema(
    batch: &RecordBatch,
    table: &DiscoveredTable,
) -> anyhow::Result<RecordBatch> {
    if table.physical_system_columns.is_empty() {
        return Ok(batch.clone());
    }
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    let mut arrays = batch.columns().to_vec();
    for system_column in table.physical_system_columns.iter() {
        let expected = system_column.kind.data_type();
        if arrays[system_column.index].data_type() != &expected {
            arrays[system_column.index] = cast(&arrays[system_column.index], &expected).map_err(
                |error| {
                    anyhow::anyhow!(
                        "ClickHouse source system column '{}' cannot be decoded as {expected:?}: {error}",
                        system_column.name,
                    )
                },
            )?;
        }
        fields[system_column.index] =
            Arc::new(Field::new(system_column.name.as_ref(), expected, false));
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}
