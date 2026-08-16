use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;

use super::provider::DiscoveredTable;
use crate::delivery::data::message::SourceBatch;
use crate::delivery::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use crate::delivery::data::table_data::TableData;
use crate::delivery::execution::source::{CommitMarker, Source};
use crate::metrics::SourceCounters;

pub(super) struct ClickHouseSource {
    table: DiscoveredTable,
    partition_id: i64,
    stream: crate::providers::clickhouse::sink::client::QueryStream,
    pending: Option<(RecordBatch, usize)>,
    batch_rows: usize,
    request_timeout: Duration,
    offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
}

impl ClickHouseSource {
    pub(super) const fn new(
        table: DiscoveredTable,
        partition_id: i64,
        stream: crate::providers::clickhouse::sink::client::QueryStream,
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
        validate_snapshot_schema(batch, &self.table)?;
        let rows = batch.num_rows();
        let rows_i64 = i64::try_from(rows)?;
        let base = batch.num_columns();
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut arrays = batch.columns().to_vec();
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
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        self.offset = self
            .offset
            .checked_add(rows_i64)
            .ok_or_else(|| anyhow::anyhow!("ClickHouse source offset overflow"))?;
        self.counters.add_messages(rows as u64);
        self.counters
            .add_decompressed_bytes(batch.get_array_memory_size() as u64);
        Ok(SourceBatch::Typed {
            tables: vec![TableData::new(
                Arc::from(self.table.config.output_name.as_str()),
                false,
                batch,
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
                ]),
            )],
            source_rows: rows as u64,
            commit_marker: Some(CommitMarker::new(self.offset)),
            memory: Vec::new(),
        })
    }
}

impl Source for ClickHouseSource {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<SourceBatch>> {
        Box::pin(async move {
            loop {
                if let Some((batch, offset)) = self.pending.take() {
                    let rows = self.batch_rows.min(batch.num_rows() - offset);
                    let slice = batch.slice(offset, rows);
                    if offset + rows < batch.num_rows() {
                        self.pending = Some((batch, offset + rows));
                    }
                    return self.output(&slice);
                }
                if self.finished {
                    return Ok(SourceBatch::Finished);
                }
                let next = tokio::time::timeout(self.request_timeout, self.stream.next())
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!(
                            "ClickHouse snapshot response timed out after {} ms",
                            self.request_timeout.as_millis()
                        )
                    })?;
                match next {
                    Some(Ok(batch)) if batch.num_rows() > 0 => {
                        validate_snapshot_schema(&batch, &self.table)?;
                        self.pending = Some((batch, 0));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        return Err(anyhow::anyhow!(
                            "ClickHouse snapshot response failed: {error}"
                        ))
                    }
                    None => self.finished = true,
                }
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, anyhow::Result<()>> {
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
