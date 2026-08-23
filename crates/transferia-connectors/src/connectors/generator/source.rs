use std::sync::Arc;

use futures_util::future::BoxFuture;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::data::table_data::TableData;
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::memory::PipelineMemory;
use transferia_core::source::{CommitMarker, Source};
use transferia_delivery_contracts::metrics::SourceCounters;

use super::connector::DataGeneratorConfig;

const GENERATED_BATCH_TARGET_BYTES: u64 = 16 * 1024 * 1024;

pub(super) struct DataGeneratorSource {
    config: DataGeneratorConfig,
    memory: PipelineMemory,
    counters: Arc<SourceCounters>,
    next_row: u64,
    total_rows: u64,
    rows_per_batch: u64,
}

impl DataGeneratorSource {
    pub(super) fn new(
        config: DataGeneratorConfig,
        memory: PipelineMemory,
        counters: Arc<SourceCounters>,
    ) -> anyhow::Result<Self> {
        let row_bytes = config.row_bytes()?;
        let total_rows = config.data_size_bytes / row_bytes;
        let memory_limit = u64::try_from(memory.limit())?;
        let batch_target_bytes = memory_limit.min(GENERATED_BATCH_TARGET_BYTES);
        let rows_per_batch = (batch_target_bytes / row_bytes).max(1);
        Ok(Self {
            config,
            memory,
            counters,
            next_row: 0,
            total_rows,
            rows_per_batch,
        })
    }
}

impl Source for DataGeneratorSource {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            if self.next_row == self.total_rows {
                return Ok(SourceBatch::Finished);
            }
            let remaining = self.total_rows - self.next_row;
            let rows = remaining.min(self.rows_per_batch);
            let batch_bytes_u64 = rows
                .checked_mul(self.config.row_bytes().map_err(DataPlaneFailure::fatal)?)
                .ok_or_else(|| {
                    DataPlaneFailure::fatal(anyhow::anyhow!("generator batch size overflow"))
                })?;
            let batch_bytes = usize::try_from(batch_bytes_u64)
                .map_err(|error| DataPlaneFailure::fatal(error.into()))?;
            let reservation = self.memory.reserve(batch_bytes).await;
            let start = self.next_row;
            let batch = self
                .config
                .preset
                .batch(start, rows)
                .map_err(DataPlaneFailure::fatal)?;
            self.next_row += rows;
            self.counters.add_messages(rows);
            self.counters.add_decompressed_bytes(batch_bytes_u64);
            Ok(SourceBatch::Typed {
                tables: vec![TableData::new(
                    Arc::from(self.config.table_name.as_str()),
                    false,
                    batch,
                    SystemColumns::default(),
                )],
                source_rows: rows,
                commit_marker: Some(CommitMarker::new(self.next_row)),
                memory: vec![reservation],
            })
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        _markers: &'ctx [CommitMarker],
    ) -> BoxFuture<'ctx, DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
}
