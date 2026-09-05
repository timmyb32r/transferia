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
    end_row: Option<u64>,
    batch_target_bytes: u64,
}

impl DataGeneratorSource {
    pub(super) fn new(
        config: DataGeneratorConfig,
        memory: PipelineMemory,
        counters: Arc<SourceCounters>,
    ) -> anyhow::Result<Self> {
        let total_rows = config.total_rows()?;
        let next_row = config.start_row;
        let end_row = total_rows.map(|rows| next_row.checked_add(rows)
            .ok_or_else(|| anyhow::anyhow!("generator row range overflows u64"))).transpose()?;
        let memory_limit = u64::try_from(memory.limit())?;
        let batch_target_bytes = memory_limit.min(GENERATED_BATCH_TARGET_BYTES);
        Ok(Self {
            config,
            memory,
            counters,
            next_row,
            end_row,
            batch_target_bytes,
        })
    }
}

impl Source for DataGeneratorSource {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            if Some(self.next_row) == self.end_row {
                return Ok(SourceBatch::Finished);
            }
            let remaining = self.end_row.unwrap_or(u64::MAX) - self.next_row;
            if remaining == 0 {
                return Err(DataPlaneFailure::fatal(anyhow::anyhow!("generator row identifiers exhausted u64")));
            }
            let rows = self
                .config
                .rows_for_batch(self.next_row, remaining, self.batch_target_bytes)
                .map_err(DataPlaneFailure::fatal)?;
            let batch_bytes_u64 = self
                .config
                .batch_bytes(self.next_row, rows)
                .map_err(DataPlaneFailure::fatal)?;
            self.config.preset.validate_range(self.next_row, rows)
                .map_err(DataPlaneFailure::fatal)?;
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
            self.counters.add_records(rows);
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
