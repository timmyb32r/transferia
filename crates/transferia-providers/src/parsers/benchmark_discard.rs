//! Benchmark-only parser that intentionally discards every source message.
//!
//! This component may only be paired with the discard sink. Compatibility
//! validation rejects it for durable `ClickHouse` and `S3` pipelines.

use alloc::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use serde::Deserialize;

use crate::parsers::{ParserFactory, ParserSession};
use transferia_core::data::message::Message;
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::data::table_data::TableData;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BenchmarkDiscardConfig {}

pub struct BenchmarkDiscardParser {
    table: Arc<str>,
}

impl BenchmarkDiscardParser {
    #[must_use]
    pub const fn new(table: Arc<str>) -> Self {
        Self { table }
    }
}

struct BenchmarkDiscardSession {
    table: Arc<str>,
}

impl ParserSession for BenchmarkDiscardSession {
    fn output_memory_bound(&self, _messages: &[Message]) -> usize {
        1
    }

    fn parse_into(
        &mut self,
        _messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        Ok((
            TableData {
                table: Arc::clone(&self.table),
                is_dlq: false,
                batch,
                system_columns: SystemColumns::default(),
            },
            None,
        ))
    }
}

impl ParserFactory for BenchmarkDiscardParser {
    fn create_session(self: Arc<Self>, _memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(BenchmarkDiscardSession {
            table: Arc::clone(&self.table),
        })
    }
}

#[cfg(test)]
#[path = "tests/benchmark_discard.rs"]
mod tests;
