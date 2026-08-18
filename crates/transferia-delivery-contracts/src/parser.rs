use std::sync::Arc;

use transferia_core::data::message::Message;
use transferia_core::data::table_data::TableData;

/// Compiled parser shared by partition pipelines.
pub trait ParserFactory: Send + Sync {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession>;
}

/// Mutable parser state owned by exactly one partition pipeline.
pub trait ParserSession: Send {
    /// Conservative allocation estimate used for admission before builders allocate.
    fn output_memory_bound(&self, messages: &[Message]) -> usize;

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)>;
}
