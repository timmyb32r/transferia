use std::sync::Arc;

use crate::parsers::{ParserFactory, ParserSession};
use transferia_core::data::message::Message;
use transferia_core::data::table_data::TableData;

pub(super) struct NativeSourceParser;

impl ParserFactory for NativeSourceParser {
    fn create_session(self: Arc<Self>, _memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(Self)
    }
}

impl ParserSession for NativeSourceParser {
    fn output_memory_bound(&self, _messages: &[Message]) -> usize {
        0
    }

    fn parse_into(
        &mut self,
        _messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        anyhow::bail!("native typed source produced a raw message batch")
    }
}
