use std::sync::Arc;

use crate::core::data::message::Message;
use crate::core::data::table_data::TableData;
use crate::parsers::{ParserFactory, ParserSession};

pub(super) struct NativeSourceParser;

impl ParserFactory for NativeSourceParser {
    fn create_session(self: Arc<Self>) -> Box<dyn ParserSession> {
        Box::new(Self)
    }
}

impl ParserSession for NativeSourceParser {
    fn output_memory_bound(&self, _messages: &[Message]) -> usize {
        0
    }

    fn hard_output_limit(&self) -> Option<usize> {
        None
    }

    fn parse_into(
        &mut self,
        _messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        anyhow::bail!("native typed source produced a raw message batch")
    }
}
