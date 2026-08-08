//! "none" parser — drops every message at the parse stage without parsing.
//!
//! Used to measure pure source/download throughput: the source reads and hands
//! messages to the pipeline, the parser stage discards them (0-row output), and
//! the writer's marker-only ack path still commits offsets so the consumer
//! advances. No DLQ, no key columns, no schema.

use alloc::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::parsers::Parser;
use crate::parsers::ParserWorkspace;
use crate::types::exactly_once::ExactlyOnceKey;
use crate::types::message::Message;
use crate::types::table_data::TableData;

/// "none" parser: produces a 0-row valid batch (empty schema) and no DLQ for
/// every `parse_into` call, discarding all input messages.
pub struct NoneParser {
    table: Arc<str>,
}

impl NoneParser {
    #[must_use]
    pub const fn new(table: Arc<str>) -> Self { Self { table } }
}

impl Parser for NoneParser {
    fn parse_into(
        &self,
        _messages: Vec<Message>,
        _partition_id: i64,
        _exactly_once_key: Option<ExactlyOnceKey>,
        _ws: &mut ParserWorkspace,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        // Empty-schema, 0-row batch — the accumulator skips 0-row batches, and
        // the writer's marker-only ack forwards the commit marker so offsets
        // advance even though no data is written or parsed.
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        Ok((
            TableData {
                table: Arc::clone(&self.table),
                is_dlq: false,
                batch,
                batch_id: crate::batch_id(),
                exactly_once_key: None,
            },
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::ParserWorkspace;
    use crate::types::message::Message;
    use bytes::Bytes;

    #[test]
    fn none_parser_drops_all() -> anyhow::Result<()> {
        let p = NoneParser::new("logs".into());
        let mut ws = ParserWorkspace::new();
        let msgs = vec![
            Message { value: Bytes::from_static(b"{\"id\":\"a\"}"), offset: Some(1), partition: None },
            Message { value: Bytes::from_static(b"{\"id\":\"b\"}"), offset: Some(2), partition: None },
        ];
        let (valid, dlq) = p.parse_into(msgs, 0, None, &mut ws)?;
        assert_eq!(valid.batch.num_rows(), 0, "none parser must produce 0 rows");
        assert!(!valid.is_dlq, "valid batch is not DLQ");
        assert!(dlq.is_none(), "none parser must not produce a DLQ batch");
        assert_eq!(valid.table.as_ref(), "logs");
        assert!(valid.exactly_once_key.is_none());
        Ok(())
    }
}
