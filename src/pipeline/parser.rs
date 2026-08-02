use std::sync::Arc;

use crate::types::arrow_batch::ArrowBatch;
use crate::types::message::Message;

/// The Parser trait transforms a batch of messages into Arrow record batches.
///
/// Implementations parse the message values (e.g., JSON), extract fields using
/// JSONPath, and produce typed Arrow arrays. Invalid rows are routed to a
/// dead-letter queue batch.
///
/// This trait is **synchronous** — implementations are CPU-bound (parsing,
/// Arrow building). No heap-allocated future per call.
pub trait Parser: Send + Sync {
    /// Transform a batch of messages into Arrow batches.
    ///
    /// Returns `(valid_batch, optional_dlq_batch)`:
    /// - `valid_batch` contains the successfully parsed rows.
    /// - `dlq_batch` contains rows that failed to parse, if any.
    ///   `None` means all rows were valid.
    fn parse(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
    ) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)>;
}

// ---------------------------------------------------------------------------
// Blanket impls
// ---------------------------------------------------------------------------

impl<T: Parser + ?Sized> Parser for &T {
    fn parse(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
    ) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)> {
        (**self).parse(messages, partition_id)
    }
}

impl<T: Parser + Send + Sync + ?Sized> Parser for Box<T> {
    fn parse(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
    ) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)> {
        (**self).parse(messages, partition_id)
    }
}

impl<T: Parser + ?Sized> Parser for Arc<T> {
    fn parse(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
    ) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)> {
        (**self).parse(messages, partition_id)
    }
}
