//! Export of process logs to an external destination.
//!
//! [`start`] turns a [`LogExportSetup`] into a `tracing` layer and a handle. The layer turns every event
//! into a [`LogRecord`], encodes it with the configured [`LogRecordEncoder`] on
//! the emitting thread and queues the encoded record. A background worker
//! batches queued records and hands them to the configured [`LogSink`].
//!
//! Contract:
//!
//! - The export adds no waiting to a logging call: an event is formatted and
//!   encoded on the caller's thread, as the console layer does, and queued
//!   without I/O, locks or awaiting. A record that does not fit the limits of
//!   [`LogExportConfig`] is dropped and counted; the next successful write, or
//!   the shutdown, is followed by one record that reports the counts.
//! - A batch rejected by the sink is retried with exponential backoff, at most
//!   `max_write_attempts` times, and then dropped and counted. Records reach
//!   the sink in emission order, and the sink may see a batch more than once.
//! - Events emitted by the worker itself, including everything the sink logs
//!   while writing, stay out of the export; they reach only the other layers.
//!   A sink keeps the tasks it spawns out as well with
//!   [`spawn_inheriting_scope`].
//! - Span fields are not exported.
//! - Records still queued when the process aborts are lost.

mod config;
mod layer;
mod record;
mod worker;

pub use config::{LogExportConfig, RawLogExportConfig};
pub use layer::LogExportLayer;
pub use record::{LogField, LogRecord, LogRecordEncoder, LogSink, LogValue};
pub use worker::{
    spawn_inheriting_scope, start, LogExport, LogExportHandle, LogExportSetup, LogExportStats,
};

#[cfg(test)]
mod tests;
