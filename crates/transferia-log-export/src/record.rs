use std::time::SystemTime;

use futures_util::future::BoxFuture;

/// One structured value attached to a log event.
#[derive(Clone, Debug, PartialEq)]
pub enum LogValue {
    Str(String),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogField {
    pub name: &'static str,

    pub value: LogValue,
}

/// Destination-neutral view of one log event, valid for one `encode` call.
#[derive(Clone, Copy, Debug)]
pub struct LogRecord<'a> {
    pub timestamp: SystemTime,

    pub level: tracing::Level,

    pub target: &'a str,

    pub file: Option<&'a str>,

    pub line: Option<u32>,

    pub message: &'a str,

    /// Event fields in declaration order, without `message`.
    pub fields: &'a [LogField],
}

/// Wire format of the destination.
pub trait LogRecordEncoder: Send + Sync + 'static {
    /// Appends exactly one non-empty wire record to `output`.
    ///
    /// Runs on the thread that emitted the event, so it must not block. A
    /// failure drops the record and is counted.
    fn encode(&self, record: &LogRecord<'_>, output: &mut Vec<u8>) -> anyhow::Result<()>;
}

/// Transport to the destination.
pub trait LogSink: Send + 'static {
    /// Hands one batch of encoded records to the destination.
    ///
    /// An error means the whole batch is offered again; a sink that accepted a
    /// part of it must tolerate the repetition. The future may be dropped when
    /// the configured write timeout expires, and the next call must recover.
    fn write<'a>(&'a mut self, batch: &'a [Vec<u8>]) -> BoxFuture<'a, anyhow::Result<()>>;
}
