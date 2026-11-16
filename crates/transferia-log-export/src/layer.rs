use std::fmt::{self, Write as _};
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_log::NormalizeEvent as _;
use tracing_subscriber::layer::{Context, Layer};

use crate::record::{LogField, LogRecord, LogValue};
use crate::worker::{in_export_worker, Shared};

const MESSAGE_FIELD: &str = "message";
const LOG_BRIDGE_FIELD_PREFIX: &str = "log.";

/// `tracing` layer that encodes every event and queues it for export.
pub struct LogExportLayer {
    shared: Arc<Shared>,
    sender: mpsc::Sender<Vec<u8>>,
}

impl LogExportLayer {
    pub(crate) const fn new(shared: Arc<Shared>, sender: mpsc::Sender<Vec<u8>>) -> Self {
        Self { shared, sender }
    }
}

impl<S: Subscriber> Layer<S> for LogExportLayer {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        // After shutdown the event stays with the other layers.
        if in_export_worker() || self.sender.is_closed() {
            return;
        }
        let timestamp = SystemTime::now();
        // Events bridged from the `log` crate carry their origin in fields.
        let normalized = event.normalized_metadata();
        let metadata = normalized.as_ref().unwrap_or_else(|| event.metadata());
        let mut visitor = FieldVisitor::new(self.shared.max_record_bytes(), normalized.is_some());
        event.record(&mut visitor);
        if visitor.overflowed {
            self.shared.count_oversized();
            return;
        }
        self.shared.submit(
            &LogRecord {
                timestamp,
                level: *metadata.level(),
                target: metadata.target(),
                file: metadata.file(),
                line: metadata.line(),
                message: &visitor.message,
                fields: &visitor.fields,
            },
            &self.sender,
        );
    }
}

/// Collects event fields while keeping the formatted text within the record
/// limit, so one oversized value cannot allocate without bound.
struct FieldVisitor {
    message: String,
    fields: Vec<LogField>,
    remaining_bytes: usize,
    overflowed: bool,
    bridged_from_log: bool,
}

impl FieldVisitor {
    const fn new(max_record_bytes: usize, bridged_from_log: bool) -> Self {
        Self {
            message: String::new(),
            fields: Vec::new(),
            remaining_bytes: max_record_bytes,
            overflowed: false,
            bridged_from_log,
        }
    }

    fn skips(&self, field: &Field) -> bool {
        self.overflowed
            || (self.bridged_from_log && field.name().starts_with(LOG_BRIDGE_FIELD_PREFIX))
    }

    fn text(&mut self, field: &Field, value: fmt::Arguments<'_>) {
        if self.skips(field) {
            return;
        }
        let mut text = BoundedText {
            text: String::new(),
            remaining_bytes: self.remaining_bytes,
        };
        if text.write_fmt(value).is_err() {
            self.overflowed = true;
            return;
        }
        self.remaining_bytes = text.remaining_bytes;
        if field.name() == MESSAGE_FIELD {
            self.message = text.text;
        } else {
            self.fields.push(LogField {
                name: field.name(),
                value: LogValue::Str(text.text),
            });
        }
    }

    fn scalar(&mut self, field: &Field, value: LogValue) {
        if !self.skips(field) {
            self.fields.push(LogField {
                name: field.name(),
                value,
            });
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.text(field, format_args!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.text(field, format_args!("{value}"));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.text(field, format_args!("{value}"));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.scalar(field, LogValue::I64(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.scalar(field, LogValue::U64(value));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        match i64::try_from(value) {
            Ok(value) => self.scalar(field, LogValue::I64(value)),
            Err(_) => self.text(field, format_args!("{value}")),
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        match u64::try_from(value) {
            Ok(value) => self.scalar(field, LogValue::U64(value)),
            Err(_) => self.text(field, format_args!("{value}")),
        }
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.scalar(field, LogValue::F64(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.scalar(field, LogValue::Bool(value));
    }
}

struct BoundedText {
    text: String,
    remaining_bytes: usize,
}

impl fmt::Write for BoundedText {
    fn write_str(&mut self, part: &str) -> fmt::Result {
        self.remaining_bytes = self
            .remaining_bytes
            .checked_sub(part.len())
            .ok_or(fmt::Error)?;
        self.text.push_str(part);
        Ok(())
    }
}
