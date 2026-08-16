use super::config::JsonFramingMode;
use super::parser::ColumnKind;
use crate::core::data::message::Message;
use crate::core::data::system_columns::{SystemColumnKind, SystemColumns};

pub(super) const MAX_DELIVERY_BYTES: usize = 256 * 1024 * 1024;
pub(super) const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub(super) enum SafetyLimitViolation {
    Record {
        input_bytes: usize,
        message_count: usize,
        record_count: usize,
        message_index: usize,
        record_index: usize,
        record_bytes: usize,
    },
    WorkingSet {
        input_bytes: usize,
        message_count: usize,
        record_count: usize,
        max_record_bytes: usize,
        estimated_working_set_bytes: usize,
    },
}

impl core::fmt::Display for SafetyLimitViolation {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Record {
                input_bytes,
                message_count,
                record_count,
                message_index,
                record_index,
                record_bytes,
            } => write!(
                formatter,
                "JSON parser record safety limit exceeded: record_bytes={record_bytes}, limit_bytes={MAX_RECORD_BYTES}, message_index={message_index}, record_index={record_index}, input_bytes={input_bytes}, message_count={message_count}, record_count={record_count}"
            ),
            Self::WorkingSet {
                input_bytes,
                message_count,
                record_count,
                max_record_bytes,
                estimated_working_set_bytes,
            } => write!(
                formatter,
                "JSON parser delivery safety limit exceeded: estimated_working_set_bytes={estimated_working_set_bytes}, limit_bytes={MAX_DELIVERY_BYTES}, input_bytes={input_bytes}, message_count={message_count}, record_count={record_count}, max_record_bytes={max_record_bytes}"
            ),
        }
    }
}

pub(super) fn safety_limit_violation(
    kinds: &[ColumnKind],
    system_kinds: &[SystemColumnKind],
    dlq_system_columns: &SystemColumns,
    framing: JsonFramingMode,
    messages: &[Message],
) -> Option<SafetyLimitViolation> {
    let input_bytes = messages.iter().fold(0_usize, |total, message| {
        total.saturating_add(message.value.len())
    });
    let message_count = messages.len();
    let record_count = messages.iter().fold(0_usize, |total, message| {
        total.saturating_add(framing.count_records(&message.value))
    });
    let mut largest_record = (0_usize, 0_usize, 0_usize);
    for (message_index, message) in messages.iter().enumerate() {
        match framing {
            JsonFramingMode::SingleDocument => {
                if message.value.len() > largest_record.2 {
                    largest_record = (message_index, 0, message.value.len());
                }
            }
            JsonFramingMode::JsonLines | JsonFramingMode::JsonArray => {
                // `split` is allocation-free and matches the parser's record framing.
                for (record_index, record) in message.value.split(|byte| *byte == b'\n').enumerate()
                {
                    if record.len() > largest_record.2 {
                        largest_record = (message_index, record_index, record.len());
                    }
                }
            }
        }
    }
    if largest_record.2 > MAX_RECORD_BYTES {
        return Some(SafetyLimitViolation::Record {
            input_bytes,
            message_count,
            record_count,
            message_index: largest_record.0,
            record_index: largest_record.1,
            record_bytes: largest_record.2,
        });
    }
    let estimated_working_set_bytes =
        output_memory_bound(kinds, system_kinds, dlq_system_columns, framing, messages);
    (estimated_working_set_bytes > MAX_DELIVERY_BYTES).then_some(SafetyLimitViolation::WorkingSet {
        input_bytes,
        message_count,
        record_count,
        max_record_bytes: largest_record.2,
        estimated_working_set_bytes,
    })
}

pub(super) fn output_memory_bound(
    kinds: &[ColumnKind],
    system_kinds: &[SystemColumnKind],
    dlq_system_columns: &SystemColumns,
    framing: JsonFramingMode,
    messages: &[Message],
) -> usize {
    let row_counts = messages
        .iter()
        .map(|message| framing.count_records(&message.value))
        .collect::<Vec<_>>();
    let rows = row_counts
        .iter()
        .fold(0_usize, |total, rows| total.saturating_add(*rows));
    let input_bytes = messages.iter().fold(0_usize, |total, message| {
        total.saturating_add(message.value.len())
    });
    let validity_bytes = rows.div_ceil(8).saturating_add(64);
    let mut main_bytes = 0_usize;
    for kind in kinds {
        main_bytes = main_bytes.saturating_add(match kind {
            ColumnKind::Utf8 => {
                input_bytes.saturating_add(rows.saturating_add(1).saturating_mul(4))
            }
            ColumnKind::LargeUtf8 => {
                input_bytes.saturating_add(rows.saturating_add(1).saturating_mul(8))
            }
            ColumnKind::Boolean => rows.div_ceil(8),
            fixed => rows.saturating_mul(fixed.fixed_width_bytes().unwrap_or_default()),
        });
        main_bytes = main_bytes.saturating_add(validity_bytes);
    }
    for kind in system_kinds {
        main_bytes = main_bytes.saturating_add(match kind {
            SystemColumnKind::Topic => rows.saturating_add(1).saturating_mul(4),
            SystemColumnKind::Partition
            | SystemColumnKind::Offset
            | SystemColumnKind::MessageIndex
            | SystemColumnKind::WriteTimestampMs => rows.saturating_mul(8),
        });
    }
    let topic_rows = messages
        .iter()
        .zip(&row_counts)
        .fold(0_usize, |total, (message, rows)| {
            total.saturating_add(
                message
                    .meta
                    .topic
                    .as_ref()
                    .map_or(0, |topic| topic.len().saturating_mul(*rows)),
            )
        });
    if system_kinds.contains(&SystemColumnKind::Topic) {
        main_bytes = main_bytes.saturating_add(topic_rows);
    }
    let dlq_topic_bytes = if dlq_system_columns.contains(SystemColumnKind::Topic) {
        topic_rows
    } else {
        0
    };
    let dlq_bytes = input_bytes
        .div_ceil(3)
        .saturating_mul(4)
        .saturating_add(rows.saturating_mul(96))
        .saturating_add(dlq_topic_bytes);
    let structural_bytes = kinds
        .len()
        .saturating_add(system_kinds.len().saturating_mul(2))
        .saturating_add(3)
        .saturating_mul(256);
    main_bytes
        .saturating_add(dlq_bytes)
        .saturating_add(structural_bytes)
        .max(1)
        .saturating_mul(2)
}
