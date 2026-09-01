use super::config::JsonFramingMode;
use super::parser::ColumnKind;
use transferia_core::data::message::Message;
use transferia_core::data::system_columns::{SystemColumnKind, SystemColumns};

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
            ColumnKind::Utf8 | ColumnKind::Json => {
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
            SystemColumnKind::Topic | SystemColumnKind::ChangeOperation => {
                rows.saturating_add(1).saturating_mul(4)
            }
            SystemColumnKind::ChangedColumns => rows
                .saturating_mul(kinds.len().div_ceil(8))
                .saturating_add(rows.saturating_add(1).saturating_mul(4)),
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
