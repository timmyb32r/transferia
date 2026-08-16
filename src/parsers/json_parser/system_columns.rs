use arrow::array::{Int64Builder, StringBuilder, UInt64Builder};

use super::parser::AnyBuilder;
use crate::core::data::system_columns::SystemColumnKind;

pub(super) fn make_system_builder(kind: SystemColumnKind, capacity: usize) -> AnyBuilder {
    const MAX_INITIAL_ROWS: usize = 65_536;
    const MAX_INITIAL_TOPIC_BYTES: usize = 1024 * 1024;
    let capacity = capacity.min(MAX_INITIAL_ROWS);
    match kind {
        SystemColumnKind::Topic => AnyBuilder::Utf8(StringBuilder::with_capacity(
            capacity,
            capacity.saturating_mul(64).min(MAX_INITIAL_TOPIC_BYTES),
        )),
        SystemColumnKind::Partition
        | SystemColumnKind::Offset
        | SystemColumnKind::WriteTimestampMs => {
            AnyBuilder::Int64(Int64Builder::with_capacity(capacity))
        }
        SystemColumnKind::MessageIndex => {
            AnyBuilder::UInt64(UInt64Builder::with_capacity(capacity))
        }
    }
}

pub(super) fn make_exact_system_builder(
    kind: SystemColumnKind,
    capacity: usize,
    topic_bytes: usize,
) -> AnyBuilder {
    match kind {
        SystemColumnKind::Topic => {
            AnyBuilder::Utf8(StringBuilder::with_capacity(capacity, topic_bytes))
        }
        SystemColumnKind::Partition
        | SystemColumnKind::Offset
        | SystemColumnKind::WriteTimestampMs => {
            AnyBuilder::Int64(Int64Builder::with_capacity(capacity))
        }
        SystemColumnKind::MessageIndex => {
            AnyBuilder::UInt64(UInt64Builder::with_capacity(capacity))
        }
    }
}

pub(super) struct SystemColumnValues<'a> {
    pub topic: &'a str,
    pub partition: i64,
    pub offset: i64,
    pub write_timestamp_ms: i64,
}

#[inline]
#[expect(
    clippy::unreachable,
    reason = "builder variants are constructed from the same system-column kind list"
)]
pub(super) fn append_system_columns(
    builders: &mut [AnyBuilder],
    data_columns: usize,
    kinds: &[SystemColumnKind],
    values: &SystemColumnValues<'_>,
    message_index: u64,
) {
    for (builder, kind) in builders[data_columns..].iter_mut().zip(kinds) {
        match (kind, builder) {
            (SystemColumnKind::Topic, AnyBuilder::Utf8(builder)) => {
                builder.append_value(values.topic);
            }
            (SystemColumnKind::Partition, AnyBuilder::Int64(builder)) => {
                builder.append_value(values.partition);
            }
            (SystemColumnKind::Offset, AnyBuilder::Int64(builder)) => {
                builder.append_value(values.offset);
            }
            (SystemColumnKind::MessageIndex, AnyBuilder::UInt64(builder)) => {
                builder.append_value(message_index);
            }
            (SystemColumnKind::WriteTimestampMs, AnyBuilder::Int64(builder)) => {
                builder.append_value(values.write_timestamp_ms);
            }
            _ => unreachable!("system column builder must match its semantic kind"),
        }
    }
}
