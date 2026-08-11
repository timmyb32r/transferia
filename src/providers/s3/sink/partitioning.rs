use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::TimeZone as _;
use chrono_tz::Tz;

use crate::pipeline::sink::SinkBatch;
use crate::types::system_columns::{SystemColumnKind, SystemColumns};

use super::config::PartitioningConfig;

#[derive(Debug, Clone)]
pub struct RowRoute {
    pub partition_path: Arc<str>,
    pub topic: Arc<str>,
    pub partition: i64,
    pub offset: i64,
    pub message_index: u64,
    pub record_time_ms: Option<i64>,
    pub time_slot_ms: Option<i64>,
}

pub struct Partitioner {
    kind: PartitionerKind,
    source_route: Option<SourceRoute>,
}

struct SourceRoute {
    topic: Arc<str>,
    partition: i64,
    path: Arc<str>,
}

enum PartitionerKind {
    Source,
    Fields(Vec<String>),
    Time {
        window_ms: i64,
        path: String,
        timezone: Tz,
    },
}

impl Partitioner {
    pub fn new(config: &PartitioningConfig) -> anyhow::Result<Self> {
        let kind = match config {
            PartitioningConfig::Source => PartitionerKind::Source,
            PartitioningConfig::Fields { columns } => PartitionerKind::Fields(columns.clone()),
            PartitioningConfig::Time {
                window,
                path,
                timezone,
            } => {
                let window_ms = i64::try_from(window.0.as_millis())?;
                let timezone = timezone
                    .parse::<Tz>()
                    .map_err(|_| anyhow::anyhow!("invalid IANA timezone '{timezone}'"))?;
                PartitionerKind::Time {
                    window_ms,
                    path: path.clone(),
                    timezone,
                }
            }
        };
        Ok(Self {
            kind,
            source_route: None,
        })
    }

    pub fn route(&mut self, output: &SinkBatch, row: usize) -> anyhow::Result<RowRoute> {
        let topic_value = system_string(
            &output.batch,
            &output.system_columns,
            SystemColumnKind::TopicName,
            row,
        )?;
        let partition = system_i64(
            &output.batch,
            &output.system_columns,
            SystemColumnKind::PartitionNum,
            row,
        )?;
        let offset = system_i64(
            &output.batch,
            &output.system_columns,
            SystemColumnKind::Offset,
            row,
        )?;
        let message_index = system_u64(
            &output.batch,
            &output.system_columns,
            SystemColumnKind::MessageIndex,
            row,
        )?;
        let source_record_time = optional_system_i64(
            &output.batch,
            &output.system_columns,
            SystemColumnKind::WriteTimestampMs,
            row,
        )?;
        let (topic, source_path) = self.cached_source_route(topic_value, partition);
        let (partition_path, record_time_ms, time_slot_ms) = if output.is_dlq {
            (source_path, source_record_time, None)
        } else {
            match &self.kind {
                PartitionerKind::Source => (source_path, source_record_time, None),
                PartitionerKind::Fields(columns) => {
                    let mut path = String::new();
                    for (position, name) in columns.iter().enumerate() {
                        let index = output.batch.schema().index_of(name).map_err(|_| {
                            anyhow::anyhow!("configured S3 partition column '{name}' is absent")
                        })?;
                        let value = scalar_partition_value(&output.batch, index, row)?;
                        if position != 0 {
                            path.push('/');
                        }
                        path.push_str(&percent_encode(name.as_bytes()));
                        path.push('=');
                        path.push_str(&percent_encode(value.as_bytes()));
                    }
                    (Arc::from(path), source_record_time, None)
                }
                PartitionerKind::Time {
                    window_ms,
                    path,
                    timezone,
                } => {
                    let timestamp_ms = system_i64(
                        &output.batch,
                        &output.system_columns,
                        SystemColumnKind::WriteTimestampMs,
                        row,
                    )?;
                    let slot = timestamp_ms.div_euclid(*window_ms) * *window_ms;
                    let instant = chrono::Utc
                        .timestamp_millis_opt(slot)
                        .single()
                        .ok_or_else(|| anyhow::anyhow!("timestamp {slot}ms is out of range"))?;
                    (
                        Arc::from(instant.with_timezone(timezone).format(path).to_string()),
                        Some(timestamp_ms),
                        Some(slot),
                    )
                }
            }
        };

        Ok(RowRoute {
            partition_path,
            topic,
            partition,
            offset,
            message_index,
            record_time_ms,
            time_slot_ms,
        })
    }

    fn cached_source_route(&mut self, topic: &str, partition: i64) -> (Arc<str>, Arc<str>) {
        if let Some(cached) = &self.source_route {
            if cached.partition == partition && cached.topic.as_ref() == topic {
                return (Arc::clone(&cached.topic), Arc::clone(&cached.path));
            }
        }
        let topic: Arc<str> = Arc::from(topic);
        let path: Arc<str> = Arc::from(format!(
            "topic={}/partition={partition}",
            percent_encode(topic.as_bytes())
        ));
        self.source_route = Some(SourceRoute {
            topic: Arc::clone(&topic),
            partition,
            path: Arc::clone(&path),
        });
        (topic, path)
    }
}

fn system_index(columns: &SystemColumns, kind: SystemColumnKind) -> anyhow::Result<usize> {
    columns
        .get(kind)
        .map(|column| column.index)
        .ok_or_else(|| anyhow::anyhow!("required system column '{}' is absent", kind.name()))
}

fn system_string<'batch>(
    batch: &'batch RecordBatch,
    columns: &SystemColumns,
    kind: SystemColumnKind,
    row: usize,
) -> anyhow::Result<&'batch str> {
    let index = system_index(columns, kind)?;
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("system column '{}' has invalid Arrow type", kind.name()))?;
    anyhow::ensure!(
        !array.is_null(row),
        "system column '{}' is NULL",
        kind.name()
    );
    Ok(array.value(row))
}

fn system_i64(
    batch: &RecordBatch,
    columns: &SystemColumns,
    kind: SystemColumnKind,
    row: usize,
) -> anyhow::Result<i64> {
    let index = system_index(columns, kind)?;
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("system column '{}' has invalid Arrow type", kind.name()))?;
    anyhow::ensure!(
        !array.is_null(row),
        "system column '{}' is NULL",
        kind.name()
    );
    Ok(array.value(row))
}

fn optional_system_i64(
    batch: &RecordBatch,
    columns: &SystemColumns,
    kind: SystemColumnKind,
    row: usize,
) -> anyhow::Result<Option<i64>> {
    let Some(column) = columns.get(kind) else {
        return Ok(None);
    };
    let array = batch
        .column(column.index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("system column '{}' has invalid Arrow type", kind.name()))?;
    anyhow::ensure!(
        !array.is_null(row),
        "system column '{}' is NULL",
        kind.name()
    );
    Ok(Some(array.value(row)))
}

fn system_u64(
    batch: &RecordBatch,
    columns: &SystemColumns,
    kind: SystemColumnKind,
    row: usize,
) -> anyhow::Result<u64> {
    let index = system_index(columns, kind)?;
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow::anyhow!("system column '{}' has invalid Arrow type", kind.name()))?;
    anyhow::ensure!(
        !array.is_null(row),
        "system column '{}' is NULL",
        kind.name()
    );
    Ok(array.value(row))
}

fn scalar_partition_value(batch: &RecordBatch, index: usize, row: usize) -> anyhow::Result<String> {
    let array = batch.column(index);
    anyhow::ensure!(
        !array.is_null(row),
        "S3 partition column '{}' is NULL",
        batch.schema().field(index).name()
    );
    macro_rules! value {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .expect("schema and Arrow array disagree")
                .value(row)
                .to_string()
        };
    }
    let value = match batch.schema().field(index).data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("schema and Arrow array disagree")
            .value(row)
            .to_owned(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("schema and Arrow array disagree")
            .value(row)
            .to_owned(),
        DataType::Boolean => value!(BooleanArray),
        DataType::Int8 => value!(Int8Array),
        DataType::Int16 => value!(Int16Array),
        DataType::Int32 => value!(Int32Array),
        DataType::Int64 => value!(Int64Array),
        DataType::UInt8 => value!(UInt8Array),
        DataType::UInt16 => value!(UInt16Array),
        DataType::UInt32 => value!(UInt32Array),
        DataType::UInt64 => value!(UInt64Array),
        DataType::Date32 => value!(Date32Array),
        DataType::Date64 => value!(Date64Array),
        DataType::Timestamp(TimeUnit::Second, _) => value!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => value!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => value!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => value!(TimestampNanosecondArray),
        unsupported => anyhow::bail!("unsupported S3 partition column type {unsupported:?}"),
    };
    Ok(value)
}

pub fn percent_encode(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for &byte in value {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
}
