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
}

pub struct Partitioner {
    kind: PartitionerKind,
    source_route: Option<SourceRoute>,
    record_time_route: Option<(i64, Arc<str>)>,
}

struct SourceRoute {
    topic: Arc<str>,
    partition: i64,
    path: Arc<str>,
}

enum PartitionerKind {
    Source,
    Fields(Vec<String>),
    RecordTime {
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
            PartitioningConfig::RecordTime {
                window,
                path,
                timezone,
            } => {
                let window_ms = i64::try_from(window.0.as_millis())?;
                let timezone = timezone
                    .parse::<Tz>()
                    .map_err(|_| anyhow::anyhow!("invalid IANA timezone '{timezone}'"))?;
                PartitionerKind::RecordTime {
                    window_ms,
                    path: path.clone(),
                    timezone,
                }
            }
        };
        Ok(Self {
            kind,
            source_route: None,
            record_time_route: None,
        })
    }

    pub fn route_batch(&mut self, output: &SinkBatch) -> anyhow::Result<Vec<RowRoute>> {
        let columns = RouteColumns::new(output, &self.kind)?;
        let mut routes = Vec::with_capacity(output.rows());
        for row in 0..output.rows() {
            ensure_not_null(columns.topic, SystemColumnKind::TopicName, row)?;
            ensure_not_null(columns.partition, SystemColumnKind::PartitionNum, row)?;
            ensure_not_null(columns.offset, SystemColumnKind::Offset, row)?;
            ensure_not_null(columns.message_index, SystemColumnKind::MessageIndex, row)?;
            let source_record_time = columns
                .record_time
                .map(|array| {
                    ensure_not_null(array, SystemColumnKind::WriteTimestampMs, row)?;
                    Ok::<i64, anyhow::Error>(array.value(row))
                })
                .transpose()?;
            let partition = columns.partition.value(row);
            let (topic, source_path) =
                self.cached_source_route(columns.topic.value(row), partition);
            let (partition_path, record_time_ms) = if output.is_dlq {
                (source_path, source_record_time)
            } else {
                match &self.kind {
                    PartitionerKind::Source => (source_path, source_record_time),
                    PartitionerKind::Fields(_) => {
                        let mut path = String::new();
                        for (position, (encoded_name, index)) in
                            columns.partition_fields.iter().enumerate()
                        {
                            let value = scalar_partition_value(&output.batch, *index, row)?;
                            if position != 0 {
                                path.push('/');
                            }
                            path.push_str(encoded_name);
                            path.push('=');
                            path.push_str(&percent_encode(value.as_bytes()));
                        }
                        (Arc::from(path), source_record_time)
                    }
                    PartitionerKind::RecordTime {
                        window_ms,
                        path,
                        timezone,
                    } => {
                        let timestamp_ms = source_record_time.ok_or_else(|| {
                            anyhow::anyhow!(
                                "required system column '{}' is absent",
                                SystemColumnKind::WriteTimestampMs.name()
                            )
                        })?;
                        let slot = timestamp_ms.div_euclid(*window_ms) * *window_ms;
                        let partition_path =
                            if let Some((cached_slot, cached_path)) = &self.record_time_route {
                                if *cached_slot == slot {
                                    Arc::clone(cached_path)
                                } else {
                                    record_time_path(slot, path, *timezone)?
                                }
                            } else {
                                record_time_path(slot, path, *timezone)?
                            };
                        self.record_time_route = Some((slot, Arc::clone(&partition_path)));
                        (partition_path, Some(timestamp_ms))
                    }
                }
            };

            routes.push(RowRoute {
                partition_path,
                topic,
                partition,
                offset: columns.offset.value(row),
                message_index: columns.message_index.value(row),
                record_time_ms,
            });
        }
        Ok(routes)
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

fn record_time_path(slot: i64, path: &str, timezone: Tz) -> anyhow::Result<Arc<str>> {
    let instant = chrono::Utc
        .timestamp_millis_opt(slot)
        .single()
        .ok_or_else(|| anyhow::anyhow!("timestamp {slot}ms is out of range"))?;
    let items = chrono::format::StrftimeItems::new(path)
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid record-time path format '{path}': {error}"))?;
    let mut rendered = String::new();
    instant
        .with_timezone(&timezone)
        .format_with_items(items.iter())
        .write_to(&mut rendered)
        .map_err(|_| anyhow::anyhow!("record-time path '{path}' could not be formatted"))?;
    validate_partition_path(&rendered)?;
    Ok(Arc::from(rendered))
}

pub(super) fn validate_partition_path(path: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!path.is_empty(), "partition path must not be empty");
    anyhow::ensure!(
        !path.starts_with('/') && !path.ends_with('/'),
        "partition path must be relative and must not have an empty edge segment"
    );
    object_store::path::Path::parse(path)
        .map(|_| ())
        .map_err(|error| anyhow::anyhow!("invalid partition path '{path}': {error}"))
}

struct RouteColumns<'batch> {
    topic: &'batch StringArray,
    partition: &'batch Int64Array,
    offset: &'batch Int64Array,
    message_index: &'batch UInt64Array,
    record_time: Option<&'batch Int64Array>,
    partition_fields: Vec<(String, usize)>,
}

impl<'batch> RouteColumns<'batch> {
    fn new(output: &'batch SinkBatch, kind: &PartitionerKind) -> anyhow::Result<Self> {
        let record_time = match kind {
            PartitionerKind::RecordTime { .. } if !output.is_dlq => {
                Some(system_array::<Int64Array>(
                    &output.batch,
                    &output.system_columns,
                    SystemColumnKind::WriteTimestampMs,
                )?)
            }
            _ => optional_system_array::<Int64Array>(
                &output.batch,
                &output.system_columns,
                SystemColumnKind::WriteTimestampMs,
            )?,
        };
        let partition_fields = match kind {
            PartitionerKind::Fields(columns) if !output.is_dlq => columns
                .iter()
                .map(|name| {
                    let index = output.batch.schema().index_of(name).map_err(|_| {
                        anyhow::anyhow!("configured S3 partition column '{name}' is absent")
                    })?;
                    Ok((percent_encode(name.as_bytes()), index))
                })
                .collect::<anyhow::Result<_>>()?,
            _ => Vec::new(),
        };
        Ok(Self {
            topic: system_array::<StringArray>(
                &output.batch,
                &output.system_columns,
                SystemColumnKind::TopicName,
            )?,
            partition: system_array::<Int64Array>(
                &output.batch,
                &output.system_columns,
                SystemColumnKind::PartitionNum,
            )?,
            offset: system_array::<Int64Array>(
                &output.batch,
                &output.system_columns,
                SystemColumnKind::Offset,
            )?,
            message_index: system_array::<UInt64Array>(
                &output.batch,
                &output.system_columns,
                SystemColumnKind::MessageIndex,
            )?,
            record_time,
            partition_fields,
        })
    }
}

fn system_index(columns: &SystemColumns, kind: SystemColumnKind) -> anyhow::Result<usize> {
    columns
        .get(kind)
        .map(|column| column.index)
        .ok_or_else(|| anyhow::anyhow!("required system column '{}' is absent", kind.name()))
}

fn system_array<'batch, T: Array + 'static>(
    batch: &'batch RecordBatch,
    columns: &SystemColumns,
    kind: SystemColumnKind,
) -> anyhow::Result<&'batch T> {
    let index = system_index(columns, kind)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow::anyhow!("system column '{}' has invalid Arrow type", kind.name()))
}

fn optional_system_array<'batch, T: Array + 'static>(
    batch: &'batch RecordBatch,
    columns: &SystemColumns,
    kind: SystemColumnKind,
) -> anyhow::Result<Option<&'batch T>> {
    let Some(column) = columns.get(kind) else {
        return Ok(None);
    };
    batch
        .column(column.index)
        .as_any()
        .downcast_ref::<T>()
        .map(Some)
        .ok_or_else(|| anyhow::anyhow!("system column '{}' has invalid Arrow type", kind.name()))
}

fn ensure_not_null(array: &dyn Array, kind: SystemColumnKind, row: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        !array.is_null(row),
        "system column '{}' is NULL",
        kind.name()
    );
    Ok(())
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

#[cfg(test)]
mod record_time_path_tests {
    use super::*;

    #[test]
    fn dynamic_empty_record_time_segment_is_rejected_without_panicking() {
        let error = record_time_path(0, "fraction/%.f/end", chrono_tz::UTC)
            .expect_err("whole-second timestamp must not create an empty path segment");
        assert!(error.to_string().contains("invalid partition path"));
    }

    #[test]
    fn default_record_time_path_is_valid() -> anyhow::Result<()> {
        let path = record_time_path(0, "dt=%Y-%m-%d/hour=%H", chrono_tz::UTC)?;
        assert_eq!(path.as_ref(), "dt=1970-01-01/hour=00");
        Ok(())
    }
}
