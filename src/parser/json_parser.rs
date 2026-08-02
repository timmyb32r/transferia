use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, Date64Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    LargeStringBuilder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use serde_json::Value;
use std::sync::{Arc, LazyLock};

use crate::config::yaml::{parse_arrow_type, SchemaConfig};
use crate::pipeline::parser::Parser;
use crate::types::arrow_batch::{ArrowBatch, BatchMeta};
use crate::types::message::Message;

/// Pre-compiled dispatch: one variant per supported Arrow column type.
#[derive(Clone, Copy)]
enum ColumnKind {
    Utf8,
    LargeUtf8,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    Date32,
    Date64,
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
}

impl ColumnKind {
    fn from_data_type(dt: &DataType) -> Option<Self> {
        Some(match dt {
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Boolean => Self::Boolean,
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Timestamp(TimeUnit::Second, _) => Self::TimestampSecond,
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::TimestampMillisecond,
            DataType::Timestamp(TimeUnit::Microsecond, _) => Self::TimestampMicrosecond,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Self::TimestampNanosecond,
            _ => return None,
        })
    }
}

static DLQ_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("raw_bytes", DataType::Utf8, false),
        Field::new("error_message", DataType::Utf8, false),
        Field::new("partition_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Utf8, false),
    ]))
});

/// JSONPath → Arrow parser (one per schema, shared across partitions).
pub struct JsonParser {
    mappings: Vec<ColumnMappingExt>,
    kinds: Vec<ColumnKind>,
    arrow_schema: Arc<Schema>,
    table_name: Arc<str>,
    dlq_table_name: Arc<str>,
}

struct ColumnMappingExt {
    jsonpath: String,
}

impl JsonParser {
    pub fn new(
        config: &SchemaConfig,
        table_name: &str,
        dlq_table_name: &str,
    ) -> anyhow::Result<Self> {
        let n = config.columns.len();
        let mut mappings = Vec::with_capacity(n);
        let mut kinds = Vec::with_capacity(n);

        for col in &config.columns {
            let arrow_type = parse_arrow_type(&col.arrow_type)?;
            let kind = ColumnKind::from_data_type(&arrow_type)
                .ok_or_else(|| anyhow::anyhow!(
                    "Column '{}': unsupported Arrow type {:?}", col.column_name, arrow_type
                ))?;
            kinds.push(kind);
            mappings.push(ColumnMappingExt {
                jsonpath: col.jsonpath.clone(),
            });
        }

        let fields: Vec<Field> = config.columns.iter()
            .map(|col| {
                let dt = parse_arrow_type(&col.arrow_type).unwrap_or(DataType::Utf8);
                Field::new(&col.column_name, dt, true)
            })
            .collect();
        let arrow_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            mappings,
            kinds,
            arrow_schema,
            table_name: Arc::from(table_name),
            dlq_table_name: Arc::from(dlq_table_name),
        })
    }

    #[inline]
    fn extract_value(&self, json: &Value, mapping: &ColumnMappingExt) -> Option<Value> {
        let results = jsonpath_lib::select(json, &mapping.jsonpath).ok()?;
        results.first().map(|v| (*v).clone())
    }

    fn build_arrow_batch(
        &self,
        rows: &[Vec<Option<Value>>],
        partition_id: i64,
        offsets: &[(i64, i64)],
        dlq_flag: bool,
    ) -> anyhow::Result<ArrowBatch> {
        let mut builders: Vec<Box<dyn ArrayBuilder>> = self
            .kinds
            .iter()
            .map(|&k| make_builder(k))
            .collect();

        for row in rows {
            for ((kind, builder), value_opt) in self.kinds.iter()
                .zip(builders.iter_mut())
                .zip(row.iter())
            {
                append_value(*kind, builder.as_mut(), value_opt);
            }
        }

        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|mut b| b.finish())
            .collect();

        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrays)?;

        let name: Arc<str> = if dlq_flag {
            self.dlq_table_name.clone()
        } else {
            self.table_name.clone()
        };

        Ok(ArrowBatch {
            batch,
            meta: BatchMeta {
                table_name: name,
                partition_id,
                dlq_flag,
                batch_id: crate::batch_id(),
                offsets: offsets.to_vec(),
                created_at: chrono::Utc::now(),
            },
        })
    }

    fn build_dlq_batch(
        &self,
        dlq_payloads: &[(Bytes, DlqReason)],
        partition_id: i64,
        offsets: &[(i64, i64)],
    ) -> anyhow::Result<ArrowBatch> {
        let n = dlq_payloads.len();
        let mut raw_builder = StringBuilder::with_capacity(n, n * 64);
        let mut err_builder = StringBuilder::with_capacity(n, n * 64);
        let mut pid_builder = Int64Builder::with_capacity(n);
        let mut ts_builder = StringBuilder::with_capacity(n, n * 32);

        let now = chrono::Utc::now();
        for (raw_bytes, reason) in dlq_payloads {
            raw_builder.append_value(&String::from_utf8_lossy(raw_bytes));
            err_builder.append_value(reason.as_str());
            pid_builder.append_value(partition_id);
            ts_builder.append_value(now.to_rfc3339());
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(raw_builder.finish()),
            Arc::new(err_builder.finish()),
            Arc::new(pid_builder.finish()),
            Arc::new(ts_builder.finish()),
        ];

        let batch = RecordBatch::try_new(DLQ_SCHEMA.clone(), arrays)?;

        Ok(ArrowBatch {
            batch,
            meta: BatchMeta {
                table_name: self.dlq_table_name.clone(),
                partition_id,
                dlq_flag: true,
                batch_id: crate::batch_id(),
                offsets: offsets.to_vec(),
                created_at: now,
            },
        })
    }
}

// ---------------------------------------------------------------------------
// DLQ reason — enum avoids heap-allocated String for fixed messages
// ---------------------------------------------------------------------------

enum DlqReason {
    JsonParse(String),    // carries serde_json error text
    ExtractionFailed,
}

impl DlqReason {
    fn as_str(&self) -> &str {
        match self {
            DlqReason::JsonParse(e) => e.as_str(),
            DlqReason::ExtractionFailed => "JSONPath extraction failed for one or more columns",
        }
    }
}

// ---------------------------------------------------------------------------
// Parser impl
// ---------------------------------------------------------------------------

impl Parser for JsonParser {
    fn parse(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
    ) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)> {
        let offsets: Vec<(i64, i64)> = messages.iter()
            .map(|m| (partition_id, m.offset))
            .collect();
        let estimated = messages.len();
        let mut valid_rows: Vec<Vec<Option<Value>>> = Vec::with_capacity(estimated);
        let mut dlq_payloads: Vec<(Bytes, DlqReason)> = Vec::new();

        for msg in messages {
            match serde_json::from_slice::<Value>(&msg.value) {
                Ok(json) => {
                    let mut row = Vec::with_capacity(self.mappings.len());
                    let mut all_ok = true;

                    for m in &self.mappings {
                        match self.extract_value(&json, m) {
                            Some(val) => row.push(Some(val)),
                            None => {
                                all_ok = false;
                                break;
                            }
                        }
                    }

                    if all_ok {
                        valid_rows.push(row);
                    } else {
                        dlq_payloads.push((msg.value.clone(), DlqReason::ExtractionFailed));
                    }
                }
                Err(e) => {
                    dlq_payloads.push((msg.value.clone(), DlqReason::JsonParse(e.to_string())));
                }
            }
        }

        let valid_batch =
            self.build_arrow_batch(&valid_rows, partition_id, &offsets, false)?;

        let dlq_batch = if !dlq_payloads.is_empty() {
            Some(self.build_dlq_batch(&dlq_payloads, partition_id, &offsets)?)
        } else {
            None
        };

        Ok((valid_batch, dlq_batch))
    }
}

// ---------------------------------------------------------------------------
// Builder helpers — ColumnKind dispatched, single downcast per value
// ---------------------------------------------------------------------------

#[inline]
fn make_builder(kind: ColumnKind) -> Box<dyn ArrayBuilder> {
    match kind {
        ColumnKind::Utf8 => Box::new(StringBuilder::new()),
        ColumnKind::LargeUtf8 => Box::new(LargeStringBuilder::new()),
        ColumnKind::Int64 => Box::new(Int64Builder::new()),
        ColumnKind::Int32 => Box::new(Int32Builder::new()),
        ColumnKind::Int16 => Box::new(Int16Builder::new()),
        ColumnKind::Int8 => Box::new(Int8Builder::new()),
        ColumnKind::UInt64 => Box::new(UInt64Builder::new()),
        ColumnKind::UInt32 => Box::new(UInt32Builder::new()),
        ColumnKind::UInt16 => Box::new(UInt16Builder::new()),
        ColumnKind::UInt8 => Box::new(UInt8Builder::new()),
        ColumnKind::Float64 => Box::new(Float64Builder::new()),
        ColumnKind::Float32 => Box::new(Float32Builder::new()),
        ColumnKind::Boolean => Box::new(BooleanBuilder::new()),
        ColumnKind::Date32 => Box::new(Date32Builder::new()),
        ColumnKind::Date64 => Box::new(Date64Builder::new()),
        ColumnKind::TimestampMillisecond => Box::new(TimestampMillisecondBuilder::new()),
        ColumnKind::TimestampMicrosecond => Box::new(TimestampMicrosecondBuilder::new()),
        ColumnKind::TimestampNanosecond => Box::new(TimestampNanosecondBuilder::new()),
        ColumnKind::TimestampSecond => Box::new(TimestampSecondBuilder::new()),
    }
}

macro_rules! downcast_append {
    ($builder:expr, $val:expr, $ty:ty, |$b:ident, $v:ident| $append_fn:expr) => {{
        let $b = $builder.as_any_mut().downcast_mut::<$ty>()
            .expect("ColumnKind mismatch");
        match $val {
            Some($v) => { $append_fn; }
            None => $b.append_null(),
        }
    }};
}

#[inline]
fn append_value(kind: ColumnKind, builder: &mut dyn ArrayBuilder, val: &Option<Value>) {
    match kind {
        ColumnKind::Utf8 => downcast_append!(builder, val, StringBuilder, |b, v| {
            b.append_value(v.as_str().unwrap_or(&v.to_string()))
        }),
        ColumnKind::LargeUtf8 => downcast_append!(builder, val, LargeStringBuilder, |b, v| {
            b.append_value(v.as_str().unwrap_or(&v.to_string()))
        }),
        ColumnKind::Int64 => downcast_append!(builder, val, Int64Builder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0))
        }),
        ColumnKind::Int32 => downcast_append!(builder, val, Int32Builder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0) as i32)
        }),
        ColumnKind::Int16 => downcast_append!(builder, val, Int16Builder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0) as i16)
        }),
        ColumnKind::Int8 => downcast_append!(builder, val, Int8Builder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0) as i8)
        }),
        ColumnKind::UInt64 => downcast_append!(builder, val, UInt64Builder, |b, v| {
            b.append_value(v.as_u64().unwrap_or(0))
        }),
        ColumnKind::UInt32 => downcast_append!(builder, val, UInt32Builder, |b, v| {
            b.append_value(v.as_u64().unwrap_or(0) as u32)
        }),
        ColumnKind::UInt16 => downcast_append!(builder, val, UInt16Builder, |b, v| {
            b.append_value(v.as_u64().unwrap_or(0) as u16)
        }),
        ColumnKind::UInt8 => downcast_append!(builder, val, UInt8Builder, |b, v| {
            b.append_value(v.as_u64().unwrap_or(0) as u8)
        }),
        ColumnKind::Float64 => downcast_append!(builder, val, Float64Builder, |b, v| {
            b.append_value(v.as_f64().unwrap_or(0.0))
        }),
        ColumnKind::Float32 => downcast_append!(builder, val, Float32Builder, |b, v| {
            b.append_value(v.as_f64().unwrap_or(0.0) as f32)
        }),
        ColumnKind::Boolean => downcast_append!(builder, val, BooleanBuilder, |b, v| {
            b.append_value(v.as_bool().unwrap_or(false))
        }),
        ColumnKind::TimestampMillisecond => downcast_append!(builder, val, TimestampMillisecondBuilder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0))
        }),
        ColumnKind::TimestampMicrosecond => downcast_append!(builder, val, TimestampMicrosecondBuilder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0))
        }),
        ColumnKind::TimestampNanosecond => downcast_append!(builder, val, TimestampNanosecondBuilder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0))
        }),
        ColumnKind::TimestampSecond => downcast_append!(builder, val, TimestampSecondBuilder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0))
        }),
        ColumnKind::Date32 => downcast_append!(builder, val, Date32Builder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0) as i32)
        }),
        ColumnKind::Date64 => downcast_append!(builder, val, Date64Builder, |b, v| {
            b.append_value(v.as_i64().unwrap_or(0))
        }),
    }
}
