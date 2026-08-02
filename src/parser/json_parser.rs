use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Date64Builder, Float32Builder,
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

// ---------------------------------------------------------------------------
// Compiled JSONPath — avoids jsonpath_lib overhead for simple $.field paths
// ---------------------------------------------------------------------------

enum CompiledPath {
    /// `$.field_name` — direct map lookup, O(1), no allocations.
    RootField(String),
    /// Arbitrary JSONPath — fallback to `jsonpath_lib::select`.
    Complex(String),
}

fn compile_path(raw: &str) -> CompiledPath {
    if let Some(field) = raw.strip_prefix("$.") {
        if !field.contains('.') && !field.contains('[') && !field.contains('*') && !field.contains('$') {
            return CompiledPath::RootField(field.to_string());
        }
    }
    CompiledPath::Complex(raw.to_string())
}

// ---------------------------------------------------------------------------
// Stack-allocated Arrow builder enum — no Box<dyn ArrayBuilder> heap allocs
// ---------------------------------------------------------------------------

enum AnyBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Date32(Date32Builder),
    Date64(Date64Builder),
    TimestampSecond(TimestampSecondBuilder),
    TimestampMillisecond(TimestampMillisecondBuilder),
    TimestampMicrosecond(TimestampMicrosecondBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
}

#[derive(Clone, Copy)]
enum ColumnKind {
    Utf8, LargeUtf8,
    Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
    Float32, Float64,
    Boolean,
    Date32, Date64,
    TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
}

impl ColumnKind {
    fn from_data_type(dt: &DataType) -> Option<Self> {
        Some(match dt {
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Int8 => Self::Int8, DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32, DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8, DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32, DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32, DataType::Float64 => Self::Float64,
            DataType::Boolean => Self::Boolean,
            DataType::Date32 => Self::Date32, DataType::Date64 => Self::Date64,
            DataType::Timestamp(TimeUnit::Second, _) => Self::TimestampSecond,
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::TimestampMillisecond,
            DataType::Timestamp(TimeUnit::Microsecond, _) => Self::TimestampMicrosecond,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Self::TimestampNanosecond,
            _ => return None,
        })
    }
}

#[inline]
fn make_builder(kind: ColumnKind) -> AnyBuilder {
    match kind {
        ColumnKind::Utf8 => AnyBuilder::Utf8(StringBuilder::new()),
        ColumnKind::LargeUtf8 => AnyBuilder::LargeUtf8(LargeStringBuilder::new()),
        ColumnKind::Int64 => AnyBuilder::Int64(Int64Builder::new()),
        ColumnKind::Int32 => AnyBuilder::Int32(Int32Builder::new()),
        ColumnKind::Int16 => AnyBuilder::Int16(Int16Builder::new()),
        ColumnKind::Int8 => AnyBuilder::Int8(Int8Builder::new()),
        ColumnKind::UInt64 => AnyBuilder::UInt64(UInt64Builder::new()),
        ColumnKind::UInt32 => AnyBuilder::UInt32(UInt32Builder::new()),
        ColumnKind::UInt16 => AnyBuilder::UInt16(UInt16Builder::new()),
        ColumnKind::UInt8 => AnyBuilder::UInt8(UInt8Builder::new()),
        ColumnKind::Float64 => AnyBuilder::Float64(Float64Builder::new()),
        ColumnKind::Float32 => AnyBuilder::Float32(Float32Builder::new()),
        ColumnKind::Boolean => AnyBuilder::Boolean(BooleanBuilder::new()),
        ColumnKind::Date32 => AnyBuilder::Date32(Date32Builder::new()),
        ColumnKind::Date64 => AnyBuilder::Date64(Date64Builder::new()),
        ColumnKind::TimestampMillisecond => AnyBuilder::TimestampMillisecond(TimestampMillisecondBuilder::new()),
        ColumnKind::TimestampMicrosecond => AnyBuilder::TimestampMicrosecond(TimestampMicrosecondBuilder::new()),
        ColumnKind::TimestampNanosecond => AnyBuilder::TimestampNanosecond(TimestampNanosecondBuilder::new()),
        ColumnKind::TimestampSecond => AnyBuilder::TimestampSecond(TimestampSecondBuilder::new()),
    }
}

impl AnyBuilder {
    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::LargeUtf8(b) => Arc::new(b.finish()),
            Self::Int8(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::UInt8(b) => Arc::new(b.finish()),
            Self::UInt16(b) => Arc::new(b.finish()),
            Self::UInt32(b) => Arc::new(b.finish()),
            Self::UInt64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::Date64(b) => Arc::new(b.finish()),
            Self::TimestampSecond(b) => Arc::new(b.finish()),
            Self::TimestampMillisecond(b) => Arc::new(b.finish()),
            Self::TimestampMicrosecond(b) => Arc::new(b.finish()),
            Self::TimestampNanosecond(b) => Arc::new(b.finish()),
        }
    }
}

/// Append a non-null `serde_json::Value` to the correct builder.
/// Valid rows always have a value for every column (missing path → DLQ).
#[inline]
fn append_value(builder: &mut AnyBuilder, val: &Value) {
    match builder {
        AnyBuilder::Utf8(b) => b.append_value(val.as_str().unwrap_or(&val.to_string())),
        AnyBuilder::LargeUtf8(b) => b.append_value(val.as_str().unwrap_or(&val.to_string())),
        AnyBuilder::Int64(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::Int32(b) => b.append_value(val.as_i64().unwrap_or(0) as i32),
        AnyBuilder::Int16(b) => b.append_value(val.as_i64().unwrap_or(0) as i16),
        AnyBuilder::Int8(b) => b.append_value(val.as_i64().unwrap_or(0) as i8),
        AnyBuilder::UInt64(b) => b.append_value(val.as_u64().unwrap_or(0)),
        AnyBuilder::UInt32(b) => b.append_value(val.as_u64().unwrap_or(0) as u32),
        AnyBuilder::UInt16(b) => b.append_value(val.as_u64().unwrap_or(0) as u16),
        AnyBuilder::UInt8(b) => b.append_value(val.as_u64().unwrap_or(0) as u8),
        AnyBuilder::Float64(b) => b.append_value(val.as_f64().unwrap_or(0.0)),
        AnyBuilder::Float32(b) => b.append_value(val.as_f64().unwrap_or(0.0) as f32),
        AnyBuilder::Boolean(b) => b.append_value(val.as_bool().unwrap_or(false)),
        AnyBuilder::TimestampMillisecond(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::TimestampMicrosecond(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::TimestampNanosecond(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::TimestampSecond(b) => b.append_value(val.as_i64().unwrap_or(0)),
        AnyBuilder::Date32(b) => b.append_value(val.as_i64().unwrap_or(0) as i32),
        AnyBuilder::Date64(b) => b.append_value(val.as_i64().unwrap_or(0)),
    }
}

// ---------------------------------------------------------------------------
// DLQ schema + reason
// ---------------------------------------------------------------------------

static DLQ_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("raw_bytes", DataType::Utf8, false),
        Field::new("error_message", DataType::Utf8, false),
        Field::new("partition_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Utf8, false),
    ]))
});

enum DlqReason {
    JsonParse(String),
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
// JsonParser
// ---------------------------------------------------------------------------

pub struct JsonParser {
    mappings: Vec<ColumnMappingExt>,
    kinds: Vec<ColumnKind>,
    arrow_schema: Arc<Schema>,
    table_name: Arc<str>,
    dlq_table_name: Arc<str>,
}

struct ColumnMappingExt {
    path: CompiledPath,
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
                path: compile_path(&col.jsonpath),
            });
        }

        let fields: Vec<Field> = config.columns.iter()
            .map(|col| {
                let dt = parse_arrow_type(&col.arrow_type).unwrap_or(DataType::Utf8);
                Field::new(&col.column_name, dt, true)
            })
            .collect();
        let arrow_schema = Arc::new(Schema::new(fields));

        Ok(Self { mappings, kinds, arrow_schema, table_name: Arc::from(table_name), dlq_table_name: Arc::from(dlq_table_name) })
    }

    /// Extract a value using either direct map lookup or jsonpath_lib.
    #[inline]
    fn extract_value(&self, json: &Value, mapping: &ColumnMappingExt) -> Option<Value> {
        match &mapping.path {
            CompiledPath::RootField(field) => json.get(field).cloned(),
            CompiledPath::Complex(path) => {
                jsonpath_lib::select(json, path).ok()
                    .and_then(|r| r.first().map(|v| (*v).clone()))
            }
        }
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
            Arc::new(raw_builder.finish()), Arc::new(err_builder.finish()),
            Arc::new(pid_builder.finish()), Arc::new(ts_builder.finish()),
        ];
        let batch = RecordBatch::try_new(DLQ_SCHEMA.clone(), arrays)?;

        Ok(ArrowBatch {
            batch,
            meta: BatchMeta {
                table_name: self.dlq_table_name.clone(),
                partition_id, dlq_flag: true,
                batch_id: crate::batch_id(), offsets: offsets.to_vec(),
                created_at: now,
            },
        })
    }
}

// ---------------------------------------------------------------------------
// Parser impl — direct-to-Arrow (no intermediate Vec<Vec<Option<Value>>>)
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

        // Build Arrow arrays directly — no intermediate row storage.
        let mut builders: Vec<AnyBuilder> = self.kinds.iter().map(|&k| make_builder(k)).collect();
        let mut dlq_payloads: Vec<(Bytes, DlqReason)> = Vec::new();

        for msg in messages {
            match serde_json::from_slice::<Value>(&msg.value) {
                Ok(json) => {
                    let n = self.mappings.len();
                    let mut row = Vec::with_capacity(n);
                    let mut all_ok = true;

                    for m in &self.mappings {
                        match self.extract_value(&json, m) {
                            Some(val) => row.push(val),
                            None => { all_ok = false; break; }
                        }
                    }

                    if all_ok {
                        for (builder, val) in builders.iter_mut().zip(row.iter()) {
                            append_value(builder, val);
                        }
                    } else {
                        dlq_payloads.push((msg.value.clone(), DlqReason::ExtractionFailed));
                    }
                }
                Err(e) => {
                    dlq_payloads.push((msg.value.clone(), DlqReason::JsonParse(e.to_string())));
                }
            }
        }

        let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrays)?;

        let valid_batch = ArrowBatch {
            batch,
            meta: BatchMeta {
                table_name: self.table_name.clone(),
                partition_id, dlq_flag: false,
                batch_id: crate::batch_id(), offsets: offsets.clone(),
                created_at: chrono::Utc::now(),
            },
        };

        let dlq_batch = if !dlq_payloads.is_empty() {
            Some(self.build_dlq_batch(&dlq_payloads, partition_id, &offsets)?)
        } else {
            None
        };

        Ok((valid_batch, dlq_batch))
    }
}
