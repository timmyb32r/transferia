use async_trait::async_trait;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, Date64Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    LargeStringBuilder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

use crate::config::yaml::{parse_arrow_type, SchemaConfig};
use crate::pipeline::middleware::Middleware;
use crate::pipeline::source::RawBatch;
use crate::types::arrow_batch::{ArrowBatch, BatchMeta};

/// Middleware that transforms JSON messages into Arrow record batches using
/// JSONPath column mappings.
pub struct JsonArrowMiddleware {
    mappings: Vec<ColumnMappingExt>,
    arrow_schema: Arc<Schema>,
    table_name: String,
    dlq_table_name: String,
}

/// Internal column mapping with a parsed Arrow DataType.
struct ColumnMappingExt {
    jsonpath: String,
    column_name: String,
    arrow_type: DataType,
    col_index: usize,
}

impl JsonArrowMiddleware {
    /// Create a new middleware from the schema configuration.
    ///
    /// Parses Arrow type strings via `config::yaml::parse_arrow_type` and
    /// builds an Arrow `Schema` for the output batches.
    pub fn new(
        config: &SchemaConfig,
        table_name: &str,
        dlq_table_name: &str,
    ) -> anyhow::Result<Self> {
        let mappings: Vec<ColumnMappingExt> = config
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                Ok(ColumnMappingExt {
                    jsonpath: col.jsonpath.clone(),
                    column_name: col.column_name.clone(),
                    arrow_type: parse_arrow_type(&col.arrow_type)?,
                    col_index: i,
                })
            })
            .collect::<anyhow::Result<_>>()?;

        let fields: Vec<Field> = mappings
            .iter()
            .map(|m| Field::new(&m.column_name, m.arrow_type.clone(), true))
            .collect();
        let arrow_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            mappings,
            arrow_schema,
            table_name: table_name.to_string(),
            dlq_table_name: dlq_table_name.to_string(),
        })
    }

    /// Extract a value from a JSON document using a JSONPath expression.
    ///
    /// Returns `None` if the path does not match or if the path expression is
    /// invalid.  The returned value is an owned `serde_json::Value`.
    fn extract_value(&self, json: &Value, jsonpath: &str) -> Option<Value> {
        let results = jsonpath_lib::select(json, jsonpath).ok()?;
        // jsonpath_lib::select returns Vec<&Value>; clone to get owned Value.
        results.first().map(|v| (*v).clone())
    }

    /// Build a valid Arrow batch from parsed rows.
    ///
    /// Each row is a vector of `Option<Value>` — one element per mapped column.
    /// Uses `make_builder` and `append_value` for type-correct Arrow array construction.
    fn build_arrow_batch(
        &self,
        rows: &[Vec<Option<Value>>],
        partition_id: i64,
        offsets: Vec<(i64, i64)>,
        dlq_flag: bool,
    ) -> anyhow::Result<ArrowBatch> {
        let mut builders: Vec<Box<dyn ArrayBuilder>> = self
            .mappings
            .iter()
            .map(|m| make_builder(&m.arrow_type))
            .collect();

        for row in rows {
            for (i, value_opt) in row.iter().enumerate() {
                append_value(builders[i].as_mut(), value_opt);
            }
        }

        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|mut b| b.finish())
            .collect();

        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrays)?;

        let table_name = if dlq_flag {
            self.dlq_table_name.clone()
        } else {
            self.table_name.clone()
        };

        Ok(ArrowBatch {
            batch,
            meta: BatchMeta {
                table_name,
                partition_id,
                dlq_flag,
                batch_id: uuid::Uuid::new_v4().to_string(),
                offsets,
                created_at: chrono::Utc::now(),
            },
        })
    }

    /// Build a DLQ (dead-letter queue) Arrow batch.
    ///
    /// Schema: `(raw_bytes: Utf8, error_message: Utf8, partition_id: Int64, timestamp: Utf8)`
    fn build_dlq_batch(
        &self,
        dlq_payloads: &[(Vec<u8>, String)],
        partition_id: i64,
        offsets: Vec<(i64, i64)>,
    ) -> anyhow::Result<ArrowBatch> {
        let dlq_schema = Arc::new(Schema::new(vec![
            Field::new("raw_bytes", DataType::Utf8, false),
            Field::new("error_message", DataType::Utf8, false),
            Field::new("partition_id", DataType::Int64, false),
            Field::new("timestamp", DataType::Utf8, false),
        ]));

        let mut raw_builder = StringBuilder::new();
        let mut err_builder = StringBuilder::new();
        let mut pid_builder = Int64Builder::new();
        let mut ts_builder = StringBuilder::new();

        let now = chrono::Utc::now();
        for (raw_bytes, error_msg) in dlq_payloads {
            let raw_str = String::from_utf8_lossy(raw_bytes).to_string();
            raw_builder.append_value(&raw_str);
            err_builder.append_value(error_msg);
            pid_builder.append_value(partition_id);
            ts_builder.append_value(&now.to_rfc3339());
        }

        // Wrap concrete arrays in Arc<dyn Array> to match ArrayRef type
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(raw_builder.finish()),
            Arc::new(err_builder.finish()),
            Arc::new(pid_builder.finish()),
            Arc::new(ts_builder.finish()),
        ];

        let batch = RecordBatch::try_new(dlq_schema, arrays)?;

        Ok(ArrowBatch {
            batch,
            meta: BatchMeta {
                table_name: self.dlq_table_name.clone(),
                partition_id,
                dlq_flag: true,
                batch_id: uuid::Uuid::new_v4().to_string(),
                offsets,
                created_at: now,
            },
        })
    }
}

#[async_trait]
impl Middleware for JsonArrowMiddleware {
    /// Process a raw batch of JSON bytes through the JSONPath mappings.
    ///
    /// Returns `(valid_batch, optional_dlq_batch)`:
    /// - `valid_batch` contains rows where all JSONPath extractions succeeded.
    /// - `dlq_batch` is `Some(...)` when one or more rows failed parsing or
    ///   extraction; `None` otherwise.
    async fn process(&self, raw: RawBatch) -> anyhow::Result<(ArrowBatch, Option<ArrowBatch>)> {
        let mut valid_rows: Vec<Vec<Option<Value>>> = Vec::new();
        let mut dlq_payloads: Vec<(Vec<u8>, String)> = Vec::new();

        for bytes in &raw.data {
            match serde_json::from_slice::<Value>(bytes) {
                Ok(json) => {
                    let mut row = Vec::with_capacity(self.mappings.len());
                    let mut all_ok = true;

                    for m in &self.mappings {
                        match self.extract_value(&json, &m.jsonpath) {
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
                        dlq_payloads.push((
                            bytes.clone(),
                            "JSONPath extraction failed for one or more columns".to_string(),
                        ));
                    }
                }
                Err(e) => {
                    dlq_payloads.push((
                        bytes.clone(),
                        format!("JSON parse error: {}", e),
                    ));
                }
            }
        }

        let offsets = raw.offsets.clone();
        let partition_id = raw.partition_id;

        // Build valid batch (may be empty)
        let valid_batch =
            self.build_arrow_batch(&valid_rows, partition_id, offsets.clone(), false)?;

        // Build DLQ batch if there were failures
        let dlq_batch = if !dlq_payloads.is_empty() {
            Some(self.build_dlq_batch(&dlq_payloads, partition_id, offsets)?)
        } else {
            None
        };

        Ok((valid_batch, dlq_batch))
    }
}

// ---------------------------------------------------------------------------
// Builder helpers
// ---------------------------------------------------------------------------

/// Create an Arrow `ArrayBuilder` for the given `DataType`.
///
/// CRITICAL: `Timestamp` types use the proper timestamp builders (e.g.
/// `TimestampMillisecondBuilder`) rather than a bare `Int64Builder`.
#[allow(unused_variables)]
fn make_builder(dt: &DataType) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::LargeUtf8 => Box::new(LargeStringBuilder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int16 => Box::new(Int16Builder::new()),
        DataType::Int8 => Box::new(Int8Builder::new()),
        DataType::UInt64 => Box::new(UInt64Builder::new()),
        DataType::UInt32 => Box::new(UInt32Builder::new()),
        DataType::UInt16 => Box::new(UInt16Builder::new()),
        DataType::UInt8 => Box::new(UInt8Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Date64 => Box::new(Date64Builder::new()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Box::new(TimestampNanosecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Second, _) => Box::new(TimestampSecondBuilder::new()),
        // Fallback: use StringBuilder for any unrecognised type
        _ => Box::new(StringBuilder::new()),
    }
}

/// Append a `serde_json::Value` to the correct Arrow builder by downcasting.
///
/// In Arrow 53 the `ArrayBuilder` trait does not expose `append_null`, so we
/// must downcast to each concrete builder type and call the inherent method.
macro_rules! try_append {
    ($builder:expr, $val:expr, $ty:ty, $b:ident, $v:ident, $append_fn:expr) => {
        if let Some($b) = $builder.as_any_mut().downcast_mut::<$ty>() {
            match $val {
                Some($v) => {
                    $append_fn;
                }
                None => $b.append_null(),
            }
            return;
        }
    };
}

/// Append a value (or null) to the correct Arrow builder by downcasting.
///
/// Type-coercion rules:
/// - Utf8/LargeUtf8 --> string representation
/// - Int64/32/16/8  --> `as_i64()` (truncated for smaller widths)
/// - UInt64/32/16/8 --> `as_u64()` (truncated for smaller widths)
/// - Float64/32     --> `as_f64()`
/// - Boolean        --> `as_bool()`
/// - Timestamp(…)   --> `as_i64()` (epoch milliseconds / microseconds / …)
/// - Date32/64      --> `as_i64()`
/// - None (SQL NULL) --> builder.append_null()
fn append_value(builder: &mut dyn ArrayBuilder, val: &Option<Value>) {
    // Utf8 / string types
    try_append!(
        builder, val, StringBuilder, b, v,
        b.append_value(v.as_str().unwrap_or(&v.to_string()))
    );
    try_append!(
        builder, val, LargeStringBuilder, b, v,
        b.append_value(v.as_str().unwrap_or(&v.to_string()))
    );

    // Signed integers
    try_append!(builder, val, Int64Builder, b, v, b.append_value(v.as_i64().unwrap_or(0)));
    try_append!(builder, val, Int32Builder, b, v, { b.append_value(v.as_i64().unwrap_or(0) as i32) });
    try_append!(builder, val, Int16Builder, b, v, { b.append_value(v.as_i64().unwrap_or(0) as i16) });
    try_append!(builder, val, Int8Builder, b, v, { b.append_value(v.as_i64().unwrap_or(0) as i8) });

    // Unsigned integers
    try_append!(builder, val, UInt64Builder, b, v, b.append_value(v.as_u64().unwrap_or(0)));
    try_append!(builder, val, UInt32Builder, b, v, { b.append_value(v.as_u64().unwrap_or(0) as u32) });
    try_append!(builder, val, UInt16Builder, b, v, { b.append_value(v.as_u64().unwrap_or(0) as u16) });
    try_append!(builder, val, UInt8Builder, b, v, { b.append_value(v.as_u64().unwrap_or(0) as u8) });

    // Float types
    try_append!(builder, val, Float64Builder, b, v, b.append_value(v.as_f64().unwrap_or(0.0)));
    try_append!(builder, val, Float32Builder, b, v, { b.append_value(v.as_f64().unwrap_or(0.0) as f32) });

    // Boolean
    try_append!(builder, val, BooleanBuilder, b, v, b.append_value(v.as_bool().unwrap_or(false)));

    // Timestamp types (epoch milliseconds / microseconds / nanoseconds / seconds)
    try_append!(builder, val, TimestampMillisecondBuilder, b, v, b.append_value(v.as_i64().unwrap_or(0)));
    try_append!(builder, val, TimestampMicrosecondBuilder, b, v, b.append_value(v.as_i64().unwrap_or(0)));
    try_append!(builder, val, TimestampNanosecondBuilder, b, v, b.append_value(v.as_i64().unwrap_or(0)));
    try_append!(builder, val, TimestampSecondBuilder, b, v, b.append_value(v.as_i64().unwrap_or(0)));

    // Date types
    try_append!(builder, val, Date32Builder, b, v, { b.append_value(v.as_i64().unwrap_or(0) as i32) });
    try_append!(builder, val, Date64Builder, b, v, b.append_value(v.as_i64().unwrap_or(0)));

    // Fallback — all known types exhausted
    tracing::warn!("Unhandled builder type in append_value");
}
