use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;

use super::client::{classify_http_failure, YTsaurusClient};
use super::config::{YTsaurusSinkConfig, YTsaurusWriteFormat};
use super::schema::{
    arrow_to_yt, parse_schema, schema_to_yt, schemas_equal, validate_column_name, MAX_COLUMNS,
};
use crate::compatibility::EndpointDescriptor;
use crate::delivery::{
    validate_batch_against_discovery, validate_stored_projection, ArrowTypeFamily,
    DeliveryDiscovery, NameSyntax, SinkLimits, SinkLimitsDescription, TextLimit,
};
use crate::metrics::SinkCounters;
use crate::pipeline::sink::{Delivery, Sink, SinkEvent, SinkIo};
use crate::pipeline::PipelineFailure;
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};

const MAX_STATIC_ROW_WEIGHT: usize = 128 * 1024 * 1024;

pub struct YTsaurusSinkProvider {
    config: Arc<YTsaurusSinkConfig>,
    client: YTsaurusClient,
}

impl YTsaurusSinkProvider {
    pub fn from_config(config: YTsaurusSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let client = YTsaurusClient::new(&config.connection)?;
        Ok(Self {
            config: Arc::new(config),
            client,
        })
    }
}

impl SinkLimits for YTsaurusSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "ytsaurus",
            dataset_name: None,
            column_name: Some(TextLimit {
                syntax: NameSyntax::AnyNonEmptyUtf8,
                max_utf8_bytes: None,
            }),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::Binary,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Date32,
                ArrowTypeFamily::Date64,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "YTsaurus sink requires at least one dataset"
        );
        anyhow::ensure!(
            discovery.datasets.len() == self.tables.len(),
            "YTsaurus sink config maps {} datasets but discovery contains {}",
            self.tables.len(),
            discovery.datasets.len()
        );
        let mut names = HashSet::new();
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                names.insert(dataset.name.as_ref()),
                "delivery discovery repeats dataset '{}'",
                dataset.name
            );
            self.path_for_dataset(&dataset.name)?;
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.stored_schema.columns.is_empty(),
                "YTsaurus table for dataset '{}' cannot have an empty schema",
                dataset.name
            );
            anyhow::ensure!(
                dataset.stored_schema.columns.len() <= MAX_COLUMNS,
                "YTsaurus table for dataset '{}' exceeds {MAX_COLUMNS} columns",
                dataset.name
            );
            for column in &dataset.stored_schema.columns {
                validate_column_name(&column.name)?;
                arrow_to_yt(&column.data_type)?;
            }
        }
        for mapping in &self.tables {
            anyhow::ensure!(
                names.contains(mapping.dataset.as_str()),
                "YTsaurus mapping for dataset '{}' does not match delivery discovery",
                mapping.dataset
            );
        }
        Ok(())
    }

    fn validate_batch(
        &self,
        discovery: &DeliveryDiscovery,
        batch: &crate::pipeline::sink::SinkBatch,
    ) -> anyhow::Result<()> {
        validate_batch_against_discovery(discovery, batch)?;
        self.path_for_dataset(&batch.table)?;
        validate_row_weight(&batch.batch)?;
        Ok(())
    }
}

impl SinkProvider for YTsaurusSinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::YTsaurusSink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            for dataset in request.datasets {
                let path = self.config.path_for_dataset(&dataset.table)?;
                if self.config.replace_tables {
                    self.client.remove_table(path).await?;
                    self.client
                        .create_table(path, schema_to_yt(&dataset.schema)?)
                        .await?;
                } else {
                    let dynamic = self.client.get_json(&format!("{path}/@dynamic")).await?;
                    anyhow::ensure!(
                        dynamic == serde_json::Value::Bool(false),
                        "YTsaurus sink table '{path}' must be static"
                    );
                    let existing =
                        parse_schema(self.client.get_json(&format!("{path}/@schema")).await?)?;
                    anyhow::ensure!(
                        schemas_equal(&existing, &dataset.schema),
                        "YTsaurus sink table '{path}' schema differs from discovered dataset '{}'",
                        dataset.table
                    );
                }
            }
            Ok(())
        })
    }

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let limits: Arc<dyn SinkLimits> = Arc::clone(&self.config) as Arc<dyn SinkLimits>;
            Ok(Box::new(YTsaurusSink {
                client: self.client.clone(),
                config: Arc::clone(&self.config),
                counters: context.counters,
                discovery: context.discovery,
                limits,
            }) as Box<dyn Sink>)
        })
    }
}

struct YTsaurusSink {
    client: YTsaurusClient,
    config: Arc<YTsaurusSinkConfig>,
    counters: Arc<SinkCounters>,
    discovery: Arc<DeliveryDiscovery>,
    limits: Arc<dyn SinkLimits>,
}

impl YTsaurusSink {
    async fn write_delivery(&self, delivery: &Delivery) -> anyhow::Result<()> {
        for batch in &delivery.outputs {
            self.limits
                .validate_batch(&self.discovery, batch)
                .map_err(PipelineFailure::fatal)?;
        }
        for batch in &delivery.outputs {
            if batch.rows() == 0 {
                continue;
            }
            let stored = if self.discovery.keep_system_columns {
                batch.batch.clone()
            } else {
                project_user_columns(&batch.batch, &batch.system_columns)?
            };
            let (format, payload) = match self.config.format {
                YTsaurusWriteFormat::Arrow => ("arrow", encode_arrow(&stored)?),
                YTsaurusWriteFormat::Yson => ("yson", encode_yson(&stored)?),
            };
            let started = Instant::now();
            self.client
                .write_table(self.config.path_for_dataset(&batch.table)?, format, payload)
                .await
                .map_err(classify_http_failure)?;
            self.counters.add_busy(started.elapsed());
            self.counters.add_rows(batch.rows() as u64);
            self.counters.add_bytes(batch.bytes() as u64);
            self.counters.add_flush();
        }
        Ok(())
    }
}

impl Sink for YTsaurusSink {
    fn run(self: Box<Self>, mut io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            while let Some(delivery) = tokio::select! {
                biased;
                () = io.cancellation.cancelled() => None,
                delivery = io.deliveries.recv() => delivery,
            } {
                let id = delivery.id;
                let source_messages = delivery.meta.source_messages;
                self.write_delivery(&delivery).await?;
                self.counters.add_source_messages(source_messages);
                io.events
                    .send(SinkEvent::CommittedThrough(id))
                    .await
                    .map_err(|_| anyhow::anyhow!("YTsaurus sink event receiver closed"))?;
            }
            Ok(())
        })
    }
}

fn project_user_columns(
    batch: &RecordBatch,
    system_columns: &crate::types::system_columns::SystemColumns,
) -> anyhow::Result<RecordBatch> {
    let system_indexes = system_columns
        .iter()
        .map(|column| column.index)
        .collect::<HashSet<_>>();
    let indexes = (0..batch.num_columns())
        .filter(|index| !system_indexes.contains(index))
        .collect::<Vec<_>>();
    Ok(batch.project(&indexes)?)
}

pub(super) fn encode_arrow(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(batch.get_array_memory_size());
    {
        let mut writer = StreamWriter::try_new(&mut output, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(output)
}

pub(super) fn encode_yson(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(batch.get_array_memory_size());
    for row in 0..batch.num_rows() {
        output.push(b'{');
        for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
            write_yson_string(&mut output, field.name().as_bytes());
            output.push(b'=');
            write_yson_value(&mut output, array.as_ref(), row)?;
            output.push(b';');
        }
        output.extend_from_slice(b"};");
    }
    Ok(output)
}

fn write_yson_string(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'"');
    for byte in value {
        match *byte {
            b'"' | b'\\' => {
                output.push(b'\\');
                output.push(*byte);
            }
            b'\n' => output.extend_from_slice(b"\\n"),
            b'\r' => output.extend_from_slice(b"\\r"),
            b'\t' => output.extend_from_slice(b"\\t"),
            0x20..=0x7e => output.push(*byte),
            other => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                output.extend_from_slice(b"\\x");
                output.push(HEX[usize::from(other >> 4)]);
                output.push(HEX[usize::from(other & 0x0f)]);
            }
        }
    }
    output.push(b'"');
}

macro_rules! primitive_yson {
    ($array:expr_2021, $ty:ty, $row:expr_2021, $output:expr_2021) => {{
        let value = $array
            .as_any()
            .downcast_ref::<$ty>()
            .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
            .value($row);
        $output.extend_from_slice(value.to_string().as_bytes());
    }};
}

fn write_yson_value(output: &mut Vec<u8>, array: &dyn Array, row: usize) -> anyhow::Result<()> {
    if array.is_null(row) {
        output.push(b'#');
        return Ok(());
    }
    match array.data_type() {
        DataType::Boolean => {
            let value = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                .value(row);
            output.extend_from_slice(if value { b"%true" } else { b"%false" });
        }
        DataType::Int8 => primitive_yson!(array, Int8Array, row, output),
        DataType::Int16 => primitive_yson!(array, Int16Array, row, output),
        DataType::Int32 => primitive_yson!(array, Int32Array, row, output),
        DataType::Int64 => primitive_yson!(array, Int64Array, row, output),
        DataType::UInt8 => {
            primitive_yson!(array, UInt8Array, row, output);
            output.push(b'u');
        }
        DataType::UInt16 => {
            primitive_yson!(array, UInt16Array, row, output);
            output.push(b'u');
        }
        DataType::UInt32 => {
            primitive_yson!(array, UInt32Array, row, output);
            output.push(b'u');
        }
        DataType::UInt64 => {
            primitive_yson!(array, UInt64Array, row, output);
            output.push(b'u');
        }
        DataType::Float32 => primitive_yson!(array, Float32Array, row, output),
        DataType::Float64 => primitive_yson!(array, Float64Array, row, output),
        DataType::Utf8 => write_yson_string(
            output,
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                .value(row)
                .as_bytes(),
        ),
        DataType::Binary => write_yson_string(
            output,
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                .value(row),
        ),
        DataType::Date32 => primitive_yson!(array, Date32Array, row, output),
        DataType::Date64 => primitive_yson!(array, Date64Array, row, output),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            primitive_yson!(array, TimestampMicrosecondArray, row, output);
            output.push(b'u');
        }
        other => anyhow::bail!("Arrow type {other:?} is not supported by YSON writer"),
    }
    Ok(())
}

pub(super) fn validate_row_weight(batch: &RecordBatch) -> anyhow::Result<()> {
    for row in 0..batch.num_rows() {
        let mut weight = 0_usize;
        for array in batch.columns() {
            if array.is_null(row) {
                continue;
            }
            weight = weight.saturating_add(match array.data_type() {
                DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
                DataType::Int16 | DataType::UInt16 => 2,
                DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,
                DataType::Int64
                | DataType::UInt64
                | DataType::Float64
                | DataType::Date64
                | DataType::Timestamp(TimeUnit::Microsecond, None) => 8,
                DataType::Utf8 => array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                    .value_length(row) as usize,
                DataType::Binary => array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                    .value_length(row) as usize,
                other => anyhow::bail!("Arrow type {other:?} is not supported by YTsaurus sink"),
            });
        }
        anyhow::ensure!(
            weight <= MAX_STATIC_ROW_WEIGHT,
            "YTsaurus row {row} weight {weight} exceeds {MAX_STATIC_ROW_WEIGHT} bytes"
        );
    }
    Ok(())
}
