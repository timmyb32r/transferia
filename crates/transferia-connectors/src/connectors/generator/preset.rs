use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use schemars::JsonSchema;
use serde::Deserialize;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DataGeneratorPreset {
    #[schemars(title = "Transfer logs")]
    TransferLogs,
    #[schemars(title = "Numeric")]
    Numeric {
        #[schemars(title = "Column count", range(min = 1))]
        column_count: usize,
    },
}

impl DataGeneratorPreset {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        if let Self::Numeric { column_count } = self {
            anyhow::ensure!(
                *column_count > 0,
                "generator numeric column_count must be positive"
            );
            let _ = self.logical_row_bytes()?;
        }
        Ok(())
    }

    pub(super) fn logical_row_bytes(&self) -> anyhow::Result<u64> {
        match self {
            Self::TransferLogs => Ok(512),
            Self::Numeric { column_count } => u64::try_from(*column_count)?
                .checked_mul(8)
                .ok_or_else(|| anyhow::anyhow!("generator numeric row width overflow")),
        }
    }

    pub(super) fn schema(&self) -> DatasetSchema {
        match self {
            Self::TransferLogs => transfer_logs_schema(),
            Self::Numeric { column_count } => DatasetSchema::new(
                (1..=*column_count)
                    .map(|index| {
                        SchemaColumn::new(format!("column_{index}"), DataType::UInt64, false)
                            .with_constraints(index == 1, false, None)
                    })
                    .collect(),
            ),
        }
    }

    pub(super) fn batch(&self, start: u64, rows: u64) -> anyhow::Result<RecordBatch> {
        let rows = usize::try_from(rows)?;
        let schema = self.schema();
        let fields = schema
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    column.name.clone(),
                    column.data_type.clone(),
                    column.nullable,
                )
                .with_metadata(column.arrow_metadata())
            })
            .collect::<Vec<_>>();
        let columns = match self {
            Self::TransferLogs => transfer_log_arrays(start, rows),
            Self::Numeric { column_count } => (0..*column_count)
                .map(|column| {
                    Arc::new(UInt64Array::from_iter_values(
                        (start..start + rows as u64).map(|row| row ^ column as u64),
                    )) as ArrayRef
                })
                .collect(),
        };
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }
}

fn transfer_logs_schema() -> DatasetSchema {
    let mut columns = vec![
        column("caller", DataType::Utf8, false),
        column("data_row_events", DataType::Int64, true),
        column("dc", DataType::Utf8, true),
        column("dst_type", DataType::Utf8, false),
        column("events", DataType::Int64, true),
        column("flush_num", DataType::Int64, true),
        column("host", DataType::Utf8, false),
        column("id", DataType::Utf8, false),
        column("job_id", DataType::Utf8, false),
        column("job_index", DataType::Int64, false),
        column("lag", DataType::Float64, true),
        column("len", DataType::Int64, true),
        column("level", DataType::Utf8, false),
        column("msg", DataType::Utf8, false),
        column("revision", DataType::Utf8, false),
        column("runtime", DataType::Utf8, false),
        column("size", DataType::Utf8, true),
        column("src_type", DataType::Utf8, false),
        column("trigging_interval", DataType::Utf8, true),
        column("ts", DataType::Utf8, false),
        column("yt_operation_id", DataType::Utf8, false),
        column("additional_properties", DataType::Utf8, false)
            .with_arrow_extension(ARROW_JSON_EXTENSION_NAME),
    ];
    columns.extend([
        primary_key("_system_topic", DataType::Utf8),
        primary_key("_system_partition", DataType::Int64),
        primary_key("_system_offset", DataType::Int64),
        primary_key("_system_message_index", DataType::UInt64),
    ]);
    DatasetSchema::new(columns)
}

fn column(name: &str, data_type: DataType, nullable: bool) -> SchemaColumn {
    SchemaColumn::new(name.to_owned(), data_type, nullable)
}

fn primary_key(name: &str, data_type: DataType) -> SchemaColumn {
    column(name, data_type, false).with_constraints(true, false, None)
}

fn transfer_log_arrays(start: u64, rows: usize) -> Vec<ArrayRef> {
    let indices = start..start + rows as u64;
    vec![
        strings(indices.clone(), |row| match row % 3 {
            0 => Some("pqv1/commit_latency_logger.go:80"),
            1 => Some("bufferer/bufferer.go:213"),
            _ => Some("stats/sink_wrapper.go:98"),
        }),
        integers(indices.clone(), |row| (row % 3 == 2).then_some(200)),
        strings(indices.clone(), |_| Some("logs.example.net")),
        strings(indices.clone(), |_| Some("ydb")),
        integers(indices.clone(), |row| (row % 3 == 2).then_some(200)),
        integers(indices.clone(), |row| {
            (row % 3 == 1).then_some(185_528 + row.cast_signed())
        }),
        strings(indices.clone(), |_| Some("worker-135.compute.example.net")),
        strings(indices.clone(), |_| Some("dtth5g2ssbu0jrr88fe9")),
        strings(indices.clone(), |_| {
            Some("191f7211-e42e2e24-3f60384-1001405")
        }),
        Arc::new(Int64Array::from_iter_values(indices.clone().map(|_| 1_i64))),
        Arc::new(
            indices
                .clone()
                .map(|row| (row % 3 == 2).then_some(0.456_616_41))
                .collect::<Float64Array>(),
        ),
        integers(indices.clone(), |row| (row % 3 == 1).then_some(200)),
        strings(indices.clone(), |_| Some("INFO")),
        strings(indices.clone(), |row| match row % 3 {
            0 => Some("Ack: [115113], durations: [199.030424ms]"),
            1 => Some("Flush is triggered by interval"),
            _ => Some(
                "Sink committed 200 row events in 103.061381ms with 238.60779ms - 456.60789ms lag",
            ),
        }),
        strings(indices.clone(), |_| Some("20729271")),
        strings(indices.clone(), |_| Some("yt")),
        strings(indices.clone(), |row| (row % 3 == 1).then_some("858 kB")),
        strings(indices.clone(), |_| Some("lf")),
        strings(indices.clone(), |row| (row % 3 == 1).then_some("333ms")),
        strings(indices.clone(), |_| Some("2026-08-22T08:29:37.215+0300")),
        strings(indices.clone(), |_| {
            Some("d94589b8-3c75384f-3f603e8-cb56576a")
        }),
        strings(indices.clone(), |_| Some("{}")),
        strings(indices.clone(), |_| Some("cdc/prod/logs")),
        Arc::new(Int64Array::from_iter_values(indices.clone().map(|_| 0_i64))),
        Arc::new(Int64Array::from_iter_values(
            indices.clone().map(u64::cast_signed),
        )),
        Arc::new(UInt64Array::from_iter_values(indices.map(|_| 0_u64))),
    ]
}

fn strings(rows: std::ops::Range<u64>, value: impl Fn(u64) -> Option<&'static str>) -> ArrayRef {
    Arc::new(rows.map(value).collect::<StringArray>())
}

fn integers(rows: std::ops::Range<u64>, value: impl Fn(u64) -> Option<i64>) -> ArrayRef {
    Arc::new(rows.map(value).collect::<Int64Array>())
}
