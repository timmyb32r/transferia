use std::sync::Arc;

use arrow::datatypes::DataType;
use chrono::TimeZone as _;
use futures_util::future::BoxFuture;
use object_store::path::PathPart;

use transferia_core::data::schema::DatasetSchema;
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DatasetRole, DeliveryDiscovery, NameSyntax,
    ObjectKeyLimit, SinkLimits, SinkLimitsDescription, TextLimit,
};
use transferia_core::sink::Sink;
use transferia_registry::{SinkBuildContext, SinkConnector, SinkPrepare};

use super::actor::S3Sink;
use super::config::{PartitioningConfig, S3SinkConfig};
use super::object_key::{validate_path_component, ObjectKey, MAX_OBJECT_KEY_BYTES};
use super::upload::{ObjectUploader, S3Uploader};
use transferia_delivery_contracts::semantics::{EndpointDescriptor, S3Descriptor, S3Partitioning};

const REQUIRED_ROUTING_COLUMNS: [SystemColumnKind; 4] = [
    SystemColumnKind::Topic,
    SystemColumnKind::Partition,
    SystemColumnKind::Offset,
    SystemColumnKind::MessageIndex,
];

const fn s3_supports(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Boolean
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

const fn s3_partitioning_supports(data_type: &DataType) -> bool {
    s3_supports(data_type) && !matches!(data_type, DataType::Float32 | DataType::Float64)
}

fn validate_schema(dataset: &str, schema_kind: &str, schema: &DatasetSchema) -> anyhow::Result<()> {
    let mut names = std::collections::HashSet::with_capacity(schema.columns.len());
    for column in &schema.columns {
        anyhow::ensure!(
            !column.name.is_empty(),
            "S3 {schema_kind} schema for dataset '{dataset}' contains an empty column name"
        );
        anyhow::ensure!(
            names.insert(column.name.as_str()),
            "S3 {schema_kind} schema for dataset '{dataset}' repeats column '{}'",
            column.name,
        );
        anyhow::ensure!(
            s3_supports(&column.data_type),
            "S3 JSON serialization does not support column '{}' in dataset '{dataset}' with Arrow type {:?}",
            column.name,
            column.data_type,
        );
    }
    Ok(())
}

fn source_partition_path_probe(source_name: &str) -> anyhow::Result<String> {
    validate_path_component("source name", source_name)?;
    Ok(format!("topic={source_name}/partition={}", i64::MIN))
}

fn record_time_path_probe(path: &str, timezone: &str) -> anyhow::Result<String> {
    let timezone: chrono_tz::Tz = timezone
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid IANA timezone '{timezone}'"))?;
    // Include both ends of the commonly interoperable four-digit-year range:
    // directives such as `%s`, `%Y`, `%+` and timezone names can have
    // different widths at either side of the epoch.
    [-62_135_596_800_000_i64, 0, 253_402_300_799_999]
        .into_iter()
        .map(|millis| {
            let instant = chrono::Utc
                .timestamp_millis_opt(millis)
                .single()
                .ok_or_else(|| anyhow::anyhow!("record-time key probe is out of range"))?;
            let items = chrono::format::StrftimeItems::new(path)
                .parse()
                .map_err(|error| anyhow::anyhow!("invalid record-time path '{path}': {error}"))?;
            let mut rendered = String::new();
            instant
                .with_timezone(&timezone)
                .format_with_items(items.iter())
                .write_to(&mut rendered)
                .map_err(|_| anyhow::anyhow!("record-time path '{path}' could not be formatted"))?;
            super::partitioning::validate_partition_path(&rendered)?;
            Ok(rendered)
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .max_by_key(String::len)
        .ok_or_else(|| anyhow::anyhow!("record-time key probe produced no path"))
}

impl S3SinkConfig {
    fn main_partition_path_probe(&self) -> anyhow::Result<String> {
        match &self.partitioning {
            PartitioningConfig::Source => {
                anyhow::bail!("source partition path probe requires discovered source identity")
            }
            PartitioningConfig::Fields { columns } => Ok(columns
                .iter()
                .map(|column| {
                    validate_path_component("partition column name", column)?;
                    Ok(format!("{column}=value"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?
                .join("/")),
            PartitioningConfig::RecordTime { path, timezone, .. } => {
                record_time_path_probe(path, timezone)
            }
        }
    }

    fn validate_object_namespace(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        validate_path_component("source name", &discovery.source_name)?;
        let source_path = source_partition_path_probe(&discovery.source_name)?;
        let main_partition_path = match self.partitioning {
            PartitioningConfig::Source => source_path.clone(),
            _ => self.main_partition_path_probe()?,
        };
        for dataset in &discovery.datasets {
            let partition_path = if dataset.role == DatasetRole::Main {
                &main_partition_path
            } else {
                &source_path
            };
            ObjectKey::for_json_object(
                &self.prefix,
                &dataset.name,
                partition_path,
                &discovery.source_name,
                i64::MIN,
                i64::MIN,
            )
            .map_err(|error| {
                anyhow::anyhow!(
                    "S3 object namespace for dataset '{}' cannot satisfy the key contract: {error}",
                    dataset.name
                )
            })?;
        }
        Ok(())
    }
}

impl SinkLimits for S3SinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "s3",
            dataset_name: Some(TextLimit {
                syntax: NameSyntax::ObjectStorePathSegment,
                max_utf8_bytes: Some(MAX_OBJECT_KEY_BYTES),
            }),
            column_name: Some(TextLimit {
                syntax: NameSyntax::AnyNonEmptyUtf8,
                max_utf8_bytes: None,
            }),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Date32,
                ArrowTypeFamily::Date64,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: Some(ObjectKeyLimit {
                max_utf8_bytes: MAX_OBJECT_KEY_BYTES,
                normalized_relative_path: true,
            }),
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "S3 requires at least one dataset"
        );
        let mut dataset_names = std::collections::HashSet::new();
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                dataset_names.insert(dataset.name.as_ref()),
                "S3 datasets repeat object namespace '{}'",
                dataset.name
            );
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.name.is_empty(),
                "S3 dataset name must not be empty"
            );
            anyhow::ensure!(
                dataset.name.len() <= MAX_OBJECT_KEY_BYTES,
                "S3 dataset name '{}' is {} UTF-8 bytes, exceeding the {}-byte limit",
                dataset.name,
                dataset.name.len(),
                MAX_OBJECT_KEY_BYTES,
            );
            PathPart::parse(&dataset.name).map_err(|error| {
                anyhow::anyhow!(
                    "S3 dataset name '{}' is not a valid object-store path segment: {error}",
                    dataset.name,
                )
            })?;
            validate_schema(&dataset.name, "incoming", &dataset.incoming_schema)?;
            validate_schema(&dataset.name, "stored", &dataset.stored_schema)?;
            for required in REQUIRED_ROUTING_COLUMNS {
                anyhow::ensure!(
                    dataset
                        .system_columns
                        .iter()
                        .any(|column| column.kind == required),
                    "S3 dataset '{}' is missing required routing system column '{}'",
                    dataset.name,
                    required.default_name(),
                );
            }
        }

        match &self.partitioning {
            PartitioningConfig::Fields { columns } => {
                for main in discovery
                    .datasets
                    .iter()
                    .filter(|dataset| dataset.role == DatasetRole::Main)
                {
                    for name in columns {
                        let column = main
                        .incoming_schema
                        .columns
                        .iter()
                        .find(|column| column.name == *name)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "configured S3 partition column '{name}' is absent from discovered dataset '{}'",
                                main.name,
                            )
                        })?;
                        anyhow::ensure!(
                        !column.nullable,
                        "configured S3 partition column '{name}' in dataset '{}' must be non-nullable",
                        main.name,
                    );
                        anyhow::ensure!(
                        s3_partitioning_supports(&column.data_type),
                        "configured S3 partition column '{name}' in dataset '{}' has unsupported Arrow type {:?}",
                        main.name,
                        column.data_type,
                    );
                    }
                }
            }
            PartitioningConfig::RecordTime { .. } => {
                for main in discovery
                    .datasets
                    .iter()
                    .filter(|dataset| dataset.role == DatasetRole::Main)
                {
                    anyhow::ensure!(
                        main.system_columns
                            .iter()
                            .any(|column| column.kind == SystemColumnKind::WriteTimestampMs),
                        "S3 record-time partitioning requires system column '{}' in dataset '{}'",
                        SystemColumnKind::WriteTimestampMs.default_name(),
                        main.name
                    );
                }
            }
            PartitioningConfig::Source => {}
        }
        self.validate_object_namespace(discovery)
    }
}

pub struct S3SinkConnector {
    cfg: S3SinkConfig,
    uploader: Arc<dyn ObjectUploader>,
}

impl S3SinkConnector {
    pub fn from_config(cfg: S3SinkConfig) -> anyhow::Result<Self> {
        cfg.validate()?;
        let uploader = Arc::new(S3Uploader::new(cfg.build_store()?, cfg.upload.clone()));
        Ok(Self { cfg, uploader })
    }
}

impl SinkConnector for S3SinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        let partitioning = match &self.cfg.partitioning {
            super::config::PartitioningConfig::Source => S3Partitioning::Source,
            super::config::PartitioningConfig::Fields { columns } => {
                S3Partitioning::Fields(columns.clone())
            }
            super::config::PartitioningConfig::RecordTime { .. } => S3Partitioning::RecordTime,
        };
        EndpointDescriptor::S3(S3Descriptor {
            partitioning,
            record_time_rotation: self.cfg.rotation.record_time_interval.is_some(),
            wall_clock_rotation: self.cfg.rotation.wall_clock_interval.is_some(),
            object_layout_version: self.cfg.object_layout_version,
        })
    }

    fn limits(&self) -> &dyn SinkLimits {
        &self.cfg
    }

    fn destination_type(
        &self,
        column: &transferia_core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        crate::serializer::SerializerConfig::Json.destination_type(&column.data_type)
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        let prefix = self.cfg.prefix.clone();
        Box::pin(async move {
            for dataset in request.datasets.iter().map(|dataset| &dataset.table) {
                let candidate = if prefix.is_empty() {
                    format!("{dataset}/partition=0/probe+0+0.json")
                } else {
                    format!("{prefix}/{dataset}/partition=0/probe+0+0.json")
                };
                object_store::path::Path::parse(&candidate).map_err(|error| {
                    anyhow::anyhow!("invalid S3 object namespace for dataset '{dataset}': {error}")
                })?;
            }
            Ok(())
        })
    }

    fn validate_pipeline_memory_limit(&self, limit_bytes: usize) -> anyhow::Result<()> {
        let epoch_bytes = self.cfg.epoch_byte_limit();
        anyhow::ensure!(
            epoch_bytes <= limit_bytes,
            "effective s3.buffering.max_epoch_bytes ({epoch_bytes}) must not exceed \
             pipeline_memory_limit_bytes ({limit_bytes}); lower the S3 epoch limit or raise \
             the pipeline memory limit"
        );
        Ok(())
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        let sink = S3Sink::new(
            self.cfg.clone(),
            Arc::clone(&self.uploader),
            context.counters,
            context.keep_system_columns,
            context.partition_id,
            context.discovery,
            context.durable.storage,
        );
        Box::pin(async move { Ok(Box::new(sink?) as Box<dyn Sink>) })
    }
}

#[cfg(test)]
#[path = "tests/connector.rs"]
mod tests;
