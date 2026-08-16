use std::collections::HashSet;
use std::sync::Arc;

use schemars::JsonSchema;
use serde::Serialize;

use crate::parsers::ParserPlan;
use crate::pipeline::sink::SinkBatch;
use crate::types::schema::{
    DatasetSchema, META_LOW_CARDINALITY, META_MAX_LENGTH, META_PRIMARY_KEY,
};
use crate::types::system_columns::SystemColumnKind;
use crate::types::table_data::dlq_name;

/// Inputs that affect the schema physically stored by a sink.
#[derive(Debug, Clone, Copy)]
pub struct DeliveryDiscoveryRequest {
    pub keep_system_columns: bool,
}

/// Semantic role of one parser output dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatasetRole {
    Main,
    DeadLetterQueue,
}

impl DatasetRole {
    #[must_use]
    pub const fn from_is_dlq(is_dlq: bool) -> Self {
        if is_dlq {
            Self::DeadLetterQueue
        } else {
            Self::Main
        }
    }
}

/// Where the logical row schema came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaOrigin {
    /// The source exposes a native typed schema.
    SourceNative,
    /// The source is raw bytes and the parser defines the logical schema.
    ParserProjection,
}

/// One dataset as seen at the sink boundary.
#[derive(Debug, Clone)]
pub struct DiscoveredDataset {
    pub role: DatasetRole,
    pub name: Arc<str>,
    /// Schema carried by [`SinkBatch`], including routing system columns.
    pub incoming_schema: DatasetSchema,
    /// Schema visible in the destination after the system-column policy.
    pub stored_schema: DatasetSchema,
    pub system_columns: Vec<DiscoveredSystemColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredSystemColumn {
    pub kind: SystemColumnKind,
    pub name: Arc<str>,
}

impl From<SystemColumnKind> for DiscoveredSystemColumn {
    fn from(kind: SystemColumnKind) -> Self {
        Self {
            kind,
            name: Arc::from(kind.default_name()),
        }
    }
}

impl PartialEq<SystemColumnKind> for DiscoveredSystemColumn {
    fn eq(&self, other: &SystemColumnKind) -> bool {
        self.kind == *other
    }
}

/// Source metadata and every dataset produced by its parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceTopology {
    StaticPartitions(Vec<i64>),
    DynamicWorkerLanes,
}

impl SourceTopology {
    pub fn validate(&self) -> anyhow::Result<()> {
        let Self::StaticPartitions(partitions) = self else {
            return Ok(());
        };
        anyhow::ensure!(!partitions.is_empty(), "source topology has no partitions");
        let mut unique = HashSet::with_capacity(partitions.len());
        for partition in partitions {
            anyhow::ensure!(
                *partition >= 0,
                "source partition must be nonnegative, got {partition}"
            );
            anyhow::ensure!(
                unique.insert(*partition),
                "source topology repeats partition {partition}"
            );
        }
        Ok(())
    }

    pub fn partitions_for_worker(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> anyhow::Result<Vec<i64>> {
        anyhow::ensure!(total_workers > 0, "total_workers must be positive");
        anyhow::ensure!(worker_index < total_workers, "worker_index out of range");
        self.validate()?;
        Ok(match self {
            Self::StaticPartitions(partitions) => partitions
                .iter()
                .copied()
                .filter(|partition| {
                    u64::try_from(*partition).is_ok_and(|partition| {
                        partition % u64::from(total_workers) == u64::from(worker_index)
                    })
                })
                .collect(),
            Self::DynamicWorkerLanes => vec![i64::from(worker_index)],
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeliveryDiscovery {
    pub source_name: Arc<str>,
    pub source_topology: SourceTopology,
    pub schema_origin: SchemaOrigin,
    pub keep_system_columns: bool,
    pub datasets: Vec<DiscoveredDataset>,
}

impl DeliveryDiscovery {
    /// Build the authoritative sink-facing discovery for a raw source whose
    /// logical rows are defined by a parser plan.
    pub fn parser_projection(
        source_name: Arc<str>,
        source_topology: SourceTopology,
        parser_plan: &ParserPlan,
        request: DeliveryDiscoveryRequest,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !source_name.is_empty(),
            "discovered source name must not be empty"
        );
        source_topology.validate()?;

        let datasets = if parser_plan.parses_rows() {
            let system_columns = parser_plan.system_columns();
            let table = parser_plan.table();
            let dlq_table: Arc<str> = dlq_name(&table).into();
            vec![
                DiscoveredDataset {
                    role: DatasetRole::Main,
                    name: table,
                    incoming_schema: parser_plan.sink_schema(true),
                    stored_schema: parser_plan.sink_schema(request.keep_system_columns),
                    system_columns: system_columns.clone(),
                },
                DiscoveredDataset {
                    role: DatasetRole::DeadLetterQueue,
                    name: dlq_table,
                    incoming_schema: parser_plan.dlq_schema(true),
                    stored_schema: parser_plan.dlq_schema(request.keep_system_columns),
                    system_columns,
                },
            ]
        } else {
            Vec::new()
        };

        Ok(Self {
            source_name,
            source_topology,
            schema_origin: SchemaOrigin::ParserProjection,
            keep_system_columns: request.keep_system_columns,
            datasets,
        })
    }

    pub fn dataset(&self, role: DatasetRole) -> anyhow::Result<&DiscoveredDataset> {
        let mut matches = self.datasets.iter().filter(|dataset| dataset.role == role);
        let dataset = matches
            .next()
            .ok_or_else(|| anyhow::anyhow!("delivery discovery has no {role:?} dataset"))?;
        anyhow::ensure!(
            matches.next().is_none(),
            "delivery discovery contains multiple {role:?} datasets"
        );
        Ok(dataset)
    }

    pub fn dataset_named(
        &self,
        role: DatasetRole,
        name: &str,
    ) -> anyhow::Result<&DiscoveredDataset> {
        self.datasets
            .iter()
            .find(|dataset| dataset.role == role && dataset.name.as_ref() == name)
            .ok_or_else(|| {
                anyhow::anyhow!("delivery discovery has no {role:?} dataset named '{name}'")
            })
    }
}

#[derive(Debug, Clone, Copy, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NameSyntax {
    AnyNonEmptyUtf8,
    AsciiIdentifier,
    ObjectStorePathSegment,
}

#[derive(Debug, Clone, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TextLimit {
    pub syntax: NameSyntax,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub max_utf8_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrowTypeFamily {
    Utf8,
    Binary,
    SignedInteger,
    UnsignedInteger,
    FloatingPoint,
    Boolean,
    Date32,
    Date64,
    Timestamp,
}

#[derive(Debug, Clone, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectKeyLimit {
    pub max_utf8_bytes: usize,
    pub normalized_relative_path: bool,
}

/// Machine-readable summary of the restrictions enforced by one sink.
#[derive(Debug, Clone, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SinkLimitsDescription {
    pub sink: &'static str,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub dataset_name: Option<TextLimit>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub column_name: Option<TextLimit>,

    pub supported_arrow_types: Vec<ArrowTypeFamily>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub object_key: Option<ObjectKeyLimit>,
}

/// Destination contract used for both startup discovery and runtime defense.
pub trait SinkLimits: Send + Sync {
    fn description(&self) -> SinkLimitsDescription;
    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()>;

    fn validate_batch(
        &self,
        discovery: &DeliveryDiscovery,
        batch: &SinkBatch,
    ) -> anyhow::Result<()> {
        validate_batch_against_discovery(discovery, batch).map(|_| ())
    }
}

/// Explicitly unrestricted contract for the benchmark-only discard sink.
pub struct NoLimits;

pub static NO_LIMITS: NoLimits = NoLimits;

impl SinkLimits for NoLimits {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "discard",
            dataset_name: None,
            column_name: None,
            supported_arrow_types: Vec::new(),
            object_key: None,
        }
    }

    fn validate_discovery(&self, _discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Validate the stable batch identity and Arrow schema shared by every sink.
pub fn validate_batch_against_discovery<'a>(
    discovery: &'a DeliveryDiscovery,
    batch: &SinkBatch,
) -> anyhow::Result<&'a DiscoveredDataset> {
    let role = DatasetRole::from_is_dlq(batch.is_dlq);
    let expected = discovery.dataset_named(role, &batch.table)?;
    anyhow::ensure!(
        batch.table.as_ref() == expected.name.as_ref(),
        "runtime {role:?} dataset name '{}' differs from discovered name '{}'",
        batch.table,
        expected.name,
    );

    let actual_schema = batch.batch.schema();
    anyhow::ensure!(
        actual_schema.fields().len() == expected.incoming_schema.columns.len(),
        "runtime dataset '{}' has {} columns, expected {}",
        batch.table,
        actual_schema.fields().len(),
        expected.incoming_schema.columns.len(),
    );
    for (position, (actual, expected_column)) in actual_schema
        .fields()
        .iter()
        .zip(&expected.incoming_schema.columns)
        .enumerate()
    {
        anyhow::ensure!(
            actual.name() == &expected_column.name
                && actual.data_type() == &expected_column.data_type
                && actual.is_nullable() == expected_column.nullable,
            "runtime dataset '{}' column {position} is '{}' ({:?}, nullable={}), expected '{}' ({:?}, nullable={})",
            batch.table,
            actual.name(),
            actual.data_type(),
            actual.is_nullable(),
            expected_column.name,
            expected_column.data_type,
            expected_column.nullable,
        );
        let metadata = actual.metadata();
        anyhow::ensure!(
            metadata
                .get(META_PRIMARY_KEY)
                .is_some_and(|value| value == "true")
                == expected_column.primary_key
                && metadata
                    .get(META_LOW_CARDINALITY)
                    .is_some_and(|value| value == "true")
                    == expected_column.low_cardinality
                && metadata.get(META_MAX_LENGTH).map(String::as_str)
                    == expected_column
                        .max_length
                        .as_ref()
                        .map(usize::to_string)
                        .as_deref(),
            "runtime dataset '{}' column '{}' metadata does not match discovery",
            batch.table,
            actual.name(),
        );
    }

    let mut actual_system_kinds = HashSet::with_capacity(batch.system_columns.iter().len());
    let mut actual_system_indexes = HashSet::with_capacity(batch.system_columns.iter().len());
    for column in batch.system_columns.iter() {
        anyhow::ensure!(
            actual_system_kinds.insert(column.kind),
            "runtime dataset '{}' repeats system column {:?}",
            batch.table,
            column.kind,
        );
        anyhow::ensure!(
            actual_system_indexes.insert(column.index),
            "runtime dataset '{}' maps multiple system columns to index {}",
            batch.table,
            column.index,
        );
        let field = actual_schema.fields().get(column.index).ok_or_else(|| {
            anyhow::anyhow!(
                "runtime dataset '{}' maps system column {:?} outside its schema",
                batch.table,
                column.kind,
            )
        })?;
        anyhow::ensure!(
            field.name() == column.name.as_ref() && field.data_type() == &column.kind.data_type(),
            "runtime dataset '{}' system column {:?} does not match field '{}' ({:?})",
            batch.table,
            column.kind,
            field.name(),
            field.data_type(),
        );
    }
    let expected_system_kinds = expected
        .system_columns
        .iter()
        .map(|column| column.kind)
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        actual_system_kinds == expected_system_kinds,
        "runtime dataset '{}' has system columns {actual_system_kinds:?}, expected {expected_system_kinds:?}",
        batch.table,
    );

    Ok(expected)
}

/// Validate the exact destination projection selected during discovery.
///
/// This prevents an incomplete contract from silently dropping or reordering
/// user columns.
pub fn validate_stored_projection(
    discovery: &DeliveryDiscovery,
    dataset: &DiscoveredDataset,
) -> anyhow::Result<()> {
    let system_names = dataset
        .system_columns
        .iter()
        .map(|column| column.name.as_ref())
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        system_names.len() == dataset.system_columns.len(),
        "discovered {:?} dataset '{}' repeats a system column",
        dataset.role,
        dataset.name,
    );
    for system in &dataset.system_columns {
        let matching = dataset
            .incoming_schema
            .columns
            .iter()
            .filter(|column| column.name == system.name.as_ref())
            .collect::<Vec<_>>();
        anyhow::ensure!(
            matching.len() == 1
                && matching[0].data_type == system.kind.data_type()
                && !matching[0].nullable,
            "discovered {:?} dataset '{}' system column '{}' must occur exactly once with Arrow type {:?} and be non-nullable",
            dataset.role,
            dataset.name,
            system.name,
            system.kind.data_type(),
        );
    }
    let expected = dataset
        .incoming_schema
        .columns
        .iter()
        .filter(|column| {
            discovery.keep_system_columns || !system_names.contains(column.name.as_str())
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        dataset.stored_schema.columns.len() == expected.len()
            && dataset
                .stored_schema
                .columns
                .iter()
                .zip(expected)
                .all(|(stored, incoming)| {
                    stored.name == incoming.name
                        && stored.data_type == incoming.data_type
                        && stored.nullable == incoming.nullable
                        && stored.primary_key == incoming.primary_key
                        && stored.low_cardinality == incoming.low_cardinality
                        && stored.max_length == incoming.max_length
                }),
        "stored schema for {:?} dataset '{}' is not the exact incoming schema after system-column projection",
        dataset.role,
        dataset.name,
    );
    Ok(())
}

#[cfg(test)]
#[path = "tests/delivery.rs"]
mod tests;
