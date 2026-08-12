use std::collections::HashSet;
use std::sync::Arc;

use serde::Serialize;

use crate::parsers::ParserPlan;
use crate::pipeline::sink::SinkBatch;
use crate::types::schema::DatasetSchema;
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
    pub system_columns: Vec<SystemColumnKind>,
}

/// Source metadata and every dataset produced by its parser.
#[derive(Debug, Clone)]
pub struct DeliveryDiscovery {
    pub source_name: Arc<str>,
    pub source_partitions: Vec<i64>,
    pub schema_origin: SchemaOrigin,
    pub keep_system_columns: bool,
    pub datasets: Vec<DiscoveredDataset>,
}

impl DeliveryDiscovery {
    /// Build the authoritative sink-facing discovery for a raw source whose
    /// logical rows are defined by a parser plan.
    pub fn parser_projection(
        source_name: Arc<str>,
        source_partitions: Vec<i64>,
        parser_plan: &ParserPlan,
        request: DeliveryDiscoveryRequest,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !source_name.is_empty(),
            "discovered source name must not be empty"
        );
        anyhow::ensure!(
            !source_partitions.is_empty(),
            "discovered source partitions must not be empty"
        );
        let mut unique_partitions = HashSet::with_capacity(source_partitions.len());
        for partition in &source_partitions {
            anyhow::ensure!(
                *partition >= 0,
                "discovered source partition must be nonnegative, got {partition}"
            );
            anyhow::ensure!(
                unique_partitions.insert(*partition),
                "discovered source contains duplicate partition {partition}"
            );
        }

        let datasets = if parser_plan.parses_rows() {
            let system_columns = parser_plan.system_columns().enabled().collect::<Vec<_>>();
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
            source_partitions,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NameSyntax {
    AnyNonEmptyUtf8,
    AsciiIdentifier,
    ObjectStorePathSegment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TextLimit {
    pub syntax: NameSyntax,
    pub max_utf8_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrowTypeFamily {
    Utf8,
    SignedInteger,
    UnsignedInteger,
    FloatingPoint,
    Boolean,
    Date32,
    Date64,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObjectKeyLimit {
    pub max_utf8_bytes: usize,
    pub normalized_relative_path: bool,
}

/// Machine-readable summary of the restrictions enforced by one sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SinkLimitsDescription {
    pub sink: &'static str,
    pub dataset_name: Option<TextLimit>,
    pub column_name: Option<TextLimit>,
    pub supported_arrow_types: Vec<ArrowTypeFamily>,
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
            field.name() == column.kind.name() && field.data_type() == &column.kind.data_type(),
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
        .copied()
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
        .map(|kind| kind.name())
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        system_names.len() == dataset.system_columns.len(),
        "discovered {:?} dataset '{}' repeats a system column",
        dataset.role,
        dataset.name,
    );
    for kind in &dataset.system_columns {
        let matching = dataset
            .incoming_schema
            .columns
            .iter()
            .filter(|column| column.name == kind.name())
            .collect::<Vec<_>>();
        anyhow::ensure!(
            matching.len() == 1
                && matching[0].data_type == kind.data_type()
                && !matching[0].nullable,
            "discovered {:?} dataset '{}' system column '{}' must occur exactly once with Arrow type {:?} and be non-nullable",
            dataset.role,
            dataset.name,
            kind.name(),
            kind.data_type(),
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
                }),
        "stored schema for {:?} dataset '{}' is not the exact incoming schema after system-column projection",
        dataset.role,
        dataset.name,
    );
    Ok(())
}

#[cfg(test)]
mod tests;
