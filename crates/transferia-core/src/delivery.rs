use std::collections::HashSet;
use std::sync::Arc;

use schemars::JsonSchema;
use serde::Serialize;

use crate::data::schema::{
    DatasetSchema, SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME,
    META_LOW_CARDINALITY, META_MAX_LENGTH, META_OLD_KEY_OF, META_OLD_VALUE_OF, META_PRIMARY_KEY,
    META_SYSTEM_ROLE,
};
use crate::data::system_columns::SystemColumnKind;
use crate::sink::SinkBatch;

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
    CoLocatedStaticPartitions(Vec<i64>),
    DynamicWorkerLanes,
}

impl SourceTopology {
    #[must_use]
    pub fn static_partitions(&self) -> Option<&[i64]> {
        match self {
            Self::StaticPartitions(partitions) | Self::CoLocatedStaticPartitions(partitions) => {
                Some(partitions)
            }
            Self::DynamicWorkerLanes => None,
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let Some(partitions) = self.static_partitions() else {
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
            Self::CoLocatedStaticPartitions(partitions) => {
                if worker_index == 0 {
                    partitions.clone()
                } else {
                    Vec::new()
                }
            }
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
    pub performance_advice: Vec<PerformanceAdvice>,
}

impl DeliveryDiscovery {
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

/// Actionable connector guidance derived during source discovery.
///
/// Stable codes and structured fields let control planes render advice without
/// parsing connector log text.
#[derive(Debug, Clone, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceAdvice {
    pub code: String,

    pub severity: PerformanceAdviceSeverity,

    pub summary: String,

    pub explanation: String,

    pub remediation: String,

    pub config_paths: Vec<String>,
}

#[derive(Debug, Clone, Copy, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceAdviceSeverity {
    Info,
    Warning,
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
    Decimal,
    Boolean,
    Date32,
    Date64,
    Timestamp,
    Duration,
    FixedSizeBinary,
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
#[allow(
    clippy::too_many_lines,
    reason = "runtime validation keeps the complete pre-side-effect contract in one boundary"
)]
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
                        .as_deref()
                && metadata.get(META_OLD_VALUE_OF) == expected_column.old_value_of.as_ref()
                && metadata.get(META_OLD_KEY_OF) == expected_column.old_key_of.as_ref()
                && metadata.get(META_SYSTEM_ROLE) == expected_column.system_role.as_ref()
                && metadata.get(META_ARROW_EXTENSION_NAME).map(String::as_str)
                    == expected_column.arrow_extension_name
                && metadata.get(META_ARROW_EXTENSION_METADATA)
                    == expected_column.arrow_extension_metadata.as_ref(),
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
    let system_names = validate_projection_names_and_system_columns(dataset)?;
    let changelog_input = dataset
        .system_columns
        .iter()
        .any(|column| column.kind == SystemColumnKind::ChangeOperation);
    validate_cdc_control_columns(dataset, changelog_input, &system_names)?;
    let mut versions = dataset.incoming_schema.columns.iter().filter(|column| {
        column.system_role.as_deref() == Some(crate::data::schema::SYSTEM_ROLE_SOURCE_VERSION)
    });
    if let Some(column) = versions.next() {
        anyhow::ensure!(changelog_input && column.data_type == arrow::datatypes::DataType::UInt64 && !column.nullable,
            "dataset '{}' logical source version must be non-null UInt64 changelog control metadata", dataset.name);
    }
    anyhow::ensure!(
        versions.next().is_none(),
        "dataset '{}' has multiple logical source versions",
        dataset.name
    );
    let expected = dataset
        .incoming_schema
        .columns
        .iter()
        .filter(|column| {
            let system = dataset
                .system_columns
                .iter()
                .find(|system| system.name.as_ref() == column.name);
            column.old_value_of.is_none()
                && column.old_key_of.is_none()
                && column.system_role.is_none()
                && !matches!(
                    system,
                    Some(system)
                    if matches!(
                        system.kind,
                        SystemColumnKind::ChangeOperation | SystemColumnKind::ChangedColumns
                    )
                )
                && (discovery.keep_system_columns || !system_names.contains(column.name.as_str()))
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        dataset.stored_schema.columns.len() == expected.len()
            && dataset
                .stored_schema
                .columns
                .iter()
                .zip(expected)
                .all(stored_column_matches),
        "stored schema for {:?} dataset '{}' is not the exact incoming schema after system-column projection",
        dataset.role,
        dataset.name,
    );
    Ok(())
}

fn validate_projection_names_and_system_columns(
    dataset: &DiscoveredDataset,
) -> anyhow::Result<HashSet<&str>> {
    let incoming_names = dataset
        .incoming_schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        incoming_names.len() == dataset.incoming_schema.columns.len(),
        "discovered {:?} dataset '{}' repeats an incoming column name",
        dataset.role,
        dataset.name,
    );
    let stored_names = dataset
        .stored_schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        stored_names.len() == dataset.stored_schema.columns.len(),
        "discovered {:?} dataset '{}' repeats a stored column name",
        dataset.role,
        dataset.name,
    );
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
    Ok(system_names)
}

fn stored_column_matches((stored, incoming): (&SchemaColumn, &SchemaColumn)) -> bool {
    stored.name == incoming.name
        && stored.data_type == incoming.data_type
        // Incoming Arrow may be more permissive than the destination contract
        // (notably for unchanged TOAST values and snapshot/CDC schema parity).
        // Runtime projection rejects actual nulls before an append-only side
        // effect; changelog projection validates them against the changed mask.
        && (stored.nullable == incoming.nullable || (!stored.nullable && incoming.nullable))
        && stored.primary_key == incoming.primary_key
        && stored.low_cardinality == incoming.low_cardinality
        && stored.max_length == incoming.max_length
        && stored.arrow_extension_name == incoming.arrow_extension_name
        && stored.arrow_extension_metadata == incoming.arrow_extension_metadata
        && stored.old_value_of.is_none()
        && stored.old_key_of.is_none()
        && stored.system_role.is_none()
}

#[allow(
    clippy::too_many_lines,
    reason = "CDC metadata invariants are reviewed and enforced as one fail-closed contract"
)]
fn validate_cdc_control_columns(
    dataset: &DiscoveredDataset,
    changelog_input: bool,
    system_names: &HashSet<&str>,
) -> anyhow::Result<()> {
    let current = dataset
        .incoming_schema
        .columns
        .iter()
        .filter(|column| {
            column.old_value_of.is_none()
                && column.old_key_of.is_none()
                && column.system_role.is_none()
                && !system_names.contains(column.name.as_str())
        })
        .map(|column| (column.name.as_str(), column))
        .collect::<std::collections::HashMap<_, _>>();
    let old = dataset
        .incoming_schema
        .columns
        .iter()
        .filter_map(|column| column.old_value_of.as_deref().map(|name| (name, column)))
        .collect::<Vec<_>>();
    let has_old_values = !old.is_empty();
    if has_old_values {
        anyhow::ensure!(
            changelog_input,
            "discovered {:?} dataset '{}' declares old-value columns without changelog operations",
            dataset.role,
            dataset.name,
        );
        anyhow::ensure!(
            old.len() == current.len(),
            "discovered {:?} dataset '{}' must declare exactly one old-value column for every current-value column",
            dataset.role,
            dataset.name,
        );
        let mut paired = HashSet::with_capacity(old.len());
        for (current_name, old_column) in old {
            let current_column = current.get(current_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "discovered {:?} dataset '{}' old-value column '{}' references unknown current-value column '{current_name}'",
                    dataset.role,
                    dataset.name,
                    old_column.name,
                )
            })?;
            anyhow::ensure!(
                paired.insert(current_name),
                "discovered {:?} dataset '{}' repeats old-value mapping for '{current_name}'",
                dataset.role,
                dataset.name,
            );
            validate_cdc_control_column(dataset, old_column, current_name, current_column)?;
        }
        anyhow::ensure!(
            paired.len() == current.len(),
            "discovered {:?} dataset '{}' does not pair every current-value column with one old-value column",
            dataset.role,
            dataset.name,
        );
    }

    let old_keys = dataset
        .incoming_schema
        .columns
        .iter()
        .filter_map(|column| column.old_key_of.as_deref().map(|name| (name, column)))
        .collect::<Vec<_>>();
    if !old_keys.is_empty() {
        anyhow::ensure!(
            changelog_input && !has_old_values,
            "discovered {:?} dataset '{}' old-key columns require changelog operations and cannot be mixed with full old-value columns",
            dataset.role,
            dataset.name,
        );
        let primary_keys = current
            .values()
            .filter(|column| column.primary_key)
            .map(|column| column.name.as_str())
            .collect::<HashSet<_>>();
        anyhow::ensure!(
            old_keys.len() == primary_keys.len(),
            "discovered {:?} dataset '{}' must declare exactly one old-key column for every primary-key column",
            dataset.role,
            dataset.name,
        );
        let mut paired = HashSet::with_capacity(old_keys.len());
        for (current_name, old_column) in old_keys {
            let current_column = current.get(current_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "discovered {:?} dataset '{}' old-key column '{}' references unknown current-value column '{current_name}'",
                    dataset.role,
                    dataset.name,
                    old_column.name,
                )
            })?;
            anyhow::ensure!(
                current_column.primary_key && paired.insert(current_name),
                "discovered {:?} dataset '{}' old-key mapping for '{current_name}' must be unique and reference a primary-key column",
                dataset.role,
                dataset.name,
            );
            validate_cdc_control_column(dataset, old_column, current_name, current_column)?;
        }
        anyhow::ensure!(
            paired == primary_keys,
            "discovered {:?} dataset '{}' does not pair every primary-key column with one old-key column",
            dataset.role,
            dataset.name,
        );
    }
    Ok(())
}

fn validate_cdc_control_column(
    dataset: &DiscoveredDataset,
    control: &crate::SchemaColumn,
    current_name: &str,
    current: &crate::SchemaColumn,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        control.old_value_of.is_some() != control.old_key_of.is_some()
            && control.name != current_name
            && control.data_type == current.data_type
            && control.arrow_extension_name == current.arrow_extension_name
            && control.arrow_extension_metadata == current.arrow_extension_metadata
            && control.nullable
            && !control.primary_key
            && !control.low_cardinality
            && control.max_length.is_none()
            && control.system_role.is_none(),
        "discovered {:?} dataset '{}' CDC control column '{}' must have exactly one mapping, be nullable and unconstrained, and have the exact Arrow type of '{current_name}'",
        dataset.role,
        dataset.name,
        control.name,
    );
    Ok(())
}

#[cfg(test)]
#[path = "tests/delivery.rs"]
mod tests;
