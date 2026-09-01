use std::collections::HashMap;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, RowConverter, SortField};

use crate::data::change::ChangeOperation;
use crate::data::schema::SchemaColumn;
use crate::data::system_columns::SystemColumnKind;
use crate::delivery::{validate_batch_against_discovery, DeliveryDiscovery};
use crate::sink::SinkBatch;

#[derive(Debug)]
pub enum ProjectedSinkBatch {
    AppendOnly(RecordBatch),
    Changelog(ChangelogBatch),
}

#[derive(Debug)]
pub struct ChangelogBatch {
    rows: RecordBatch,
    operations: Vec<ChangeOperation>,
    primary_key_indexes: Vec<usize>,
    pub primary_keys: Vec<String>,
    pub primary_key_columns: Vec<SchemaColumn>,
    source_versions: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangelogAction {
    Upsert,
    Delete,
}

#[derive(Debug)]
pub struct ChangelogRun {
    pub action: ChangelogAction,
    pub batch: RecordBatch,
    pub source_versions: Vec<u64>,
}

impl ChangelogBatch {
    #[must_use]
    pub const fn rows(&self) -> &RecordBatch {
        &self.rows
    }

    #[must_use]
    pub fn operations(&self) -> &[ChangeOperation] {
        &self.operations
    }

    #[must_use]
    pub fn primary_key_indexes(&self) -> &[usize] {
        &self.primary_key_indexes
    }

    #[must_use]
    pub fn source_versions(&self) -> &[u64] {
        &self.source_versions
    }

    /// Collapse all changes of the same primary key to its last state, then
    /// return operation-homogeneous runs in original event order.
    ///
    /// A PostgreSQL transaction assigns one WAL LSN to all of its changes. A
    /// state sink must therefore settle same-key ordering before performing
    /// any side effect instead of relying on the source version to break ties.
    pub fn collapsed_runs(&self) -> anyhow::Result<Vec<ChangelogRun>> {
        let key_columns = self
            .primary_key_indexes
            .iter()
            .map(|&index| self.rows.column(index).clone())
            .collect::<Vec<_>>();
        let converter = RowConverter::new(
            key_columns
                .iter()
                .map(|column| SortField::new(column.data_type().clone()))
                .collect(),
        )?;
        let keys = converter.convert_columns(&key_columns)?;
        let mut latest = HashMap::<Row<'_>, usize>::with_capacity(self.rows.num_rows());
        for row in 0..self.rows.num_rows() {
            latest.insert(keys.row(row), row);
        }
        let mut selected = latest.into_values().collect::<Vec<_>>();
        selected.sort_unstable();

        let mut runs = Vec::new();
        let mut start = 0;
        while start < selected.len() {
            let action = operation_action(self.operations[selected[start]]);
            let mut end = start + 1;
            while end < selected.len()
                && operation_action(self.operations[selected[end]]) == action
            {
                end += 1;
            }
            let indexes = selected[start..end]
                .iter()
                .map(|&row| u32::try_from(row))
                .collect::<Result<Vec<_>, _>>()?;
            let indexes = arrow::array::UInt32Array::from(indexes);
            let arrays = self
                .rows
                .columns()
                .iter()
                .map(|column| take(column.as_ref(), &indexes, None))
                .collect::<Result<Vec<_>, _>>()?;
            let batch = RecordBatch::try_new(self.rows.schema(), arrays)?;
            let batch = if action == ChangelogAction::Delete {
                batch.project(&self.primary_key_indexes)?
            } else {
                batch
            };
            let source_versions = selected[start..end]
                .iter()
                .map(|&row| self.source_versions[row])
                .collect();
            runs.push(ChangelogRun {
                action,
                batch,
                source_versions,
            });
            start = end;
        }
        Ok(runs)
    }
}

pub fn project_sink_batch(
    discovery: &DeliveryDiscovery,
    batch: &SinkBatch,
) -> anyhow::Result<ProjectedSinkBatch> {
    let dataset = validate_batch_against_discovery(discovery, batch)?;
    let stored = project_columns(&batch.batch, &dataset.stored_schema.columns)?;
    let Some(operation) = batch
        .system_columns
        .get(SystemColumnKind::ChangeOperation)
    else {
        return Ok(ProjectedSinkBatch::AppendOnly(stored));
    };
    anyhow::ensure!(
        !dataset
            .stored_schema
            .columns
            .iter()
            .any(|column| column.name == operation.name.as_ref()),
        "changelog operation column '{}' is control metadata and cannot be stored as a user column",
        operation.name
    );
    let operations = batch
        .batch
        .column(operation.index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("changelog operation column must be Arrow Utf8"))?;
    let source_version = batch
        .system_columns
        .get(SystemColumnKind::Offset)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "changelog dataset '{}' has no source-version offset column",
                dataset.name
            )
        })?;
    let source_versions = batch
        .batch
        .column(source_version.index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("changelog source-version column must be Arrow Int64"))?;
    let primary_key_indexes = dataset
        .stored_schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key.then_some(index))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !primary_key_indexes.is_empty(),
        "changelog dataset '{}' requires at least one primary-key column",
        dataset.name
    );

    let mut parsed_operations = Vec::with_capacity(batch.rows());
    let mut parsed_source_versions = Vec::with_capacity(batch.rows());
    for row in 0..batch.rows() {
        anyhow::ensure!(
            !operations.is_null(row),
            "changelog dataset '{}' row {row} has a null operation",
            dataset.name
        );
        let code = operations.value(row);
        let operation = ChangeOperation::from_code(code).ok_or_else(|| {
            anyhow::anyhow!(
                "changelog dataset '{}' row {row} has unsupported operation '{code}'",
                dataset.name
            )
        })?;
        parsed_operations.push(operation);
        anyhow::ensure!(
            !source_versions.is_null(row) && source_versions.value(row) >= 0,
            "changelog dataset '{}' row {row} has an invalid source version",
            dataset.name
        );
        parsed_source_versions.push(u64::try_from(source_versions.value(row))?);
    }

    for &index in &primary_key_indexes {
        let column = stored.column(index);
        anyhow::ensure!(
            column.null_count() == 0,
            "changelog dataset '{}' has a null primary-key value in column '{}'",
            dataset.name,
            stored.schema().field(index).name()
        );
    }
    let primary_keys = primary_key_indexes
        .iter()
        .map(|&index| stored.schema().field(index).name().clone())
        .collect();
    let primary_key_columns = primary_key_indexes
        .iter()
        .map(|&index| dataset.stored_schema.columns[index].clone())
        .collect();
    Ok(ProjectedSinkBatch::Changelog(ChangelogBatch {
        rows: stored,
        operations: parsed_operations,
        primary_key_indexes,
        primary_keys,
        primary_key_columns,
        source_versions: parsed_source_versions,
    }))
}

const fn operation_action(operation: ChangeOperation) -> ChangelogAction {
    if operation.writes_current_value() {
        ChangelogAction::Upsert
    } else {
        ChangelogAction::Delete
    }
}

fn project_columns(
    batch: &RecordBatch,
    stored_columns: &[crate::data::schema::SchemaColumn],
) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let indexes = stored_columns
        .iter()
        .map(|column| {
            schema.index_of(&column.name).map_err(|_| {
                anyhow::anyhow!(
                    "stored column '{}' is absent from the incoming Arrow batch",
                    column.name
                )
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(batch.project(&indexes)?)
}

#[cfg(test)]
#[path = "../tests/changelog.rs"]
mod tests;
