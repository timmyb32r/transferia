use std::collections::HashMap;

use arrow::array::{Array, BinaryArray, Int64Array, StringArray, UInt32Array};
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
    pub stored_columns: Vec<SchemaColumn>,
    source_versions: Vec<u64>,
    changed_columns: Vec<Vec<bool>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangelogAction {
    Upsert,
    Delete,
}

#[derive(Debug)]
pub struct ChangelogRun {
    pub action: ChangelogAction,
    pub operation: ChangeOperation,
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
        struct CollapsedRow {
            final_row: usize,
            operation: ChangeOperation,
            value_rows: Vec<Option<usize>>,
            source_version: u64,
        }

        let mut latest = HashMap::<Row<'_>, CollapsedRow>::with_capacity(self.rows.num_rows());
        for row in 0..self.rows.num_rows() {
            let operation = self.operations[row];
            let changed = &self.changed_columns[row];
            match latest.entry(keys.row(row)) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let value_rows = if operation == ChangeOperation::Delete {
                        (0..self.rows.num_columns())
                            .map(|index| self.primary_key_indexes.contains(&index).then_some(row))
                            .collect()
                    } else {
                        changed
                            .iter()
                            .map(|changed| changed.then_some(row))
                            .collect()
                    };
                    entry.insert(CollapsedRow {
                        final_row: row,
                        operation,
                        value_rows,
                        source_version: self.source_versions[row],
                    });
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let collapsed = entry.get_mut();
                    match operation {
                        ChangeOperation::Create | ChangeOperation::SnapshotRead => {
                            collapsed.operation = operation;
                            collapsed.value_rows.fill(Some(row));
                        }
                        ChangeOperation::Update => {
                            anyhow::ensure!(
                                collapsed.operation != ChangeOperation::Delete,
                                "changelog updates a deleted primary key without recreating it"
                            );
                            if !matches!(
                                collapsed.operation,
                                ChangeOperation::Create | ChangeOperation::SnapshotRead
                            ) {
                                collapsed.operation = ChangeOperation::Update;
                            }
                            for (source, changed) in
                                collapsed.value_rows.iter_mut().zip(changed)
                            {
                                if *changed {
                                    *source = Some(row);
                                }
                            }
                        }
                        ChangeOperation::Delete => {
                            collapsed.operation = ChangeOperation::Delete;
                            for (index, source) in collapsed.value_rows.iter_mut().enumerate() {
                                *source = self.primary_key_indexes.contains(&index).then_some(row);
                            }
                        }
                    }
                    collapsed.final_row = row;
                    collapsed.source_version = self.source_versions[row];
                }
            }
        }
        let mut selected = latest.into_values().collect::<Vec<_>>();
        selected.sort_unstable_by_key(|row| row.final_row);

        let mut runs = Vec::new();
        let mut start = 0;
        while start < selected.len() {
            let operation = selected[start].operation;
            let action = operation_action(operation);
            let included = selected[start]
                .value_rows
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>();
            let mut end = start + 1;
            while end < selected.len()
                && same_sink_operation(selected[end].operation, operation)
                && selected[end]
                    .value_rows
                    .iter()
                    .map(Option::is_some)
                    .eq(included.iter().copied())
            {
                end += 1;
            }
            let column_indexes = included
                .iter()
                .enumerate()
                .filter_map(|(index, included)| included.then_some(index))
                .collect::<Vec<_>>();
            let arrays = column_indexes
                .iter()
                .map(|&column_index| {
                    let indexes = selected[start..end]
                        .iter()
                        .map(|row| {
                            row.value_rows[column_index]
                                .ok_or_else(|| anyhow::anyhow!("collapsed changelog column source is missing"))
                                .and_then(|row| u32::try_from(row).map_err(Into::into))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    take(
                        self.rows.column(column_index).as_ref(),
                        &UInt32Array::from(indexes),
                        None,
                    )
                    .map_err(Into::into)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let fields = column_indexes
                .iter()
                .map(|&index| self.rows.schema().field(index).clone())
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
                arrays,
            )?;
            let source_versions = selected[start..end]
                .iter()
                .map(|row| row.source_version)
                .collect();
            runs.push(ChangelogRun {
                action,
                operation,
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
    let changed_columns = batch
        .system_columns
        .get(SystemColumnKind::ChangedColumns)
        .map(|column| {
            batch
                .batch
                .column(column.index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("changelog changed-columns column must be Arrow Binary")
                })
        })
        .transpose()?;
    let primary_key_indexes = dataset
        .stored_schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key.then_some(index))
        .collect::<Vec<_>>();
    let system_names = dataset
        .system_columns
        .iter()
        .map(|column| column.name.as_ref())
        .collect::<std::collections::HashSet<_>>();
    let user_column_names = dataset
        .incoming_schema
        .columns
        .iter()
        .filter(|column| !system_names.contains(column.name.as_str()))
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !primary_key_indexes.is_empty(),
        "changelog dataset '{}' requires at least one primary-key column",
        dataset.name
    );

    let mut parsed_operations = Vec::with_capacity(batch.rows());
    let mut parsed_source_versions = Vec::with_capacity(batch.rows());
    let mut parsed_changed_columns = Vec::with_capacity(batch.rows());
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
        let changed = match changed_columns {
            None => vec![true; stored.num_columns()],
            Some(changed_columns) => {
                anyhow::ensure!(
                    !changed_columns.is_null(row),
                    "changelog dataset '{}' row {row} has a null changed-columns mask",
                    dataset.name
                );
                let user_changed = decode_changed_columns(
                    changed_columns.value(row),
                    user_column_names.len(),
                    &dataset.name,
                    row,
                )?;
                dataset
                    .stored_schema
                    .columns
                    .iter()
                    .map(|column| {
                        user_column_names
                            .iter()
                            .position(|name| *name == column.name.as_str())
                            .map_or(true, |index| user_changed[index])
                    })
                    .collect()
            }
        };
        match operation {
            ChangeOperation::Create | ChangeOperation::SnapshotRead => anyhow::ensure!(
                changed.iter().all(|changed| *changed),
                "changelog dataset '{}' row {row} operation '{}' omits a column",
                dataset.name,
                operation.code()
            ),
            ChangeOperation::Update | ChangeOperation::Delete => {}
        }
        parsed_changed_columns.push(changed);
    }

    for &index in &primary_key_indexes {
        let column = stored.column(index);
        anyhow::ensure!(
            column.null_count() == 0,
            "changelog dataset '{}' has a null primary-key value in column '{}'",
            dataset.name,
            stored.schema().field(index).name()
        );
        for (row, changed) in parsed_changed_columns.iter().enumerate() {
            anyhow::ensure!(
                changed[index],
                "changelog dataset '{}' row {row} omits primary-key column '{}'",
                dataset.name,
                stored.schema().field(index).name()
            );
        }
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
        stored_columns: dataset.stored_schema.columns.clone(),
        source_versions: parsed_source_versions,
        changed_columns: parsed_changed_columns,
    }))
}

fn decode_changed_columns(
    mask: &[u8],
    columns: usize,
    dataset: &str,
    row: usize,
) -> anyhow::Result<Vec<bool>> {
    let expected_bytes = columns.div_ceil(8);
    anyhow::ensure!(
        mask.len() == expected_bytes,
        "changelog dataset '{dataset}' row {row} changed-columns mask has {} bytes, expected {expected_bytes}",
        mask.len()
    );
    if columns % 8 != 0 {
        let used = columns % 8;
        let unused = mask.last().copied().unwrap_or_default() & !((1_u8 << used) - 1);
        anyhow::ensure!(
            unused == 0,
            "changelog dataset '{dataset}' row {row} changed-columns mask sets unused bits"
        );
    }
    Ok((0..columns)
        .map(|column| mask[column / 8] & (1 << (column % 8)) != 0)
        .collect())
}

const fn operation_action(operation: ChangeOperation) -> ChangelogAction {
    if operation.writes_current_value() {
        ChangelogAction::Upsert
    } else {
        ChangelogAction::Delete
    }
}

const fn same_sink_operation(left: ChangeOperation, right: ChangeOperation) -> bool {
    matches!(
        (left, right),
        (
            ChangeOperation::Create | ChangeOperation::SnapshotRead,
            ChangeOperation::Create | ChangeOperation::SnapshotRead
        ) | (ChangeOperation::Update, ChangeOperation::Update)
            | (ChangeOperation::Delete, ChangeOperation::Delete)
    )
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
