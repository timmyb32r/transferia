use std::collections::{BTreeSet, HashMap};
use std::str::FromStr as _;
use std::sync::Arc;

use arrow::array::{Array as _, StringArray, UInt8Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use clickhouse_arrow::Type;

use super::client::{quote_identifier, ReconnectingClient};
use super::identifier::validate_identifier;
use super::ClickHouseSinkConfig;
use crate::providers::traits::SinkPrepare;
use crate::types::schema::{DatasetSchema, SchemaColumn};

/// Validate the part of a discovered dataset that is materialized in
/// `ClickHouse`. This is deliberately the same validation used by DDL
/// generation, so discovery cannot approve a schema that table preparation
/// will later reject.
pub(super) fn validate_table_schema(name: &str, schema: &DatasetSchema) -> anyhow::Result<()> {
    validated_column_definitions(name, schema).map(drop)
}

fn validated_column_definitions(name: &str, schema: &DatasetSchema) -> anyhow::Result<Vec<String>> {
    validate_identifier(name)
        .map_err(|error| error.context(format!("invalid ClickHouse table name {name:?}")))?;
    anyhow::ensure!(
        !schema.columns.is_empty(),
        "cannot create ClickHouse table '{name}' with an empty schema"
    );

    let mut names = BTreeSet::new();
    let mut definitions = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
        anyhow::ensure!(
            names.insert(column.name.as_str()),
            "ClickHouse table '{name}' contains duplicate column '{}'",
            column.name,
        );
        definitions.push(column_definition(column)?);
    }
    Ok(definitions)
}

pub(super) async fn prepare_tables(
    client: &ReconnectingClient,
    config: &ClickHouseSinkConfig,
    request: &SinkPrepare,
) -> anyhow::Result<()> {
    for dataset in &request.datasets {
        let schema_primary_key = dataset
            .schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let sorting_key: &[String] = if dataset.role == crate::delivery::DatasetRole::Main {
            anyhow::ensure!(
                config.sorting_key.is_empty() || config.sorting_key == schema_primary_key,
                "clickhouse.sorting_key must be empty or exactly match json_parser.primary_key {schema_primary_key:?}"
            );
            &schema_primary_key
        } else {
            &[]
        };
        for key in sorting_key {
            anyhow::ensure!(
                dataset
                    .schema
                    .columns
                    .iter()
                    .any(|column| &column.name == key),
                "clickhouse.sorting_key column '{key}' is absent from dataset '{}'",
                dataset.table,
            );
        }
        create_table(client, config, &dataset.table, &dataset.schema, sorting_key).await?;
    }
    Ok(())
}

async fn create_table(
    client: &ReconnectingClient,
    config: &ClickHouseSinkConfig,
    name: &str,
    schema: &DatasetSchema,
    sorting_key: &[String],
) -> anyhow::Result<()> {
    let ddl = create_table_ddl(name, schema, sorting_key)?;
    client
        .execute(&ddl)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to create table '{name}': {error}"))?;
    let target = fetch_target_schema(client, &config.database, name).await?;
    validate_target_schema(name, schema, &target, sorting_key)?;
    let metadata = fetch_target_table_metadata(client, &config.database, name).await?;
    validate_target_engine(name, &metadata.engine)?;
    validate_sorting_key(name, sorting_key, &metadata.sorting_key)
}

fn create_table_ddl(
    name: &str,
    schema: &DatasetSchema,
    sorting_key: &[String],
) -> anyhow::Result<String> {
    let columns = validated_column_definitions(name, schema)?.join(", ");
    let mut unique_sorting_keys = BTreeSet::new();
    for key in sorting_key {
        anyhow::ensure!(
            unique_sorting_keys.insert(key.as_str()),
            "clickhouse.sorting_key contains duplicate column '{key}'"
        );
    }
    let sorting_key = sorting_key
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let order_by = if sorting_key.is_empty() {
        "tuple()"
    } else {
        &sorting_key
    };
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({columns}) ENGINE = MergeTree ORDER BY ({order_by})",
        quote_identifier(name),
    ))
}

#[derive(Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "orthogonal ClickHouse column facts"
)]
struct TargetColumn {
    data_type: Option<DataType>,
    nullable: bool,
    datetime_precision: Option<usize>,
    datetime64: bool,
    timezone: Option<String>,
    default_kind: String,
    in_sorting_key: bool,
    low_cardinality: bool,
}

async fn fetch_target_schema(
    client: &ReconnectingClient,
    database: &str,
    table: &str,
) -> anyhow::Result<HashMap<String, TargetColumn>> {
    let query = format!(
        "SELECT name, type, default_kind, is_in_sorting_key FROM system.columns WHERE database = {} AND table = {} ORDER BY position",
        quote_string_literal(database),
        quote_string_literal(table),
    );
    let batches = client
        .query_all(&query)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to inspect table '{table}': {error}"))?;
    let mut columns = HashMap::new();
    for batch in batches {
        anyhow::ensure!(
            batch.num_columns() == 4,
            "ClickHouse schema query for '{table}' returned {} columns instead of 4",
            batch.num_columns()
        );
        let names = cast(batch.column(0), &DataType::Utf8)?;
        let names = names
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse schema column names are not strings"))?;
        let types = cast(batch.column(1), &DataType::Utf8)?;
        let types = types
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse schema types are not strings"))?;
        let default_kinds = cast(batch.column(2), &DataType::Utf8)?;
        let default_kinds = default_kinds
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse schema default kinds are not strings"))?;
        let sorting_keys = cast(batch.column(3), &DataType::UInt8)?;
        let sorting_keys = sorting_keys
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse schema sorting-key flags are not UInt8"))?;
        for row in 0..batch.num_rows() {
            anyhow::ensure!(
                !names.is_null(row)
                    && !types.is_null(row)
                    && !default_kinds.is_null(row)
                    && !sorting_keys.is_null(row),
                "ClickHouse schema query for '{table}' returned NULL metadata"
            );
            let name = names.value(row).to_string();
            let target = target_column_with_metadata(
                types.value(row),
                default_kinds.value(row),
                sorting_keys.value(row) != 0,
            )
            .map_err(|error| {
                error.context(format!(
                    "Failed to parse ClickHouse type for '{table}.{name}'"
                ))
            })?;
            anyhow::ensure!(
                columns.insert(name.clone(), target).is_none(),
                "ClickHouse schema for '{table}' contains duplicate column '{name}'"
            );
        }
    }
    anyhow::ensure!(
        !columns.is_empty(),
        "ClickHouse table '{table}' has no visible columns after CREATE TABLE"
    );
    Ok(columns)
}

struct TargetTableMetadata {
    engine: String,
    sorting_key: String,
}

async fn fetch_target_table_metadata(
    client: &ReconnectingClient,
    database: &str,
    table: &str,
) -> anyhow::Result<TargetTableMetadata> {
    let query = format!(
        "SELECT engine, sorting_key FROM system.tables WHERE database = {} AND name = {}",
        quote_string_literal(database),
        quote_string_literal(table),
    );
    let batches = client.query_all(&query).await.map_err(|error| {
        anyhow::anyhow!("Failed to inspect table metadata for '{table}': {error}")
    })?;
    let mut metadata = None;
    for batch in batches {
        anyhow::ensure!(
            batch.num_columns() == 2,
            "ClickHouse metadata query for '{table}' returned {} columns instead of 2",
            batch.num_columns()
        );
        let engines = cast(batch.column(0), &DataType::Utf8)?;
        let engines = engines
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse table engines are not strings"))?;
        let sorting_keys = cast(batch.column(1), &DataType::Utf8)?;
        let sorting_keys = sorting_keys
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse table sorting keys are not strings"))?;
        for row in 0..batch.num_rows() {
            anyhow::ensure!(
                !engines.is_null(row) && !sorting_keys.is_null(row),
                "ClickHouse metadata query for '{table}' returned NULL"
            );
            anyhow::ensure!(
                metadata
                    .replace(TargetTableMetadata {
                        engine: engines.value(row).to_string(),
                        sorting_key: sorting_keys.value(row).to_string(),
                    })
                    .is_none(),
                "ClickHouse metadata query for '{table}' returned multiple rows"
            );
        }
    }
    metadata.ok_or_else(|| anyhow::anyhow!("ClickHouse table '{table}' disappeared after CREATE"))
}

fn validate_target_engine(table: &str, engine: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(engine, "MergeTree" | "ReplicatedMergeTree"),
        "ClickHouse table '{table}' uses unsupported engine '{engine}'; expected exactly MergeTree or ReplicatedMergeTree"
    );
    Ok(())
}

fn validate_sorting_key(table: &str, expected: &[String], actual: &str) -> anyhow::Result<()> {
    if expected.is_empty() {
        anyhow::ensure!(
            matches!(actual, "" | "tuple()"),
            "ClickHouse table '{table}' has sorting key '{actual}', expected the canonical empty key"
        );
        return Ok(());
    }
    let body = actual
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(actual);
    let actual_columns = body
        .split(',')
        .map(|column| column.trim().trim_matches('`'))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        actual_columns == expected.iter().map(String::as_str).collect::<Vec<_>>(),
        "ClickHouse table '{table}' has sorting key '{actual}', expected plain columns in order {expected:?}"
    );
    Ok(())
}

fn target_column_with_metadata(
    type_name: &str,
    default_kind: &str,
    in_sorting_key: bool,
) -> anyhow::Result<TargetColumn> {
    let clickhouse_type = Type::from_str(type_name)?;
    let nullable = clickhouse_type.is_nullable();
    let low_cardinality = contains_low_cardinality(&clickhouse_type);
    let data_type = target_data_type(&clickhouse_type);
    let (datetime_precision, datetime64, timezone) = match clickhouse_type.strip_null() {
        Type::DateTime(timezone) => (Some(0), false, Some(timezone.name().to_string())),
        Type::DateTime64(precision, timezone) => {
            (Some(*precision), true, Some(timezone.name().to_string()))
        }
        _ => (None, false, None),
    };
    Ok(TargetColumn {
        data_type,
        nullable,
        datetime_precision,
        datetime64,
        timezone,
        default_kind: default_kind.to_string(),
        in_sorting_key,
        low_cardinality,
    })
}

fn contains_low_cardinality(clickhouse_type: &Type) -> bool {
    match clickhouse_type {
        Type::Nullable(inner) => contains_low_cardinality(inner),
        Type::LowCardinality(_) => true,
        _ => false,
    }
}

fn target_data_type(clickhouse_type: &Type) -> Option<DataType> {
    let mut clickhouse_type = clickhouse_type.strip_null();
    if let Type::LowCardinality(inner) = clickhouse_type {
        clickhouse_type = inner.strip_null();
    }
    Some(match clickhouse_type {
        Type::Int8 => DataType::Int8,
        Type::Int16 => DataType::Int16,
        Type::Int32 => DataType::Int32,
        Type::Int64 => DataType::Int64,
        Type::UInt8 => DataType::UInt8,
        Type::UInt16 => DataType::UInt16,
        Type::UInt32 => DataType::UInt32,
        Type::UInt64 => DataType::UInt64,
        Type::Float32 => DataType::Float32,
        Type::Float64 => DataType::Float64,
        Type::String => DataType::Utf8,
        // `Date` has a narrower range than Arrow `Date32`; accepting it could
        // make a previously valid input fail only after the pipeline starts.
        Type::Date32 => DataType::Date32,
        Type::DateTime(timezone) => {
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from(timezone.name())))
        }
        Type::DateTime64(precision, timezone) => DataType::Timestamp(
            match precision {
                0 => TimeUnit::Second,
                1..=3 => TimeUnit::Millisecond,
                4..=6 => TimeUnit::Microsecond,
                7..=9 => TimeUnit::Nanosecond,
                _ => return None,
            },
            Some(Arc::from(timezone.name())),
        ),
        _ => return None,
    })
}

fn validate_target_schema(
    table: &str,
    expected: &DatasetSchema,
    target: &HashMap<String, TargetColumn>,
    sorting_key: &[String],
) -> anyhow::Result<()> {
    for column in &expected.columns {
        let actual = target.get(&column.name).ok_or_else(|| {
            anyhow::anyhow!(
                "ClickHouse table '{table}' is missing required input column '{}'",
                column.name
            )
        })?;
        anyhow::ensure!(
            data_types_compatible(&column.data_type, actual),
            "ClickHouse table '{table}' column '{}' has incompatible type {:?}; expected {:?}",
            column.name,
            actual.data_type,
            column.data_type,
        );
        anyhow::ensure!(
            !column.nullable || actual.nullable,
            "ClickHouse table '{table}' column '{}' is non-nullable, but the input column is nullable",
            column.name,
        );
        anyhow::ensure!(
            column.low_cardinality == actual.low_cardinality,
            "ClickHouse table '{table}' column '{}' LowCardinality={} but discovery requires {}",
            column.name,
            actual.low_cardinality,
            column.low_cardinality,
        );
        anyhow::ensure!(
            matches!(actual.default_kind.as_str(), "" | "DEFAULT"),
            "ClickHouse table '{table}' column '{}' is not writable because default_kind is '{}'",
            column.name,
            actual.default_kind,
        );
    }
    // `system.columns` exposes sorting-key membership, not the canonical ORDER BY
    // expression. This validates the supported plain-column configuration without
    // pretending to compare expression text or key order.
    let expected_sorting: BTreeSet<_> = sorting_key.iter().map(String::as_str).collect();
    let actual_sorting: BTreeSet<_> = target
        .iter()
        .filter_map(|(name, column)| column.in_sorting_key.then_some(name.as_str()))
        .collect();
    anyhow::ensure!(
        actual_sorting == expected_sorting,
        "ClickHouse table '{table}' has sorting-key columns {actual_sorting:?}, expected {expected_sorting:?}",
    );
    Ok(())
}

fn data_types_compatible(expected: &DataType, target: &TargetColumn) -> bool {
    let Some(target_data_type) = &target.data_type else {
        return false;
    };
    match (expected, target_data_type) {
        (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8)
        | (DataType::Boolean, DataType::UInt8) => true,
        (DataType::Timestamp(expected_unit, expected_timezone), DataType::Timestamp(unit, _)) => {
            let expected_precision = match expected_unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            expected_unit == unit
                && target.datetime64
                && target.datetime_precision == Some(expected_precision)
                && expected_timezone
                    .as_deref()
                    .is_none_or(|expected| target.timezone.as_deref() == Some(expected))
        }
        _ => expected == target_data_type,
    }
}

fn column_definition(column: &SchemaColumn) -> anyhow::Result<String> {
    validate_identifier(&column.name).map_err(|error| {
        error.context(format!("invalid ClickHouse column name {:?}", column.name))
    })?;
    let data_type = clickhouse_type(&column.data_type)?;
    let data_type = if column.low_cardinality {
        anyhow::ensure!(
            matches!(column.data_type, DataType::Utf8 | DataType::LargeUtf8),
            "ClickHouse LowCardinality is supported only for string column '{}'",
            column.name
        );
        format!("LowCardinality({data_type})")
    } else {
        data_type
    };
    let data_type = if column.nullable {
        format!("Nullable({data_type})")
    } else {
        data_type
    };
    Ok(format!("{} {data_type}", quote_identifier(&column.name)))
}

fn clickhouse_type(data_type: &DataType) -> anyhow::Result<String> {
    Ok(match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => "String".into(),
        DataType::Int8 => "Int8".into(),
        DataType::Int16 => "Int16".into(),
        DataType::Int32 => "Int32".into(),
        DataType::Int64 => "Int64".into(),
        DataType::UInt8 => "UInt8".into(),
        DataType::UInt16 => "UInt16".into(),
        DataType::UInt32 => "UInt32".into(),
        DataType::UInt64 => "UInt64".into(),
        DataType::Float32 => "Float32".into(),
        DataType::Float64 => "Float64".into(),
        DataType::Boolean => "Bool".into(),
        DataType::Date32 => anyhow::bail!(
            "Arrow Date32 is unavailable for ClickHouse: clickhouse-arrow 0.2 shifts values by 25,567 days"
        ),
        DataType::Date64 => anyhow::bail!(
            "Arrow Date64 is unavailable for ClickHouse without an explicit configured conversion to Timestamp(Millisecond)"
        ),
        DataType::Timestamp(unit, timezone) => {
            let timezone = timezone.as_deref().map(quote_string_literal);
            let precision = match unit {
                TimeUnit::Second => Some(0),
                TimeUnit::Millisecond => Some(3),
                TimeUnit::Microsecond => Some(6),
                TimeUnit::Nanosecond => Some(9),
            };
            match (precision, timezone) {
                (Some(precision), None) => format!("DateTime64({precision})"),
                (Some(precision), Some(timezone)) => {
                    format!("DateTime64({precision}, {timezone})")
                }
                (None, _) => unreachable!("every Arrow timestamp unit has a precision"),
            }
        }
        other => anyhow::bail!("No ClickHouse type mapping for Arrow type {other:?}"),
    })
}

pub fn quote_string_literal(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for character in value.chars() {
        match character {
            '\'' | '\\' => {
                quoted.push('\\');
                quoted.push(character);
            }
            '\0' => quoted.push_str("\\0"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            _ => quoted.push(character),
        }
    }
    quoted.push('\'');
    quoted
}

#[cfg(test)]
mod tests;
