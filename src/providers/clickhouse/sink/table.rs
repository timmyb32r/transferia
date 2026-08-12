use std::collections::{BTreeSet, HashMap};
use std::str::FromStr as _;
use std::sync::Arc;

use arrow::array::{Array as _, StringArray, UInt8Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use clickhouse_arrow::Type;

use super::client::{quote_identifier, ReconnectingClient};
use super::ClickHouseSinkConfig;
use crate::providers::traits::SinkPrepare;
use crate::types::schema::{DatasetSchema, SchemaColumn};

pub(super) async fn prepare_tables(
    client: &ReconnectingClient,
    config: &ClickHouseSinkConfig,
    request: &SinkPrepare,
) -> anyhow::Result<()> {
    for key in &config.sorting_key {
        anyhow::ensure!(
            request
                .schema
                .columns
                .iter()
                .any(|column| &column.name == key),
            "clickhouse.sorting_key column '{key}' is absent from the dataset schema"
        );
    }

    create_table(
        client,
        config,
        &request.table,
        &request.schema,
        &config.sorting_key,
    )
    .await?;
    create_table(client, config, &request.dlq_table, &request.dlq_schema, &[]).await
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
    validate_target_schema(name, schema, &target, sorting_key)
}

fn create_table_ddl(
    name: &str,
    schema: &DatasetSchema,
    sorting_key: &[String],
) -> anyhow::Result<String> {
    anyhow::ensure!(
        !schema.columns.is_empty(),
        "cannot create ClickHouse table '{name}' with an empty schema"
    );
    let columns = schema
        .columns
        .iter()
        .map(column_definition)
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
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
struct TargetColumn {
    data_type: Option<DataType>,
    nullable: bool,
    datetime_precision: Option<usize>,
    datetime64: bool,
    timezone: Option<String>,
    default_kind: String,
    in_sorting_key: bool,
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

#[cfg(test)]
fn target_column(type_name: &str) -> anyhow::Result<TargetColumn> {
    target_column_with_metadata(type_name, "", false)
}

fn target_column_with_metadata(
    type_name: &str,
    default_kind: &str,
    in_sorting_key: bool,
) -> anyhow::Result<TargetColumn> {
    let clickhouse_type = Type::from_str(type_name)?;
    let nullable = clickhouse_type.is_nullable();
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
    })
}

fn target_data_type(clickhouse_type: &Type) -> Option<DataType> {
    Some(match clickhouse_type.strip_null() {
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
        (DataType::Date64, DataType::Timestamp(TimeUnit::Millisecond, _)) => {
            target.datetime64 && target.datetime_precision == Some(3)
        }
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
    let data_type = clickhouse_type(&column.data_type)?;
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
        DataType::Date64 => "DateTime64(3)".into(),
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

fn quote_string_literal(value: &str) -> String {
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
mod tests {
    use std::sync::Arc;

    use super::*;

    fn schema(columns: Vec<SchemaColumn>) -> DatasetSchema {
        DatasetSchema::new(columns)
    }

    #[test]
    fn ddl_preserves_timestamp_timezone_and_quotes_it() -> anyhow::Result<()> {
        let schema = schema(vec![SchemaColumn::new(
            "created_at".into(),
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Europe/Moscow"))),
            true,
        )]);
        assert_eq!(
            create_table_ddl("events", &schema, &[])?,
            "CREATE TABLE IF NOT EXISTS `events` (`created_at` Nullable(DateTime64(6, 'Europe/Moscow'))) ENGINE = MergeTree ORDER BY (tuple())"
        );
        assert_eq!(quote_string_literal("db'\\name"), "'db\\'\\\\name'");
        Ok(())
    }

    #[test]
    fn target_schema_allows_extra_and_more_nullable_columns() -> anyhow::Result<()> {
        let expected = schema(vec![
            SchemaColumn::new("id".into(), DataType::Int64, false),
            SchemaColumn::new("name".into(), DataType::Utf8, true),
            SchemaColumn::new("enabled".into(), DataType::Boolean, false),
        ]);
        let target = HashMap::from([
            ("id".into(), target_column("Nullable(Int64)")?),
            ("name".into(), target_column("Nullable(String)")?),
            ("enabled".into(), target_column("Bool")?),
            ("extra".into(), target_column("String")?),
        ]);
        validate_target_schema("events", &expected, &target, &[])
    }

    #[test]
    fn target_schema_rejects_missing_type_and_nullability_mismatches() -> anyhow::Result<()> {
        let expected = schema(vec![SchemaColumn::new(
            "value".into(),
            DataType::Int64,
            true,
        )]);
        assert!(validate_target_schema("events", &expected, &HashMap::new(), &[]).is_err());

        let wrong_type = HashMap::from([("value".into(), target_column("String")?)]);
        assert!(validate_target_schema("events", &expected, &wrong_type, &[]).is_err());

        let non_nullable = HashMap::from([("value".into(), target_column("Int64")?)]);
        assert!(validate_target_schema("events", &expected, &non_nullable, &[]).is_err());

        let date_schema = schema(vec![SchemaColumn::new(
            "date".into(),
            DataType::Date32,
            false,
        )]);
        let narrow_date = HashMap::from([("date".into(), target_column("Date")?)]);
        assert!(validate_target_schema("events", &date_schema, &narrow_date, &[]).is_err());
        Ok(())
    }

    #[test]
    fn target_schema_checks_datetime_precision_and_timezone() -> anyhow::Result<()> {
        let expected = schema(vec![SchemaColumn::new(
            "ts".into(),
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Europe/Moscow"))),
            false,
        )]);
        let matching = HashMap::from([(
            "ts".into(),
            target_column("DateTime64(6, 'Europe/Moscow')")?,
        )]);
        validate_target_schema("events", &expected, &matching, &[])?;

        let wrong_precision = HashMap::from([(
            "ts".into(),
            target_column("DateTime64(3, 'Europe/Moscow')")?,
        )]);
        assert!(validate_target_schema("events", &expected, &wrong_precision, &[]).is_err());

        let wrong_timezone = HashMap::from([("ts".into(), target_column("DateTime64(6, 'UTC')")?)]);
        assert!(validate_target_schema("events", &expected, &wrong_timezone, &[]).is_err());
        Ok(())
    }

    #[test]
    fn seconds_use_signed_datetime64_and_reject_legacy_datetime() -> anyhow::Result<()> {
        let expected = schema(vec![SchemaColumn::new(
            "ts".into(),
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]);
        assert_eq!(
            create_table_ddl("events", &expected, &[])?,
            "CREATE TABLE IF NOT EXISTS `events` (`ts` DateTime64(0)) ENGINE = MergeTree ORDER BY (tuple())"
        );

        let signed = HashMap::from([("ts".into(), target_column("DateTime64(0)")?)]);
        validate_target_schema("events", &expected, &signed, &[])?;
        let unsigned = HashMap::from([("ts".into(), target_column("DateTime")?)]);
        assert!(validate_target_schema("events", &expected, &unsigned, &[]).is_err());
        Ok(())
    }

    #[test]
    fn date32_is_rejected_before_table_creation() {
        let date32 = schema(vec![SchemaColumn::new(
            "date".into(),
            DataType::Date32,
            false,
        )]);
        let error = create_table_ddl("events", &date32, &[]).unwrap_err();
        assert!(error.to_string().contains("shifts values by 25,567 days"));
    }

    #[test]
    fn date64_uses_datetime64_milliseconds() -> anyhow::Result<()> {
        let date64 = schema(vec![SchemaColumn::new(
            "date".into(),
            DataType::Date64,
            false,
        )]);
        assert_eq!(
            create_table_ddl("events", &date64, &[])?,
            "CREATE TABLE IF NOT EXISTS `events` (`date` DateTime64(3)) ENGINE = MergeTree ORDER BY (tuple())"
        );
        Ok(())
    }

    #[test]
    fn target_schema_rejects_generated_input_columns_and_wrong_sorting_set() -> anyhow::Result<()> {
        let expected = schema(vec![SchemaColumn::new("id".into(), DataType::Int64, false)]);
        let materialized = HashMap::from([(
            "id".into(),
            target_column_with_metadata("Int64", "MATERIALIZED", true)?,
        )]);
        assert!(
            validate_target_schema("events", &expected, &materialized, &["id".into()]).is_err()
        );

        let sorted =
            HashMap::from([("id".into(), target_column_with_metadata("Int64", "", true)?)]);
        validate_target_schema("events", &expected, &sorted, &["id".into()])?;
        assert!(validate_target_schema("events", &expected, &sorted, &[]).is_err());
        Ok(())
    }
}
