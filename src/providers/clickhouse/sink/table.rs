use std::collections::HashMap;
use std::str::FromStr as _;
use std::sync::Arc;

use arrow::array::{Array as _, StringArray};
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
    validate_target_schema(name, schema, &target)
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
    timezone: Option<String>,
}

async fn fetch_target_schema(
    client: &ReconnectingClient,
    database: &str,
    table: &str,
) -> anyhow::Result<HashMap<String, TargetColumn>> {
    let query = format!(
        "SELECT name, type FROM system.columns WHERE database = {} AND table = {} ORDER BY position",
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
            batch.num_columns() == 2,
            "ClickHouse schema query for '{table}' returned {} columns instead of 2",
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
        for row in 0..batch.num_rows() {
            anyhow::ensure!(
                !names.is_null(row) && !types.is_null(row),
                "ClickHouse schema query for '{table}' returned NULL metadata"
            );
            let name = names.value(row).to_string();
            let target = target_column(types.value(row)).map_err(|error| {
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

fn target_column(type_name: &str) -> anyhow::Result<TargetColumn> {
    let clickhouse_type = Type::from_str(type_name)?;
    let nullable = clickhouse_type.is_nullable();
    let data_type = target_data_type(&clickhouse_type);
    let (datetime_precision, timezone) = match clickhouse_type.strip_null() {
        Type::DateTime(timezone) => (Some(0), Some(timezone.name().to_string())),
        Type::DateTime64(precision, timezone) => {
            (Some(*precision), Some(timezone.name().to_string()))
        }
        _ => (None, None),
    };
    Ok(TargetColumn {
        data_type,
        nullable,
        datetime_precision,
        timezone,
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
    }
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
            target.datetime_precision == Some(3)
        }
        (DataType::Timestamp(expected_unit, expected_timezone), DataType::Timestamp(unit, _)) => {
            let expected_precision = match expected_unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            expected_unit == unit
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
        DataType::Date32 => "Date32".into(),
        DataType::Date64 => "DateTime64(3)".into(),
        DataType::Timestamp(unit, timezone) => {
            let timezone = timezone.as_deref().map(quote_string_literal);
            let precision = match unit {
                TimeUnit::Second => None,
                TimeUnit::Millisecond => Some(3),
                TimeUnit::Microsecond => Some(6),
                TimeUnit::Nanosecond => Some(9),
            };
            match (precision, timezone) {
                (None, None) => "DateTime".into(),
                (None, Some(timezone)) => format!("DateTime({timezone})"),
                (Some(precision), None) => format!("DateTime64({precision})"),
                (Some(precision), Some(timezone)) => {
                    format!("DateTime64({precision}, {timezone})")
                }
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
        validate_target_schema("events", &expected, &target)
    }

    #[test]
    fn target_schema_rejects_missing_type_and_nullability_mismatches() -> anyhow::Result<()> {
        let expected = schema(vec![SchemaColumn::new(
            "value".into(),
            DataType::Int64,
            true,
        )]);
        assert!(validate_target_schema("events", &expected, &HashMap::new()).is_err());

        let wrong_type = HashMap::from([("value".into(), target_column("String")?)]);
        assert!(validate_target_schema("events", &expected, &wrong_type).is_err());

        let non_nullable = HashMap::from([("value".into(), target_column("Int64")?)]);
        assert!(validate_target_schema("events", &expected, &non_nullable).is_err());

        let date_schema = schema(vec![SchemaColumn::new(
            "date".into(),
            DataType::Date32,
            false,
        )]);
        let narrow_date = HashMap::from([("date".into(), target_column("Date")?)]);
        assert!(validate_target_schema("events", &date_schema, &narrow_date).is_err());
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
        validate_target_schema("events", &expected, &matching)?;

        let wrong_precision = HashMap::from([(
            "ts".into(),
            target_column("DateTime64(3, 'Europe/Moscow')")?,
        )]);
        assert!(validate_target_schema("events", &expected, &wrong_precision).is_err());

        let wrong_timezone = HashMap::from([("ts".into(), target_column("DateTime64(6, 'UTC')")?)]);
        assert!(validate_target_schema("events", &expected, &wrong_timezone).is_err());
        Ok(())
    }
}
