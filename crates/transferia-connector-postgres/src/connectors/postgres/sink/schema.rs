//! Existing destination types are part of the COPY contract, not implicit casts.
//! Validate under a relation lock in the same transaction as the write. This
//! prevents DDL between validation and COPY and rejects text-to-numeric binary
//! reinterpretation and PostgreSQL typmod rounding before any data is written.
use arrow::datatypes::{DataType, TimeUnit};
use tokio_postgres::{types::Type, Transaction};

use crate::connectors::postgres::common::{arrow_to_postgres, quote_identifier};
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::schema::DatasetSchema;

pub(super) async fn validate_destination(
    transaction: &Transaction<'_>,
    table: &str,
    schema: &DatasetSchema,
) -> anyhow::Result<()> {
    let relation = quote_identifier(table);
    observe_external_request(
        "postgresql",
        "lock_copy_destination",
        transaction.batch_execute(&format!("LOCK TABLE {relation} IN ROW EXCLUSIVE MODE")),
    ).await?;
    let rows = observe_external_request(
        "postgresql",
        "validate_copy_destination",
        transaction.query_typed(
            "SELECT attname, atttypid, atttypmod, attnotnull, attgenerated::pg_catalog.text \
             FROM pg_catalog.pg_attribute \
             WHERE attrelid OPERATOR(pg_catalog.=) pg_catalog.to_regclass($1) AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped",
            &[(&relation, Type::TEXT)],
        ),
    ).await?;
    validate_columns(table, schema, &rows).map_err(transferia_core::failure::DataPlaneFailure::fatal)?;
    Ok(())
}

fn validate_columns(table: &str, schema: &DatasetSchema, rows: &[tokio_postgres::Row]) -> anyhow::Result<()> {
    let columns = rows.iter().map(|row| {
        Ok((row.try_get::<_, &str>(0)?, row))
    }).collect::<anyhow::Result<std::collections::BTreeMap<_, _>>>()?;
    for column in &schema.columns {
        let row = columns.get(column.name.as_str())
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL destination '{table}' is missing column '{}'", column.name))?;
        let oid: u32 = row.try_get(1)?;
        let typmod: i32 = row.try_get(2)?;
        let not_null: bool = row.try_get(3)?;
        let generated: &str = row.try_get(4)?;
        anyhow::ensure!(generated.is_empty(), "PostgreSQL destination '{table}.{}' is generated and cannot receive COPY data", column.name);
        anyhow::ensure!(!column.nullable || !not_null, "PostgreSQL destination '{table}.{}' is NOT NULL but its source schema permits NULL", column.name);
        validate_type(&column.data_type, oid, typmod).map_err(|error| {
            anyhow::anyhow!("PostgreSQL destination '{table}.{}' is incompatible with {:?}: {error}", column.name, column.data_type)
        })?;
    }
    Ok(())
}

/// The binary COPY representation must match exactly. A constrained numeric
/// destination must contain the complete declared source domain; an unbounded
/// NUMERIC is exact. Strings may target only unbounded text/varchar: fixed char
/// padding and bounded string conversions are not implicit transformations.
pub(super) fn validate_type(source: &DataType, oid: u32, typmod: i32) -> anyhow::Result<()> {
    let expected = arrow_to_postgres(source)?;
    if matches!(source, DataType::Utf8) {
        anyhow::ensure!(
            (oid == Type::TEXT.oid() || oid == Type::VARCHAR.oid()) && typmod == -1,
            "expected unbounded text or varchar, got type OID {oid} typmod {typmod}; explicit upstream conversion is required"
        );
        return Ok(());
    }
    anyhow::ensure!(oid == expected.oid(), "expected PostgreSQL {expected}, got type OID {oid}; explicit upstream conversion is required");
    match source {
        DataType::UInt64 | DataType::Decimal128(_, _) if typmod != -1 => {
            anyhow::ensure!(typmod >= 4, "invalid NUMERIC typmod {typmod}");
            let numeric = typmod - 4;
            let precision = (numeric >> 16) & 0xffff;
            let scale = ((numeric & 0x7ff) ^ 1024) - 1024;
            let (source_precision, source_scale) = match source {
                DataType::Decimal128(p, s) => (i32::from(*p), i32::from(*s)),
                _ => (20, 0),
            };
            anyhow::ensure!(
                scale >= source_scale && precision - scale >= source_precision - source_scale,
                "numeric({precision},{scale}) cannot losslessly represent numeric({source_precision},{source_scale})"
            );
        }
        DataType::Timestamp(unit, _) if typmod != -1 => {
            let needed = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond | TimeUnit::Nanosecond => 6,
            };
            anyhow::ensure!(typmod >= needed, "timestamp precision {typmod} may round source values (requires {needed})");
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
