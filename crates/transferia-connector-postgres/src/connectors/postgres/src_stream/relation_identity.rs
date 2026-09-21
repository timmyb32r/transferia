use std::collections::HashMap;

use tokio_postgres::error::SqlState;
use tokio_postgres::{GenericClient, types::Type};
use transferia_connector_support::external_request::observe_external_request;

use super::publication::replication_contract_violation;
use crate::connectors::postgres::common::quote_identifier;
use crate::connectors::postgres::source::DiscoveredTable;

const RELATION_IDENTITY_SQL: &str = "\
WITH expected AS (\
    SELECT configured_schema, configured_table, ordinality \
    FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.text[]), pg_catalog.unnest($2::pg_catalog.text[])) \
         WITH ORDINALITY \
         AS configured(configured_schema, configured_table, ordinality)\
) \
SELECT expected.configured_schema, expected.configured_table, current_table.oid, \
       current_table.relreplident::pg_catalog.text, \
       ARRAY( \
           SELECT attribute.attname::pg_catalog.text \
           FROM pg_catalog.pg_attribute AS attribute \
           WHERE attribute.attrelid OPERATOR(pg_catalog.=) current_table.oid \
             AND attribute.attnum OPERATOR(pg_catalog.>) 0 \
             AND NOT attribute.attisdropped \
           ORDER BY attribute.attnum \
       ), \
       ARRAY( \
           SELECT attribute.atttypid \
           FROM pg_catalog.pg_attribute AS attribute \
           WHERE attribute.attrelid OPERATOR(pg_catalog.=) current_table.oid \
             AND attribute.attnum OPERATOR(pg_catalog.>) 0 \
             AND NOT attribute.attisdropped \
           ORDER BY attribute.attnum \
       ), \
       ARRAY( \
           SELECT column_metadata.is_nullable OPERATOR(pg_catalog.=) 'YES' \
           FROM information_schema.columns AS column_metadata \
           WHERE column_metadata.table_schema OPERATOR(pg_catalog.=) expected.configured_schema \
             AND column_metadata.table_name OPERATOR(pg_catalog.=) expected.configured_table \
           ORDER BY column_metadata.ordinal_position \
       ), \
       ARRAY( \
           SELECT EXISTS ( \
               SELECT 1 \
               FROM pg_catalog.pg_index AS index_metadata \
               WHERE index_metadata.indrelid OPERATOR(pg_catalog.=) current_table.oid \
                 AND index_metadata.indisprimary \
                 AND attribute.attnum OPERATOR(pg_catalog.=) ANY(index_metadata.indkey) \
           ) \
           FROM pg_catalog.pg_attribute AS attribute \
           WHERE attribute.attrelid OPERATOR(pg_catalog.=) current_table.oid \
             AND attribute.attnum OPERATOR(pg_catalog.>) 0 \
             AND NOT attribute.attisdropped \
           ORDER BY attribute.attnum \
       ), \
       ARRAY( \
           SELECT (WITH RECURSIVE resolved(oid, modifier) AS ( \
               SELECT attribute.atttypid, attribute.atttypmod UNION ALL \
               SELECT t.typbasetype, CASE WHEN r.modifier OPERATOR(pg_catalog.>=) 0 THEN r.modifier ELSE t.typtypmod END \
               FROM resolved r JOIN pg_catalog.pg_type t ON t.oid OPERATOR(pg_catalog.=) r.oid WHERE t.typbasetype OPERATOR(pg_catalog.<>) 0 \
           ) SELECT modifier FROM resolved r JOIN pg_catalog.pg_type t ON t.oid OPERATOR(pg_catalog.=) r.oid WHERE t.typbasetype OPERATOR(pg_catalog.=) 0) \
           FROM pg_catalog.pg_attribute attribute \
           WHERE attribute.attrelid OPERATOR(pg_catalog.=) current_table.oid AND attribute.attnum OPERATOR(pg_catalog.>) 0 AND NOT attribute.attisdropped \
           ORDER BY attribute.attnum \
       ) \
FROM expected \
LEFT JOIN pg_catalog.pg_namespace AS current_namespace \
       ON current_namespace.nspname OPERATOR(pg_catalog.=) expected.configured_schema \
LEFT JOIN pg_catalog.pg_class AS current_table \
       ON current_table.relnamespace OPERATOR(pg_catalog.=) current_namespace.oid \
      AND current_table.relname OPERATOR(pg_catalog.=) expected.configured_table \
ORDER BY expected.ordinality";

#[derive(Clone, Debug, Eq, PartialEq)]
struct CurrentRelationIdentity {
    schema: String,

    name: String,

    relation_oid: Option<u32>,

    replica_identity: Option<String>,

    column_names: Vec<String>,

    type_oids: Vec<u32>,

    nullable: Vec<bool>,

    primary_key: Vec<bool>,

    type_modifiers: Vec<i32>,
}

pub async fn validate_relation_identities<C>(
    client: &C,
    tables: &[DiscoveredTable],
) -> anyhow::Result<()>
where
    C: GenericClient + Sync,
{
    let schemas = tables
        .iter()
        .map(|table| table.config.schema.as_str())
        .collect::<Vec<_>>();
    let names = tables
        .iter()
        .map(|table| table.config.name.as_str())
        .collect::<Vec<_>>();
    let rows = observe_external_request(
        "postgres",
        "validate_relation_identities",
        client.query_typed(RELATION_IDENTITY_SQL, &[(&schemas, Type::TEXT_ARRAY), (&names, Type::TEXT_ARRAY)]),
    )
    .await?;
    let current = rows
        .iter()
        .map(|row| {
            Ok(CurrentRelationIdentity {
                schema: row.try_get(0)?,
                name: row.try_get(1)?,
                relation_oid: row.try_get(2)?,
                replica_identity: row.try_get(3)?,
                column_names: row.try_get(4)?,
                type_oids: row.try_get(5)?,
                nullable: row.try_get(6)?,
                primary_key: row.try_get(7)?,
                type_modifiers: row.try_get(8)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(replication_contract_violation)?;
    validate_relation_identity_contract(tables, &current).map_err(replication_contract_violation)
}

pub async fn lock_and_validate_relation_identities<C>(
    client: &C,
    tables: &[DiscoveredTable],
) -> anyhow::Result<()>
where
    C: GenericClient + Sync,
{
    lock_authoritative_relations(client, tables).await?;
    validate_relation_identities(client, tables).await
}

pub async fn lock_authoritative_relations<C>(
    client: &C,
    tables: &[DiscoveredTable],
) -> anyhow::Result<()>
where
    C: GenericClient + Sync,
{
    let lock_sql = relation_lock_sql(tables).map_err(replication_contract_violation)?;
    let lock_result = observe_external_request(
        "postgres",
        "lock_authoritative_relations",
        client.batch_execute(&lock_sql),
    )
    .await;
    match lock_result {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.code(),
                Some(code)
                    if code == &SqlState::UNDEFINED_TABLE
                        || code == &SqlState::INVALID_SCHEMA_NAME
            ) =>
        {
            return Err(replication_contract_violation(anyhow::Error::new(error)));
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn relation_lock_sql(tables: &[DiscoveredTable]) -> anyhow::Result<String> {
    anyhow::ensure!(
        !tables.is_empty(),
        "PostgreSQL replication requires at least one authoritative table"
    );
    let relations = tables
        .iter()
        .map(|table| {
            format!(
                "{}.{}",
                quote_identifier(&table.config.schema),
                quote_identifier(&table.config.name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("LOCK TABLE {relations} IN ACCESS SHARE MODE"))
}

fn validate_relation_identity_contract(
    expected: &[DiscoveredTable],
    current: &[CurrentRelationIdentity],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        current.len() == expected.len(),
        "PostgreSQL relation identity query returned {} rows, expected {}",
        current.len(),
        expected.len()
    );
    let mut by_name = HashMap::with_capacity(current.len());
    for relation in current {
        anyhow::ensure!(
            by_name
                .insert((relation.schema.as_str(), relation.name.as_str()), relation)
                .is_none(),
            "PostgreSQL relation identity query repeated configured table '{}.{}'",
            relation.schema,
            relation.name
        );
    }
    for table in expected {
        let current = by_name
            .get(&(table.config.schema.as_str(), table.config.name.as_str()))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "PostgreSQL relation identity query omitted configured table '{}.{}'",
                    table.config.schema,
                    table.config.name
                )
            })?;
        anyhow::ensure!(
            current.relation_oid == Some(table.relation_oid),
            "PostgreSQL configured table '{}.{}' was removed or replaced after discovery (expected relation OID {}, found {:?})",
            table.config.schema,
            table.config.name,
            table.relation_oid,
            current.relation_oid
        );
        anyhow::ensure!(
            current.replica_identity.as_deref() == Some(table.replica_identity.as_str()),
            "PostgreSQL configured table '{}.{}' replica identity changed after discovery (expected '{}', found {:?})",
            table.config.schema,
            table.config.name,
            table.replica_identity,
            current.replica_identity
        );
        anyhow::ensure!(
            table.schema.columns.len() == table.type_oids.len(),
            "PostgreSQL discovery retained {} columns but {} type OIDs for configured table '{}.{}'",
            table.schema.columns.len(),
            table.type_oids.len(),
            table.config.schema,
            table.config.name
        );
        let expected_columns = table.schema.columns.len();
        anyhow::ensure!(
            current.column_names.len() == expected_columns
                && current.type_oids.len() == expected_columns
                && current.nullable.len() == expected_columns
                && current.primary_key.len() == expected_columns
                && current.type_modifiers.len() == expected_columns,
            "PostgreSQL configured table '{}.{}' schema changed after discovery (expected {expected_columns} columns; found {} names, {} type OIDs, {} nullability values, and {} primary-key values)",
            table.config.schema,
            table.config.name,
            current.column_names.len(),
            current.type_oids.len(),
            current.nullable.len(),
            current.primary_key.len()
        );
        for (index, (column, expected_oid)) in table
            .schema
            .columns
            .iter()
            .zip(&table.type_oids)
            .enumerate()
        {
            anyhow::ensure!(
                current.column_names[index] == column.name,
                "PostgreSQL configured table '{}.{}' column {index} name changed after discovery (expected '{}', found '{}')",
                table.config.schema,
                table.config.name,
                column.name,
                current.column_names[index]
            );
            anyhow::ensure!(
                current.type_oids[index] == *expected_oid,
                "PostgreSQL configured table '{}.{}' column '{}' type OID changed after discovery (expected {}, found {})",
                table.config.schema,
                table.config.name,
                column.name,
                expected_oid,
                current.type_oids[index]
            );
            if matches!(column.data_type, arrow::datatypes::DataType::Decimal128(..)) {
                anyhow::ensure!(crate::connectors::postgres::numeric::data_type(current.type_modifiers[index])? == column.data_type,
                    "PostgreSQL configured table '{}.{}' column '{}' NUMERIC precision/scale changed after discovery",
                    table.config.schema, table.config.name, column.name);
            }
            anyhow::ensure!(
                current.nullable[index] == column.nullable,
                "PostgreSQL configured table '{}.{}' column '{}' nullability changed after discovery (expected {}, found {})",
                table.config.schema,
                table.config.name,
                column.name,
                column.nullable,
                current.nullable[index]
            );
            anyhow::ensure!(
                current.primary_key[index] == column.primary_key,
                "PostgreSQL configured table '{}.{}' column '{}' primary-key membership changed after discovery (expected {}, found {})",
                table.config.schema,
                table.config.name,
                column.name,
                column.primary_key,
                current.primary_key[index]
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/relation_identity.rs"]
mod tests;
