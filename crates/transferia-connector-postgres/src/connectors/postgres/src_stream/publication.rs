use std::collections::HashMap;

use tokio_postgres::GenericClient;
use transferia_connector_support::external_request::observe_external_request;

use crate::connectors::postgres::source::DiscoveredTable;

#[derive(Debug)]
struct ReplicationContractViolation {
    source: anyhow::Error,
}

impl std::fmt::Display for ReplicationContractViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.source)
    }
}

impl std::error::Error for ReplicationContractViolation {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

const PUBLICATION_CONTRACT_SQL: &str = "\
WITH expected AS (\
    SELECT configured_schema, configured_table, ordinality \
    FROM ROWS FROM (pg_catalog.unnest($2::text[]), pg_catalog.unnest($3::text[])) \
         WITH ORDINALITY \
         AS configured(configured_schema, configured_table, ordinality)\
), publication AS (\
    SELECT p.oid, \
           (pg_catalog.to_jsonb(p) ->> 'pubinsert')::boolean AS pubinsert, \
           (pg_catalog.to_jsonb(p) ->> 'pubupdate')::boolean AS pubupdate, \
           (pg_catalog.to_jsonb(p) ->> 'pubdelete')::boolean AS pubdelete, \
           (pg_catalog.to_jsonb(p) ->> 'pubtruncate')::boolean AS pubtruncate, \
           (pg_catalog.to_jsonb(p) ->> 'pubviaroot')::boolean AS pubviaroot \
    FROM pg_catalog.pg_publication AS p \
    WHERE p.pubname = $1\
), catalog_shape AS (\
    SELECT EXISTS (\
               SELECT 1 FROM pg_catalog.pg_attribute \
               WHERE attrelid = 'pg_catalog.pg_publication_tables'::pg_catalog.regclass \
                 AND attname = 'attnames' AND attnum > 0 AND NOT attisdropped\
           ) AS has_attnames, \
           EXISTS (\
               SELECT 1 FROM pg_catalog.pg_attribute \
               WHERE attrelid = 'pg_catalog.pg_publication_tables'::pg_catalog.regclass \
                 AND attname = 'rowfilter' AND attnum > 0 AND NOT attisdropped\
           ) AS has_rowfilter\
) \
SELECT publication.pubinsert, publication.pubupdate, publication.pubdelete, \
       publication.pubtruncate, publication.pubviaroot, \
       catalog_shape.has_attnames, catalog_shape.has_rowfilter, \
       expected.configured_schema, expected.configured_table, \
       current_table.oid, published_table.oid, \
       CASE WHEN catalog_shape.has_attnames THEN \
           pg_catalog.to_jsonb(publication_table) -> 'attnames' = (\
               SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(attribute.attname) \
                                      ORDER BY attribute.attnum), '[]'::jsonb) \
               FROM pg_catalog.pg_attribute AS attribute \
               WHERE attribute.attrelid = current_table.oid \
                 AND attribute.attnum > 0 AND NOT attribute.attisdropped\
           ) \
       ELSE true END AS publishes_all_columns, \
       (pg_catalog.to_jsonb(publication_table) ->> 'rowfilter') IS NULL AS has_no_row_filter \
FROM expected \
CROSS JOIN publication \
CROSS JOIN catalog_shape \
LEFT JOIN pg_catalog.pg_namespace AS current_namespace \
       ON current_namespace.nspname = expected.configured_schema \
LEFT JOIN pg_catalog.pg_class AS current_table \
       ON current_table.relnamespace = current_namespace.oid \
      AND current_table.relname = expected.configured_table \
LEFT JOIN pg_catalog.pg_publication_tables AS publication_table \
       ON publication_table.pubname = $1 \
      AND publication_table.schemaname = expected.configured_schema \
      AND publication_table.tablename = expected.configured_table \
LEFT JOIN pg_catalog.pg_namespace AS published_namespace \
       ON published_namespace.nspname = publication_table.schemaname \
LEFT JOIN pg_catalog.pg_class AS published_table \
       ON published_table.relnamespace = published_namespace.oid \
      AND published_table.relname = publication_table.tablename \
ORDER BY expected.ordinality";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PublicationActions {
    insert: Option<bool>,
    update: Option<bool>,
    delete: Option<bool>,
    truncate: Option<bool>,
    via_partition_root: Option<bool>,
    has_attnames: bool,
    has_rowfilter: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicationTable {
    schema: String,
    name: String,
    current_oid: Option<u32>,
    published_oid: Option<u32>,
    publishes_all_columns: Option<bool>,
    has_no_row_filter: bool,
}

pub(crate) async fn validate_pgoutput_publication<C>(
    client: &C,
    publication: &str,
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
        "validate_pgoutput_publication",
        client.query(PUBLICATION_CONTRACT_SQL, &[&publication, &schemas, &names]),
    )
    .await?;
    decode_and_validate_publication(publication, tables, &rows)
        .map_err(replication_contract_violation)
}

fn decode_and_validate_publication(
    publication: &str,
    tables: &[DiscoveredTable],
    rows: &[tokio_postgres::Row],
) -> anyhow::Result<()> {
    let first = rows
        .first()
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL publication '{publication}' does not exist"))?;
    let actions = PublicationActions {
        insert: first.try_get(0)?,
        update: first.try_get(1)?,
        delete: first.try_get(2)?,
        truncate: first.try_get(3)?,
        via_partition_root: first.try_get(4)?,
        has_attnames: first.try_get(5)?,
        has_rowfilter: first.try_get(6)?,
    };
    let published = rows
        .iter()
        .map(|row| {
            let current = PublicationActions {
                insert: row.try_get(0)?,
                update: row.try_get(1)?,
                delete: row.try_get(2)?,
                truncate: row.try_get(3)?,
                via_partition_root: row.try_get(4)?,
                has_attnames: row.try_get(5)?,
                has_rowfilter: row.try_get(6)?,
            };
            anyhow::ensure!(
                current == actions,
                "PostgreSQL publication '{publication}' catalog state is internally inconsistent"
            );
            Ok(PublicationTable {
                schema: row.try_get(7)?,
                name: row.try_get(8)?,
                current_oid: row.try_get(9)?,
                published_oid: row.try_get(10)?,
                publishes_all_columns: row.try_get(11)?,
                has_no_row_filter: row.try_get(12)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    validate_publication_contract(publication, actions, tables, &published)
}

pub(crate) fn is_replication_contract_violation(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<ReplicationContractViolation>()
        .is_some()
}

pub(super) fn replication_contract_violation(source: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(ReplicationContractViolation { source })
}

fn validate_publication_contract(
    publication: &str,
    actions: PublicationActions,
    expected: &[DiscoveredTable],
    published: &[PublicationTable],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        actions.insert == Some(true)
            && actions.update == Some(true)
            && actions.delete == Some(true),
        "PostgreSQL publication '{publication}' must publish INSERT, UPDATE, and DELETE"
    );
    anyhow::ensure!(
        !actions.truncate.unwrap_or(false),
        "PostgreSQL publication '{publication}' must not publish TRUNCATE because Transferia's row-change model cannot represent table-level truncation; create a row-DML-only publication"
    );
    anyhow::ensure!(
        !actions.via_partition_root.unwrap_or(false),
        "PostgreSQL publication '{publication}' must have publish_via_partition_root disabled so relation identity is preserved"
    );
    anyhow::ensure!(
        actions.has_attnames == actions.has_rowfilter,
        "PostgreSQL publication catalog has an unsupported pg_publication_tables shape"
    );
    anyhow::ensure!(
        published.len() == expected.len(),
        "PostgreSQL publication '{publication}' returned {} configured table rows, expected {}",
        published.len(),
        expected.len()
    );

    let mut by_name = HashMap::with_capacity(published.len());
    for table in published {
        anyhow::ensure!(
            by_name
                .insert((table.schema.as_str(), table.name.as_str()), table)
                .is_none(),
            "PostgreSQL publication '{publication}' repeats configured table '{}.{}'",
            table.schema,
            table.name
        );
    }
    for table in expected {
        let published = by_name
            .get(&(table.config.schema.as_str(), table.config.name.as_str()))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "PostgreSQL publication '{publication}' does not contain configured table '{}.{}'",
                    table.config.schema,
                    table.config.name
                )
            })?;
        anyhow::ensure!(
            published.current_oid == Some(table.relation_oid),
            "PostgreSQL configured table '{}.{}' was replaced after discovery (expected relation OID {}, found {:?})",
            table.config.schema,
            table.config.name,
            table.relation_oid,
            published.current_oid
        );
        anyhow::ensure!(
            published.published_oid == Some(table.relation_oid),
            "PostgreSQL publication '{publication}' does not contain the exact configured relation '{}.{}' with OID {}",
            table.config.schema,
            table.config.name,
            table.relation_oid
        );
        anyhow::ensure!(
            !actions.has_attnames || published.publishes_all_columns == Some(true),
            "PostgreSQL publication '{publication}' projects columns of configured table '{}.{}'; column projections are not supported",
            table.config.schema,
            table.config.name
        );
        anyhow::ensure!(
            !actions.has_rowfilter || published.has_no_row_filter,
            "PostgreSQL publication '{publication}' filters rows of configured table '{}.{}'; row filters are not supported",
            table.config.schema,
            table.config.name
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/publication.rs"]
mod tests;
