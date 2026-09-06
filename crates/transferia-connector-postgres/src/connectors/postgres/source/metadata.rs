use std::collections::{BTreeMap, HashSet};
use tokio_postgres::{Client, Row, types::{Kind, Type}};
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::SchemaColumn;
use transferia_registry::TableIdentity;
use super::config::{TableConfig, UnsupportedTypePolicy};
use super::connector::{assemble_metadata_table, DiscoveredTable};
use crate::connectors::postgres::common::{quote_identifier, validate_identifier};

// One catalog snapshot for the complete requested set. LEFT JOINs retain missing
// tables/columns so they become explicit errors, not silently omitted datasets.
pub(super) const CATALOG_QUERY: &str = r#"
WITH RECURSIVE requested AS (
    SELECT * FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS r(namespace, name, request_ordinal)
), attributes AS (
    SELECT r.*, c.oid AS relation_oid, c.relreplident, a.attnum, a.attname, a.atttypid
    FROM requested r
    LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = r.namespace
    LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = r.name
    LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
), resolved_types AS (
    SELECT DISTINCT t.oid AS physical_oid, t.oid AS effective_oid, t.typbasetype, t.typname, t.typtype, t.typnamespace
    FROM attributes a JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    UNION
    SELECT r.physical_oid, t.oid, t.typbasetype, t.typname, t.typtype, t.typnamespace
    FROM resolved_types r JOIN pg_catalog.pg_type t ON t.oid = r.typbasetype WHERE r.typbasetype <> 0
)
SELECT a.request_ordinal, a.relation_oid, a.relreplident::text AS replica_identity,
    a.attnum, a.attname::text AS column_name, a.atttypid AS physical_oid,
    t.effective_oid, t.typname::text AS type_name, t.typtype::text AS type_kind, tn.nspname::text AS type_namespace,
    ic.is_nullable = 'YES' AS nullable,
    EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = a.relation_oid AND i.indisprimary AND a.attnum = ANY(i.indkey)) AS primary_key
FROM attributes a
LEFT JOIN resolved_types t ON t.physical_oid = a.atttypid AND t.typbasetype = 0
LEFT JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
LEFT JOIN information_schema.columns ic ON ic.table_schema = a.namespace AND ic.table_name = a.name AND ic.column_name = a.attname
ORDER BY a.request_ordinal, a.attnum
"#;

pub(super) async fn discover_tables(client: &Client, tables: &[TableIdentity], policy: UnsupportedTypePolicy)
    -> anyhow::Result<BTreeMap<TableIdentity, anyhow::Result<DiscoveredTable>>> {
    if tables.is_empty() { return Ok(BTreeMap::new()); }
    observe_external_request("postgres", "begin_metadata_batch",
        client.batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")).await?;
    let result = discover_in_transaction(client, tables, policy).await;
    let rollback = observe_external_request("postgres", "finish_metadata_batch", client.batch_execute("ROLLBACK")).await;
    // The caller only retains the connection after successful rollback.
    rollback?;
    result.map_err(|error| {
        let diagnostic = error.chain().find_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
            .and_then(tokio_postgres::Error::as_db_error)
            .map(|database| format!("{} (SQLSTATE {})", database.message(), database.code().code()))
            .unwrap_or_else(|| format!("{error:#}"));
        anyhow::anyhow!("PostgreSQL schema batch failed: {diagnostic}")
    })
}

async fn discover_in_transaction(client: &Client, tables: &[TableIdentity], policy: UnsupportedTypePolicy)
    -> anyhow::Result<BTreeMap<TableIdentity, anyhow::Result<DiscoveredTable>>> {
    let namespaces = tables.iter().map(|table| table.namespace.as_str()).collect::<Vec<_>>();
    let names = tables.iter().map(|table| table.name.as_str()).collect::<Vec<_>>();
    let rows = observe_external_request("postgres", "load_schema_batch", client.query(CATALOG_QUERY, &[&namespaces, &names])).await?;
    let mut groups = (0..tables.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    for row in rows {
        let ordinal: i64 = row.try_get("request_ordinal")?;
        let index = usize::try_from(ordinal)?.checked_sub(1).ok_or_else(|| anyhow::anyhow!("Invalid PostgreSQL metadata ordinal"))?;
        groups.get_mut(index).ok_or_else(|| anyhow::anyhow!("PostgreSQL metadata returned an unknown request ordinal"))?.push(row);
    }
    let mut results = BTreeMap::new();
    let mut projections = Vec::new();
    for (table, rows) in tables.iter().zip(groups) {
        let result = decode_table(table, &rows, policy).map(|(discovered, projection)| {
            projections.push((table.clone(), projection));
            discovered
        });
        results.insert(table.clone(), result);
    }
    if !projections.is_empty() {
        // Validate relation lookup and explicit casts in one Parse/Describe.
        // As in single-table discovery, execution still enforces SELECT ACLs.
        // Each inner projection has its own target list: 100 wide tables must
        // not accumulate into one PostgreSQL target-list width limit.
        let query = projection_query(&projections);
        drop(observe_external_request("postgres", "validate_schema_batch_projection", client.prepare(&query)).await?);
    }
    Ok(results)
}

pub(super) fn projection_query(projections: &[(TableIdentity, String)]) -> String {
    projections.iter().enumerate().map(|(index, (table, projection))| format!(
        "SELECT 1 FROM (SELECT {projection} FROM {}.{} LIMIT 0) AS metadata_{index}",
        quote_identifier(&table.namespace), quote_identifier(&table.name),
    )).collect::<Vec<_>>().join(" UNION ALL ")
}

pub(super) fn catalog_type(oid: u32, name: String, kind: &str, namespace: String) -> anyhow::Result<Type> {
    anyhow::ensure!(matches!(kind, "b" | "c" | "e" | "r" | "m" | "p"), "Unsupported PostgreSQL catalog type kind {kind:?} for '{name}'");
    if let Some(native) = Type::from_oid(oid) { return Ok(native); }
    // The existing source contract explicitly reads custom base/enum/array/
    // composite/range values as text. No nested layout is decoded here. Pseudo
    // types still go through the user's Fail/to_string policy.
    Ok(Type::new(name, oid, if kind == "p" { Kind::Pseudo } else { Kind::Simple }, namespace))
}

fn decode_table(table: &TableIdentity, rows: &[Row], policy: UnsupportedTypePolicy)
    -> anyhow::Result<(DiscoveredTable, String)> {
    let first = rows.first().ok_or_else(|| anyhow::anyhow!("PostgreSQL table '{}' returned no metadata", table.qualified_name()))?;
    let relation_oid: u32 = first.try_get::<_, Option<u32>>("relation_oid")?.ok_or_else(|| anyhow::anyhow!("PostgreSQL table '{}' does not exist", table.qualified_name()))?;
    let replica_identity: String = first.try_get("replica_identity")?;
    let mut columns = Vec::new();
    let mut type_oids = Vec::new();
    let mut expressions = Vec::new();
    let mut names = HashSet::new();
    let mut previous_position = 0;
    for row in rows {
        anyhow::ensure!(row.try_get::<_, u32>("relation_oid")? == relation_oid
            && row.try_get::<_, String>("replica_identity")? == replica_identity, "PostgreSQL table identity changed within metadata batch");
        let name = row.try_get::<_, Option<String>>("column_name")?.ok_or_else(|| anyhow::anyhow!("PostgreSQL table '{}' has no columns", table.qualified_name()))?;
        validate_identifier("column", &name)?;
        let position: i16 = row.try_get("attnum")?;
        anyhow::ensure!(position > previous_position && names.insert(name.clone()), "PostgreSQL table '{}' returned unordered or duplicate columns", table.qualified_name());
        previous_position = position;
        let nullable = row.try_get::<_, Option<bool>>("nullable")?.ok_or_else(|| anyhow::anyhow!("missing nullability metadata for column '{name}'"))?;
        // RowDescription unwraps domains, but CDC identity uses the physical OID.
        let data_type = catalog_type(row.try_get("effective_oid")?, row.try_get("type_name")?,
            &row.try_get::<_, String>("type_kind")?, row.try_get("type_namespace")?)?;
        let arrow_type = policy.arrow_type(&data_type).map_err(|error| anyhow::anyhow!("column '{name}' type '{data_type}': {error:#}"))?;
        expressions.push(crate::connectors::postgres::src_batch::source_column_expression(&name, &data_type, policy)?);
        columns.push(SchemaColumn::new(name, arrow_type, nullable).with_constraints(row.try_get("primary_key")?, false, None));
        type_oids.push(row.try_get("physical_oid")?);
    }
    let discovered = assemble_metadata_table(TableConfig { schema: table.namespace.clone(), name: table.name.clone() },
        columns, type_oids, replica_identity, relation_oid)?;
    Ok((discovered, expressions.join(", ")))
}
