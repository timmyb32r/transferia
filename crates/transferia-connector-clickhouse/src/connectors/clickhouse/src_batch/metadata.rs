use std::collections::BTreeMap;
use arrow::{array::{Array, StringArray}, compute::cast, datatypes::DataType, record_batch::RecordBatch};
use transferia_connector_support::external_request::observe_external_request;
use transferia_registry::TableIdentity;
use super::config::{TableConfig, UnsupportedTypePolicy};
use super::connector::{decode_table, validate_projection, DiscoveredTable};
use crate::connectors::clickhouse::sink::{client::ReconnectingClient, table::quote_string_literal};

pub(super) fn catalog_query(tables: &[TableIdentity]) -> String {
    let identities = tables.iter().map(|table| format!("({}, {})",
        quote_string_literal(&table.namespace), quote_string_literal(&table.name))).collect::<Vec<_>>().join(", ");
    format!("SELECT c.database, c.table, c.name, c.type, c.default_kind, t.primary_key, t.sorting_key \
        FROM system.columns AS c INNER JOIN system.tables AS t ON c.database = t.database AND c.table = t.name \
        WHERE (c.database, c.table) IN ({identities}) ORDER BY c.database, c.table, c.position")
}

pub(super) async fn discover_tables(client: &ReconnectingClient, tables: &[TableIdentity], policy: UnsupportedTypePolicy)
    -> anyhow::Result<BTreeMap<TableIdentity, anyhow::Result<DiscoveredTable>>> {
    if tables.is_empty() { return Ok(BTreeMap::new()); }
    let batches = observe_external_request("clickhouse", "load_schema_batch", client.query_all(&catalog_query(tables))).await?;
    let mut decoded = decode_batch(tables, batches, policy)?;
    // Catalog reads are batched; permission/function and exact wire-projection
    // checks remain table-specific, including system-table introspection rules.
    for discovered in decoded.values_mut() {
        if let Ok(table) = discovered {
            if let Err(error) = validate_projection(client, table).await { *discovered = Err(error); }
        }
    }
    Ok(decoded)
}

pub(super) fn decode_batch(tables: &[TableIdentity], batches: Vec<RecordBatch>, policy: UnsupportedTypePolicy)
    -> anyhow::Result<BTreeMap<TableIdentity, anyhow::Result<DiscoveredTable>>> {
    let mut groups = tables.iter().cloned().map(|table| (table, (Vec::new(), Vec::new()))).collect::<BTreeMap<_, _>>();
    for batch in batches {
        anyhow::ensure!(batch.num_columns() == 7, "ClickHouse metadata batch must contain seven columns");
        let databases = cast(batch.column(0), &DataType::Utf8)?;
        let names = cast(batch.column(1), &DataType::Utf8)?;
        let databases = databases.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow::anyhow!("Invalid metadata database names"))?;
        let names = names.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow::anyhow!("Invalid metadata table names"))?;
        let mut start = 0;
        while start < batch.num_rows() {
            anyhow::ensure!(!databases.is_null(start) && !names.is_null(start), "ClickHouse metadata table identity is NULL");
            let table = TableIdentity { namespace: databases.value(start).into(), name: names.value(start).into() };
            let mut end = start + 1;
            while end < batch.num_rows() && !databases.is_null(end) && !names.is_null(end)
                && databases.value(end) == table.namespace && names.value(end) == table.name { end += 1; }
            let (columns, keys) = groups.get_mut(&table).ok_or_else(|| anyhow::anyhow!("ClickHouse returned an unrequested table identity"))?;
            columns.push(batch.slice(start, end - start).project(&[2, 3, 4])?);
            if keys.is_empty() { keys.push(batch.slice(start, 1).project(&[5, 6])?); }
            start = end;
        }
    }
    Ok(groups.into_iter().map(|(table, (columns, keys))| {
        let result = decode_table(TableConfig { database: table.namespace.clone(), name: table.name.clone() }, columns, keys, policy);
        (table, result)
    }).collect())
}
