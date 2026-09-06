use std::collections::BTreeMap;
use mysql_async::{Conn, Params, Row, Value};
use mysql_async::prelude::Queryable;
use transferia_connector_support::external_request::observe_external_request;
use transferia_registry::TableIdentity;
use super::config::{MySqlReadProtocol, TableConfig};
use super::connector::{assemble_table, DiscoveredTable};

pub(super) fn catalog_query(count: usize, mysql8: bool) -> String {
    // Values are bound parameters, including names with quotes or wildcard
    // characters. Ordinals preserve exact requested identity under MySQL's
    // case-insensitive catalog comparisons.
    let requested = (0..count).map(|index| format!(
        "SELECT {index} AS request_index, ? AS requested_schema, ? AS requested_table"
    )).collect::<Vec<_>>().join(" UNION ALL ");
    let (srs, collation, padding) = if mysql8 { ("c.SRS_ID", "col.ID", "col.PAD_ATTRIBUTE") }
        else { ("NULL", "NULL", "NULL") };
    format!("SELECT r.request_index, t.ENGINE, t.TABLE_SCHEMA, t.TABLE_NAME, \
        c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, c.IS_NULLABLE, c.CHARACTER_SET_NAME, c.COLLATION_NAME, \
        {collation} AS COLLATION_ID, {padding} AS COLLATION_PADDING, c.EXTRA, c.GENERATION_EXPRESSION, \
        c.CHARACTER_MAXIMUM_LENGTH, c.CHARACTER_OCTET_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, \
        c.DATETIME_PRECISION, {srs} AS SRS_ID, s.SEQ_IN_INDEX, s.SUB_PART, s.COLLATION \
        FROM ({requested}) AS r \
        LEFT JOIN information_schema.TABLES AS t ON t.TABLE_SCHEMA = r.requested_schema AND t.TABLE_NAME = r.requested_table AND t.TABLE_TYPE = 'BASE TABLE' \
        LEFT JOIN information_schema.COLUMNS AS c ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME \
        LEFT JOIN information_schema.COLLATIONS AS col ON col.COLLATION_NAME = c.COLLATION_NAME \
        LEFT JOIN information_schema.STATISTICS AS s ON s.TABLE_SCHEMA = c.TABLE_SCHEMA AND s.TABLE_NAME = c.TABLE_NAME AND s.INDEX_NAME = 'PRIMARY' AND s.COLUMN_NAME = c.COLUMN_NAME \
        ORDER BY r.request_index, c.ORDINAL_POSITION")
}

pub(super) async fn discover_tables(connection: &mut Conn, tables: &[TableIdentity], replication: bool,
    protocol: MySqlReadProtocol) -> anyhow::Result<BTreeMap<TableIdentity, anyhow::Result<DiscoveredTable>>> {
    if tables.is_empty() { return Ok(BTreeMap::new()); }
    let mysql8 = connection.server_version().0 == 8;
    let parameters = tables.iter().flat_map(|table| [Value::from(table.namespace.clone()), Value::from(table.name.clone())]).collect();
    let rows: Vec<Row> = observe_external_request("mysql", "load_schema_batch",
        connection.exec(catalog_query(tables.len(), mysql8), Params::Positional(parameters))).await?;
    let mut groups = (0..tables.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    for row in rows {
        let index = row.get_opt::<u64, _>("request_index").ok_or_else(|| anyhow::anyhow!("MySQL metadata has no request ordinal"))??;
        let index = usize::try_from(index)?;
        let group = groups.get_mut(index).ok_or_else(|| anyhow::anyhow!("MySQL metadata returned an unknown request ordinal"))?;
        group.push(row);
    }
    Ok(tables.iter().zip(groups).map(|(table, rows)| {
        let result = decode_table(table, rows, mysql8, replication, protocol);
        (table.clone(), result)
    }).collect())
}

fn field<T: mysql_async::prelude::FromValue>(row: &Row, name: &str) -> anyhow::Result<T> {
    row.get_opt(name).ok_or_else(|| anyhow::anyhow!("MySQL metadata has no {name}"))?.map_err(Into::into)
}

fn decode_table(table: &TableIdentity, rows: Vec<Row>, mysql8: bool, replication: bool,
    protocol: MySqlReadProtocol) -> anyhow::Result<DiscoveredTable> {
    let first = rows.first().ok_or_else(|| anyhow::anyhow!("MySQL table '{}' returned no metadata", table.qualified_name()))?;
    let engine = field::<Option<String>>(first, "ENGINE")?.ok_or_else(|| anyhow::anyhow!(
        "MySQL table '{}' does not exist or is not a base table", table.qualified_name()))?;
    let mut names = std::collections::HashSet::new();
    for row in &rows {
        anyhow::ensure!(field::<String>(row, "TABLE_SCHEMA")? == table.namespace
            && field::<String>(row, "TABLE_NAME")? == table.name
            && field::<String>(row, "ENGINE")? == engine,
            "MySQL resolved configured table '{}' to different table metadata; exact identifier identity is required", table.qualified_name());
        let name = field::<Option<String>>(row, "COLUMN_NAME")?.ok_or_else(|| anyhow::anyhow!(
            "MySQL table '{}' does not exist or has no columns", table.qualified_name()))?;
        anyhow::ensure!(names.insert(name), "MySQL table '{}' returned duplicate column metadata", table.qualified_name());
    }
    assemble_table(&table.namespace, TableConfig { database: table.namespace.clone(), name: table.name.clone() },
        engine, rows, mysql8, replication, protocol)
}
