import type { TableIdentity } from "../../generated/apiContract";
import type { JsonObject } from "../../json";

// Endpoint presentation filters do not change the authenticated connection.
// Keep connector-specific namespace policy here, out of the schema renderer.
export function tableConnectionConfig(connector: string, config: JsonObject): JsonObject | undefined {
  const { tables, ...connection } = config;
  if (tables === null || typeof tables !== "object" || Array.isArray(tables)
    || (tables.type !== "selected" && tables.type !== "all")) return undefined;
  if (connector === "clickhouse" || connector === "mysql") delete connection.hide_system_tables;
  return connection;
}

export function visibleTableCatalog(connector: string, hideSystemTables: boolean, tables: TableIdentity[]): TableIdentity[] {
  if (!hideSystemTables) return tables;
  switch (connector) {
    case "clickhouse":
      return tables.filter(({ namespace }) => !(
        namespace === "system" || namespace === "_system" || namespace === "INFORMATION_SCHEMA"
        || namespace.startsWith("information_schema")
      ));
    case "mysql":
      return tables.filter(({ namespace }) => !(
        namespace === "mysql" || namespace === "information_schema"
        || namespace === "performance_schema" || namespace === "sys"
      ));
    default:
      return tables;
  }
}
