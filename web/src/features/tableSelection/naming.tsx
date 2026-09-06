import { createContext, type ComponentChildren } from "preact";
import { useContext } from "preact/hooks";

type TableNamespace = "schema" | "database" | "namespace";
const TableNamespaceContext = createContext<TableNamespace>("namespace");

// Naming is known before connection checking. Keep it independent of the
// verified catalog, whose presence controls whether table editing is allowed.
export function TableNamingProvider({ connector, children }: {
  connector: string; children: ComponentChildren;
}) {
  const namespace = connector === "postgres" ? "schema"
    : connector === "mysql" || connector === "clickhouse" ? "database" : "namespace";
  return <TableNamespaceContext.Provider value={namespace}>{children}</TableNamespaceContext.Provider>;
}

export const useTableNamespace = () => useContext(TableNamespaceContext);
