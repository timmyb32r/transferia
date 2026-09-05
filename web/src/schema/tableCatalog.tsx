import { createContext } from "preact";
import { useContext } from "preact/hooks";
import type { ControlPlanePort } from "../application/ports/controlPlane";
import type { TableIdentity } from "../generated/apiContract";

export interface TableCatalog {
  tables: TableIdentity[];
  preview: ControlPlanePort["previewTables"];
}

export const TableCatalogContext = createContext<TableCatalog | undefined>(undefined);
export const useTableCatalog = () => useContext(TableCatalogContext);
