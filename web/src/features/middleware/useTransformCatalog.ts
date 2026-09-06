import { useEffect, useMemo, useState } from "preact/hooks";
import type { ControlPlanePort } from "../../application/ports/controlPlane";
import { tableConnectionIdentity } from "../../delivery/useEndpointActions";
import type { TableIdentity, TableSelection, TransformPreviewSource } from "../../generated/apiContract";
import type { TableCatalog } from "../../schema/tableCatalog";
import { visibleTableCatalog } from "../tableSelection/catalog";

export interface VerifiedTableCatalog { identity: string; tables: TableIdentity[] }

export async function selectedSourceTables(source: TransformPreviewSource, tables: TableIdentity[],
  api: Pick<ControlPlanePort, "previewTables">, signal: AbortSignal): Promise<TableIdentity[]> {
  const visible = visibleTableCatalog(source.connector, source.config.hide_system_tables !== false, tables);
  // The endpoint validates the authored selection; this is not a client-side
  // validity assertion or a fallback for malformed configuration.
  const selection = source.config.tables as TableSelection;
  if (selection?.type === "all") return visible;
  const result = await api.previewTables({ catalog: visible, selection }, signal);
  if (result.issues.length) throw new Error("Correct the source table selection before choosing transform tables.");
  return result.cards.flatMap(card => card.selected);
}

export function useTransformCatalog(source: TransformPreviewSource | undefined,
  checked: VerifiedTableCatalog | undefined, api: ControlPlanePort): TableCatalog | undefined {
  const key = JSON.stringify(source ?? null);
  const identity = source ? tableConnectionIdentity(source.connector, source.config) : undefined;
  const tables = identity !== undefined && checked?.identity === identity ? checked.tables : undefined;
  const [result, setResult] = useState<{ key: string; catalog: TableIdentity[]; selected: TableIdentity[] }>();
  useEffect(() => {
    if (!source || !tables) return;
    const controller = new AbortController();
    void selectedSourceTables(source, tables, api, controller.signal).then(selected => {
      if (!controller.signal.aborted) setResult({ key, catalog: tables, selected });
    }).catch(() => {
      // Source settings own validation feedback. Never keep an old selection
      // available while the current one is invalid or still being resolved.
    });
    return () => controller.abort();
  }, [key, tables, api]);
  const selected = result?.key === key && result.catalog === tables ? result.selected : undefined;
  return useMemo(() => selected ? { tables: selected, preview: api.previewTables } : undefined, [selected, api.previewTables]);
}
