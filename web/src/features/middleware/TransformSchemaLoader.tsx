import { useLayoutEffect, useRef, useState } from "preact/hooks";
import { useControlPlane } from "../../bootstrap/ApplicationServicesProvider";
import { useSourceMetadataContext } from "../../delivery/sourceMetadata";
import type { TableIdentity, TransformPreviewSource } from "../../generated/apiContract";
import { Button } from "../../ui/Button";

export function TransformSchemaLoader({ tables, source, disabled }: {
  tables: TableIdentity[] | undefined; source: TransformPreviewSource | undefined; disabled: boolean;
}) {
  const api = useControlPlane();
  const metadata = useSourceMetadataContext();
  const cached = metadata?.metadata;
  const [pending, setPending] = useState(false);
  const [failure, setFailure] = useState<{ key: string; message: string }>();
  const active = useRef<AbortController>();
  const key = JSON.stringify([cached?.id, source, tables]);
  // Reset before the new catalog's button can be activated. A delayed reset
  // could abort that new request and allow a second click to send it again.
  useLayoutEffect(() => {
    active.current?.abort(); active.current = undefined; setPending(false);
    return () => { active.current?.abort(); };
  }, [key]);
  const loaded = new Set(cached?.loaded.map(table => JSON.stringify([table.namespace, table.name])));
  const errors = new Map(cached?.errors.map(error => [JSON.stringify([error.table.namespace, error.table.name]), error.message]));
  const ready = tables?.filter(table => loaded.has(JSON.stringify([table.namespace, table.name]))).length ?? 0;
  const tableError = tables?.map(table => errors.get(JSON.stringify([table.namespace, table.name]))).find(Boolean);
  const error = failure?.key === key ? failure.message : tableError;
  const manual = cached !== undefined && cached.catalog_count >= 1000;
  const load = async () => {
    if (!cached || !source || !tables || active.current || disabled) return;
    const request = new AbortController();
    active.current = request; setPending(true); setFailure(undefined);
    metadata?.updateMetadata({ ...cached, loading: true });
    try {
      const result = await api.loadMetadataSchemas(cached.id, { source, tables }, request.signal);
      if (!request.signal.aborted) metadata?.updateMetadata(result);
    } catch (reason) {
      if (!request.signal.aborted) setFailure({ key, message: reason instanceof Error ? reason.message : String(reason) });
    } finally {
      if (active.current === request) { active.current = undefined; setPending(false); }
      if (!request.signal.aborted) void api.metadataStatus(cached.id).then(metadata?.updateMetadata).catch(() => {});
    }
  };
  return <div class="transform-schema-loader">
    <span class={error ? "error" : "muted"} role="status" aria-live="polite" aria-atomic="true" title={error}>
      {error ? `Schema load failed: ${error}` : !cached || !tables ? "Load source metadata to inspect schemas"
        : `Schemas cached ${ready}/${tables.length}`}
    </span>
    <Button class="transform-load-schemas" pending={pending} disabled={disabled || !tables?.length || ready === tables.length || !!tableError}
      style={{ visibility: manual ? "visible" : "hidden" }}
      title={tableError ? "Refresh metadata in Source to retry failed schemas" : "Load schemas for this transform’s matched tables. No rows are read."}
      onClick={() => { void load(); }}>Load schemas</Button>
  </div>;
}
