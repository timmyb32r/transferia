import { createPortal } from "preact/compat";
import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "preact/hooks";
import type { PatternMode, TableIdentity } from "../../generated/apiContract";
import { TableCatalogContext, useTableCatalog, type TableCatalog } from "../../schema/tableCatalog";
import { Button } from "../../ui/Button";
import { CopyButton, type CopyState } from "../../ui/CopyButton";
import { TablePatternInput } from "./TablePatternInput";
import { completionPattern, qualifiedName } from "./model";

export function AvailableTablesDialog({ catalog, onClose, onUse, showUse = onUse !== undefined }: {
  catalog: TableCatalog;
  onClose: () => void;
  onUse?: ((table: TableIdentity) => void) | undefined;
  showUse?: boolean;
}) {
  const id = useId();
  const dialog = useRef<HTMLElement>(null);
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<PatternMode>("glob");
  const [result, setResult] = useState<{ key: string; catalog: TableIdentity[]; tables?: TableIdentity[]; error?: string }>();
  const [copy, setCopy] = useState<{ name: string; state: CopyState }>();
  const copying = useRef(false);
  const key = JSON.stringify([query, mode]);
  useLayoutEffect(() => {
    const previous = document.activeElement;
    const overflow = document.documentElement.style.overflow;
    // Lock the root scroller, retaining its existing stable scrollbar gutter.
    // Locking body would create a second gutter and shift the page sideways.
    document.documentElement.style.overflow = "hidden";
    dialog.current?.querySelector<HTMLInputElement>("input")?.focus();
    return () => {
      document.documentElement.style.overflow = overflow;
      if (previous instanceof HTMLElement && previous.isConnected) previous.focus({ preventScroll: true });
    };
  }, []);
  useEffect(() => {
    if (!query) return;
    const controller = new AbortController();
    const timer = setTimeout(() => {
      void catalog.preview({ catalog: catalog.tables, selection: { type: "selected", rules: [{
        include: completionPattern(query, mode), include_mode: mode,
      }] } }, controller.signal).then(response => {
        if (!controller.signal.aborted) setResult({ key, catalog: catalog.tables, tables: response.cards[0]?.selected ?? [] });
      }).catch(error => {
        if (!controller.signal.aborted) setResult({ key, catalog: catalog.tables, error: error instanceof Error ? error.message : String(error) });
      });
    }, 150);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [key, catalog.tables, catalog.preview]);
  const current = result?.key === key && result.catalog === catalog.tables ? result : undefined;
  const tables = query ? current?.tables : catalog.tables;
  const schemaStates = useMemo(() => {
    const states = new Map<string, { label: string; error?: string }>();
    for (const table of catalog.metadata?.loaded ?? []) states.set(qualifiedName(table), { label: "Loaded" });
    for (const error of catalog.metadata?.errors ?? []) states.set(qualifiedName(error.table), { label: "Failed", error: error.message });
    return states;
  }, [catalog.metadata]);
  return createPortal(<div class="message-preview-backdrop" onMouseDown={event => {
    if (event.target === event.currentTarget) onClose();
  }}>
    <section class="available-tables-dialog" ref={dialog} role="dialog" aria-modal="true" aria-labelledby={`${id}-title`}
      onKeyDownCapture={event => {
        // Enter completes Include/Exclude by blurring, but a modal search must
        // retain focus so the next Tab cannot escape to the background page.
        if (event.key === "Enter" && event.target instanceof HTMLInputElement && !event.isComposing) {
          event.preventDefault(); event.stopPropagation();
        }
      }}
      onKeyDown={event => {
        if (event.key === "Escape") { event.preventDefault(); event.stopPropagation(); onClose(); }
        if (event.key !== "Tab") return;
        const controls = [...(dialog.current?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), [tabindex="0"]') ?? [])];
        const first = controls[0], last = controls[controls.length - 1];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
      }}>
      <header><h2 id={`${id}-title`}>Available tables <span class="table-match-count" aria-hidden="true">({catalog.tables.length})</span></h2>
        <Button shape="icon" aria-label="Close available tables" onClick={onClose}>×</Button>
      </header>
      <TableCatalogContext.Provider value={undefined}>
        <TablePatternInput id={`${id}-search`} label="Search tables" value={query} mode={mode} disabled={false}
          required={false} invalid={Boolean(current?.error)}
          onChange={value => { setQuery(value); if (!copying.current) setCopy(undefined); }}
          onModeChange={value => { setMode(value); if (!copying.current) setCopy(undefined); }}
          placeholder="Search tables · * and ? supported" />
      </TableCatalogContext.Provider>
      <div class="available-tables-status" role="status" aria-live="polite" title={catalog.metadataError}>
        {catalog.metadataError ?? current?.error ?? (!tables ? "Searching…" : copy?.state === "error" ? `Could not copy ${copy.name}.` : copy?.state === "copied" ? `Copied ${copy.name}`
          : `${tables.length} tables`)}
      </div>
      <div class="available-tables-list" role="region" aria-label="Available table names" aria-busy={!tables && !current?.error}>
        {tables?.map(table => {
          const name = qualifiedName(table);
          const schema = schemaStates.get(name);
          return <div class="available-table-row" key={JSON.stringify(table)}>
            <span title={name}>{name}</span>
            {catalog.metadata && <span class={`available-table-schema${schema?.error ? " has-error" : ""}`}
              tabIndex={schema?.error ? 0 : undefined} title={schema?.error ?? schema?.label ?? "Not loaded"}
              aria-label={schema?.error ? `Schema failed for ${name}: ${schema.error}` : `Schema ${schema?.label ?? "Not loaded"} for ${name}`}>
              {schema?.label ?? "Not loaded"}
            </span>}
            <div class="available-table-actions">
              <CopyButton text={name} label={`Copy ${name}`} framed lock={copying}
                disabled={copy?.state === "copying" && copy.name !== name}
                onStateChange={state => setCopy(current => state === "idle" ? current?.name === name ? undefined : current : { name, state })} />
              {showUse && <Button class="available-table-use" aria-label={`Use ${name} in Include`} disabled={!onUse}
                title={onUse ? "Use this table in Include and close" : "Enter edit mode to use this table"}
                onClick={() => { onUse?.(table); onClose(); }}>Use</Button>}
            </div>
          </div>;
        })}
        {tables?.length === 0 && <p>No matching tables.</p>}
      </div>
    </section>
  </div>, document.body);
}

export function AvailableTablesButton({ label, title, onUse, showUse = false, showMetadata = false }: {
  label: string; title: string;
  onUse?: ((table: TableIdentity) => void) | undefined;
  showUse?: boolean;
  showMetadata?: boolean;
}) {
  const catalog = useTableCatalog();
  const [open, setOpen] = useState(false);
  useLayoutEffect(() => { if (!catalog) setOpen(false); }, [catalog]);
  const summary = useMemo(() => {
    if (!catalog?.metadata) return undefined;
    const visible = new Set(catalog.tables.map(qualifiedName));
    return { loaded: catalog.metadata.loaded.filter(table => visible.has(qualifiedName(table))).length,
      failed: catalog.metadata.errors.filter(error => visible.has(qualifiedName(error.table))).length };
  }, [catalog?.tables, catalog?.metadata]);
  return <div class={`available-tables-action${showMetadata ? " available-tables-metadata" : ""}`}>
    <Button class="table-matches-height-toggle" aria-label={label} aria-haspopup="dialog" disabled={!catalog}
      title={catalog ? title : "Connect & load metadata in Source first"} onClick={() => setOpen(true)}>
      <span class="available-tables-label">Available tables <span class="table-match-count">({catalog?.tables.length ?? "—"})</span></span>
      {showMetadata && <span class="available-tables-summary" aria-live="polite">
        {catalog?.metadataError ? <span class="has-error" title={catalog.metadataError}>Metadata unavailable</span> : summary
          ? <>Schemas loaded {summary.loaded}/{catalog!.tables.length}{summary.failed > 0 && <span class="has-error"> · {summary.failed} failed</span>}</>
          : catalog ? "Browse table names" : "Connect to load metadata"}
      </span>}
    </Button>
    {open && catalog && <AvailableTablesDialog catalog={catalog} onUse={onUse} showUse={showUse} onClose={() => setOpen(false)} />}
  </div>;
}
