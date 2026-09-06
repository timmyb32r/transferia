import { createPortal } from "preact/compat";
import { useEffect, useId, useLayoutEffect, useRef, useState } from "preact/hooks";
import type { PatternMode, TableIdentity } from "../../generated/apiContract";
import { TableCatalogContext, type TableCatalog } from "../../schema/tableCatalog";
import { Button } from "../../ui/Button";
import { TablePatternInput } from "../tableSelection/TablePatternInput";
import { completionPattern, qualifiedName } from "../tableSelection/model";

export function AvailableTablesDialog({ catalog, onClose }: { catalog: TableCatalog; onClose: () => void }) {
  const id = useId();
  const dialog = useRef<HTMLElement>(null);
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<PatternMode>("glob");
  const [result, setResult] = useState<{ key: string; catalog: TableIdentity[]; tables?: TableIdentity[]; error?: string }>();
  const [copy, setCopy] = useState<{ name: string; state: "pending" | "copied" | "error" }>();
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
  const copyName = async (name: string) => {
    if (copying.current) return;
    copying.current = true;
    setCopy({ name, state: "pending" });
    try {
      await navigator.clipboard.writeText(name);
      setCopy({ name, state: "copied" });
    } catch {
      setCopy({ name, state: "error" });
    } finally { copying.current = false; }
  };
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
      <div class="available-tables-status" role="status" aria-live="polite">
        {current?.error ?? (!tables ? "Searching…" : copy?.state === "error" ? `Could not copy ${copy.name}.` : copy?.state === "copied" ? `Copied ${copy.name}`
          : `${tables.length} tables`)}
      </div>
      <div class="available-tables-list" role="region" aria-label="Available table names" aria-busy={!tables && !current?.error}>
        {tables?.map(table => {
          const name = qualifiedName(table);
          return <div class="available-table-row" key={JSON.stringify(table)}>
            <span title={name}>{name}</span>
            <Button variant="plain" shape="icon" class="copy-action copy-action-framed" title="Copy table name" aria-label={`Copy ${name}`} pending={copy?.name === name && copy.state === "pending"}
              disabled={copy?.state === "pending" && copy.name !== name}
              onClick={() => { void copyName(name); }}><span class="ui-icon copy-icon" aria-hidden="true" /></Button>
          </div>;
        })}
        {tables?.length === 0 && <p>No matching tables.</p>}
      </div>
    </section>
  </div>, document.body);
}
