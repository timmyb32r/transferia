import type { ComponentChildren } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";
import type { TableIdentity } from "../../generated/apiContract";
import { Button } from "../../ui/Button";
import { qualifiedName } from "./model";

export function MatchedTablesDisclosure({ id, label, regionLabel = label, toggleLabel,
  open, onToggle, tables, headerClass, before, after }: {
  id: string; label: string; regionLabel?: string; toggleLabel?: string;
  open: boolean; onToggle: () => void; tables: TableIdentity[] | undefined;
  headerClass: "table-rule-result" | "table-selection-footer";
  before?: ComponentChildren; after?: ComponentChildren;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const previousHeight = useRef("");
  const [size, setSize] = useState({ full: false, height: "" });
  useEffect(() => { if (!open) setSize({ full: false, height: "" }); }, [open]);
  const { full } = size;
  const toggleHeight = () => {
    const list = viewport.current;
    if (!list) return;
    if (full) {
      setSize({ full: false, height: previousHeight.current });
      return;
    }
    if (!tables) return;
    // Measure only on explicit activation. A later preview must never resize
    // this region and move the controls below it while the user is editing.
    previousHeight.current = list.style.height;
    const height = Math.ceil(Math.max(list.offsetHeight,
      list.scrollHeight + list.offsetHeight - list.clientHeight));
    setSize({ full: true, height: `${height}px` });
    list.scrollTop = 0;
  };
  return <>
    <div class={headerClass} aria-live={headerClass === "table-rule-result" ? "polite" : undefined}
      aria-atomic={headerClass === "table-rule-result" ? "true" : undefined}>
      {before}
      <Button class="matched-toggle" aria-label={toggleLabel}
        aria-expanded={open} aria-controls={id} disabled={!tables}
        onClick={() => { setSize({ full: false, height: "" }); onToggle(); }}>
        <span class="table-matches-chevron" aria-hidden="true" />
        <span class="table-match-label">{label}</span>{" "}
        <span class="table-match-count">{tables ? tables.length : "—"}</span>
      </Button>
      {after}
      {open && <Button class="table-matches-height-toggle" aria-controls={id} aria-expanded={full}
        title={full ? "Restore the previous list height." : "Show all matched tables without an internal scrollbar."}
        disabled={!tables && !full} onClick={toggleHeight}>
        <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor"
          stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">
          <path d={full ? "M8 1v5M5 3l3 3 3-3M8 15v-5M5 13l3-3 3 3" : "M8 2v12M5 5l3-3 3 3M5 11l3 3 3-3"} />
        </svg>
        <span class="table-matches-height-label">
          <span aria-hidden={full} style={{ visibility: full ? "hidden" : "visible" }}>Show all</span>
          <span aria-hidden={!full} style={{ visibility: full ? "visible" : "hidden" }}>Restore height</span>
        </span>
      </Button>}
    </div>
    {open && <div ref={viewport} id={id} class="table-rule-matches" role="region" tabIndex={0}
      aria-label={regionLabel} aria-busy={!tables} style={{ height: size.height }}>
      {!tables ? <div>Waiting for a valid table selection…</div>
        : tables.length === 0 ? <div>No matched tables.</div>
        : tables.map(table => <div key={JSON.stringify([table.namespace, table.name])}>{qualifiedName(table)}</div>)}
    </div>}
  </>;
}
