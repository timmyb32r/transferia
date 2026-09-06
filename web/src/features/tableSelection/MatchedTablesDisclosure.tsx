import type { ComponentChildren } from "preact";
import { useLayoutEffect, useRef, useState } from "preact/hooks";
import type { TableIdentity } from "../../generated/apiContract";
import { Button } from "../../ui/Button";
import { CopyButton } from "../../ui/CopyButton";
import { qualifiedName } from "./model";

export function MatchedTablesDisclosure({ id, label, regionLabel = label, toggleLabel,
  open, onToggle, tables, headerClass, before, after }: {
  id: string; label: string; regionLabel?: string; toggleLabel?: string;
  open: boolean; onToggle: () => void; tables: TableIdentity[] | undefined;
  headerClass: "table-rule-result" | "table-selection-footer";
  before?: ComponentChildren; after?: ComponentChildren;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const disclosure = useRef<HTMLButtonElement>(null);
  const heightAction = useRef<HTMLButtonElement>(null);
  const previousHeight = useRef("");
  const [size, setSize] = useState({ full: false, height: "" });
  const [overflowing, setOverflowing] = useState(false);
  const restoreDisclosureFocus = () => {
    if (document.activeElement === heightAction.current) disclosure.current?.focus({ preventScroll: true });
  };
  useLayoutEffect(() => {
    if (!open) { setSize({ full: false, height: "" }); return; }
    const list = viewport.current;
    if (!list) return;
    // Natural content height is capped by CSS on opening. Pin it before paint:
    // later pending/results updates may change content, never this footprint.
    const measure = () => {
      if (list.offsetHeight === 0) return;
      if (!size.height) setSize({ full: false, height: `${list.offsetHeight}px` });
      if (tables) {
        const nextOverflow = list.scrollHeight > list.clientHeight;
        if (!nextOverflow && !size.full) restoreDisclosureFocus();
        setOverflowing(nextOverflow);
      }
    };
    measure();
    const observer = typeof ResizeObserver === "undefined" ? undefined : new ResizeObserver(measure);
    observer?.observe(list);
    return () => observer?.disconnect();
  }, [open, tables, size.height]);
  const { full } = size;
  const showHeightToggle = full || overflowing;
  const toggleHeight = () => {
    const list = viewport.current;
    if (!list) return;
    if (full) {
      // Restore may hide this action when refreshed results now fit. Move focus
      // before disabling it, while the browser still knows the active control.
      restoreDisclosureFocus();
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
      <Button buttonRef={disclosure} variant="plain" class="matched-toggle" aria-label={toggleLabel}
        aria-expanded={open} aria-controls={id} disabled={!tables}
        onClick={() => { setSize({ full: false, height: "" }); onToggle(); }}>
        <span class="table-matches-chevron" aria-hidden="true" />
        <span class="table-match-label">{label}</span>{" "}
        <span class="table-match-count">{tables ? tables.length : "—"}</span>
      </Button>
      {after}
      {open && <Button buttonRef={heightAction} class="table-matches-height-toggle" aria-controls={id} aria-expanded={full}
        aria-hidden={!showHeightToggle} style={{ visibility: showHeightToggle ? "visible" : "hidden" }}
        title={full ? "Restore the previous list height." : "Show all matched tables without an internal scrollbar."}
        disabled={!showHeightToggle || (!tables && !full)} onClick={toggleHeight}>
        <span class="table-matches-height-content" aria-hidden={full}
          style={{ visibility: !showHeightToggle || full ? "hidden" : "visible" }}>
          <span class="ui-icon table-matches-height-icon" aria-hidden="true" />
          <span>Show all</span>
        </span>
        <span class="table-matches-height-content" aria-hidden={!full}
          style={{ visibility: full ? "visible" : "hidden" }}>
          <span class="ui-icon table-matches-height-icon table-matches-height-icon-restore" aria-hidden="true" />
          <span>Restore height</span>
        </span>
      </Button>}
    </div>
    {open && <div ref={viewport} id={id} class="table-rule-matches" role="region" tabIndex={0}
      aria-label={regionLabel} aria-busy={!tables} style={{ height: size.height, maxHeight: size.height ? "none" : undefined }}>
      {!tables ? <div>Waiting for a valid table selection…</div>
        : tables.length === 0 ? <div>No matched tables.</div>
        : tables.map(table => <div class="matched-table-row" key={JSON.stringify([table.namespace, table.name])}>
          <span>{qualifiedName(table)}</span><CopyButton text={qualifiedName(table)} label={`Copy ${qualifiedName(table)}`} />
        </div>)}
    </div>}
  </>;
}
