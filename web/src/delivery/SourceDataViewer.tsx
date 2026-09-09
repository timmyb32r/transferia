import { useEffect, useId, useLayoutEffect, useRef, useState } from "preact/hooks";
import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import type { TableIdentity, SourcePreviewResult, TransformPreviewSource } from "../generated/apiContract";
import { Button } from "../ui/Button";
import { AutofillResistantInput } from "../ui/AutofillResistantField";
import { SelectControl } from "../ui/SelectControl";
import { useSourceMetadataContext } from "./sourceMetadata";
import { selectedSourceTables } from "../features/middleware/useTransformCatalog";
import { PreviewTable } from "../features/middleware/TransformPreview";
import { qualifiedName } from "../features/tableSelection/model";

export function SourceDataViewer({ source, mode = "tables", onClose }: {
  source: TransformPreviewSource; mode?: "tables" | "parsed"; onClose: () => void;
}) {
  const api = useControlPlane();
  const actions = useSourceMetadataContext()!;
  const metadata = actions.metadata;
  const checked = actions.discovery.state === "success" ? actions.discovery.tables : undefined;
  const selectionKey = JSON.stringify([source, metadata?.id, checked]);
  const [selectionResult, setSelectionResult] = useState<{ key: string; tables?: TableIdentity[]; error?: string }>();
  const selection = selectionResult?.key === selectionKey ? selectionResult : undefined;
  const selectionError = mode === "parsed" ? undefined : checked ? selection?.error : "Discover tables again before reading a sample.";
  const tables = selection?.tables ?? [];
  useEffect(() => {
    if (mode === "parsed" || !checked) return;
    const controller = new AbortController();
    void selectedSourceTables(source, checked, api, controller.signal).then(tables => {
      if (!controller.signal.aborted) setSelectionResult({ key: selectionKey, tables });
    }).catch(reason => {
      if (!controller.signal.aborted) setSelectionResult({ key: selectionKey,
        error: reason instanceof Error ? reason.message : String(reason) });
    });
    return () => controller.abort();
  }, [selectionKey, mode, api]);
  const [chosen, setChosen] = useState("");
  const table = tables.find(item => JSON.stringify(item) === chosen) ?? tables[0];
  const [rows, setRows] = useState("20");
  const [maxMiB, setMaxMiB] = useState("16");
  const [seconds, setSeconds] = useState("30");
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<SourcePreviewResult>();
  const [error, setError] = useState<string>();
  const request = useRef<AbortController>();
  const dialog = useRef<HTMLElement>(null);
  const close = useRef(onClose); close.current = onClose;
  const id = useId();
  useLayoutEffect(() => {
    const previous = document.activeElement;
    const overflow = document.documentElement.style.overflow;
    document.documentElement.style.overflow = "hidden";
    dialog.current?.querySelector<HTMLButtonElement>("button")?.focus({ preventScroll: true });
    return () => {
      document.documentElement.style.overflow = overflow;
      if (previous instanceof HTMLElement && previous.isConnected) previous.focus({ preventScroll: true });
    };
  }, []);
  useLayoutEffect(() => {
    request.current?.abort(); request.current = undefined; setRunning(false); setResult(undefined); setError(undefined);
    return () => { request.current?.abort(); };
  }, [JSON.stringify([source, metadata?.id, table, rows, maxMiB, seconds, mode])]);
  const run = async () => {
    if ((mode === "tables" && (!table || !metadata)) || request.current) return;
    const row_limit = Number(rows), max_sample_bytes = Number(maxMiB) * 1024 * 1024, timeout_ms = Number(seconds) * 1000;
    if ([rows, maxMiB, seconds].some(value => !/^\d+$/.test(value)) ||
        [row_limit, max_sample_bytes, timeout_ms].some(value => !Number.isSafeInteger(value) || value <= 0)) {
      setError("Sample limits must be positive integers."); return;
    }
    const controller = new AbortController(); request.current = controller;
    setRunning(true); setResult(undefined); setError(undefined);
    try {
      const sample = await api.previewSource({ metadata_id: mode === "tables" ? metadata!.id : null,
        source, table: mode === "tables" ? table! : null, row_limit, max_sample_bytes, timeout_ms }, controller.signal);
      if (request.current === controller && !controller.signal.aborted) setResult(sample);
    } catch (reason) {
      if (request.current === controller && !controller.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      if (request.current === controller) { request.current = undefined; setRunning(false); }
    }
  };
  return <div class="message-preview-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose(); }}>
    <section ref={dialog} class="source-data-viewer" role="dialog" aria-modal="true" aria-labelledby={`${id}-title`}
      onKeyDown={event => {
        if (event.key === "Escape" && !event.defaultPrevented) { event.stopPropagation(); close.current(); }
        if (event.key === "Tab") {
          const controls = Array.from(dialog.current!.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), [tabindex="0"]'))
            .filter(item => item.getClientRects().length > 0);
          const first = controls[0], last = controls.at(-1);
          if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
          else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
        }
      }}>
      <header><h2 id={`${id}-title`}>Data viewer</h2><Button shape="icon" aria-label="Close data viewer" onClick={onClose}>×</Button></header>
      <div class="source-data-viewer-controls">
        {mode === "tables" ? <label><span>Sample table</span><SelectControl value={table ? JSON.stringify(table) : ""} disabled={running || !tables.length}
          searchable clearable={false} placeholder="No selected tables" options={tables.map(item => ({ value: JSON.stringify(item), label: qualifiedName(item) }))}
          onChange={setChosen} /></label> : <p class="source-data-viewer-parser">Source output · configured parser<br /><small>One complete message/object · row limit applies per table, including DLQ</small></p>}
        <label><span>Sample rows</span><AutofillResistantInput type="number" min={1} value={rows} disabled={running} onInput={event => setRows(event.currentTarget.value)} /></label>
        <Button variant="primary" pending={running} disabled={mode === "tables" && (!table || !metadata)} onClick={() => { void run(); }}>Load sample</Button>
      </div>
      <div class="source-data-viewer-limits">
        <label>Max sample MiB<AutofillResistantInput type="number" min={1} value={maxMiB} disabled={running} onInput={event => setMaxMiB(event.currentTarget.value)} /></label>
        <label>Timeout seconds<AutofillResistantInput type="number" min={1} value={seconds} disabled={running} onInput={event => setSeconds(event.currentTarget.value)} /></label>
      </div>
      <p class={`transform-preview-status ${error || selectionError ? "error" : ""}`} role="status" aria-live="polite">{running ? "Reading source data…" : error ?? selectionError ?? (result
        ? `${result.frames.reduce((count, frame) => count + frame.rows.length, 0)} sample rows. ${mode === "parsed" ? "Configured parser applied; main and DLQ output included. " : ""}No transforms or destination writes.`
        : mode === "parsed" ? "Preview the source using its configured parser. Scan and parser detection are separate."
        : selection ? (tables.length ? "Read a source sample without applying transforms or writing to the destination." : "No tables match the source selection. Update Tables to choose a sample.") : "Resolving selected source tables…")}</p>
      <div class="transform-preview-output" aria-label="Source sample" aria-busy={running} tabIndex={0}>
        {result ? result.frames.map((frame, index) => <section key={index}><h4>{qualifiedName({ namespace: frame.table.namespace ?? "", name: frame.table.name })}</h4><PreviewTable frame={frame} /></section>)
          : <p>{mode === "parsed" ? "Load a sample to see parsed source data." : "Select a table and load a sample."}</p>}
      </div>
    </section>
  </div>;
}
