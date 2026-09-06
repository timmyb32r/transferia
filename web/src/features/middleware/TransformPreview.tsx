import { useEffect, useId, useRef, useState } from "preact/hooks";

import { useControlPlane } from "../../bootstrap/ApplicationServicesProvider";
import type { TableIdentity, TransformPreviewFrame, TransformPreviewResult, TransformPreviewSource } from "../../generated/apiContract";
import type { JsonValue } from "../../types";
import { AutofillResistantInput } from "../../ui/AutofillResistantField";
import { Button } from "../../ui/Button";
import { SelectControl } from "../../ui/SelectControl";
import { qualifiedName } from "../tableSelection/model";
import { useTableCatalog } from "../../schema/tableCatalog";
import { selectedSourceTables } from "./useTransformCatalog";

export function TransformPreview({ entries, index, source }: {
  entries: JsonValue[]; index: number; source: TransformPreviewSource | undefined;
}) {
  const api = useControlPlane();
  const verifiedCatalog = useTableCatalog();
  const id = useId();
  const sourceKey = JSON.stringify(source ?? null);
  const [catalog, setCatalog] = useState<{ key: string; tables: TableIdentity[]; shared: TableIdentity[] | undefined }>();
  const [selected, setSelected] = useState<{ key: string; table: TableIdentity }>();
  const [rowLimit, setRowLimit] = useState("20");
  const [limitsOpen, setLimitsOpen] = useState(false);
  const [limits, setLimits] = useState({ sampleMiB: "16", memoryMiB: "256", timeoutSeconds: "30" });
  const [tab, setTab] = useState<"before" | "after">("after");
  const [loadingTables, setLoadingTables] = useState(false);
  const [running, setRunning] = useState(false);
  const [status, setStatus] = useState<{ key: string; text: string; error?: boolean }>();
  const [result, setResult] = useState<{ key: string; value: TransformPreviewResult }>();
  const tableRequest = useRef<AbortController>();
  const previewRequest = useRef<AbortController>();
  const tables = catalog?.key === sourceKey && catalog.shared === verifiedCatalog?.tables
    ? catalog.tables : verifiedCatalog?.tables ?? [];
  const table = selected?.key === sourceKey && tables.some(candidate => candidate.namespace === selected.table.namespace && candidate.name === selected.table.name)
    ? selected.table : undefined;
  const resultKey = JSON.stringify([sourceKey, entries.slice(0, index + 1), index, table, rowLimit, limits]);
  const live = useRef({ sourceKey, resultKey });
  live.current = { sourceKey, resultKey };

  useEffect(() => {
    tableRequest.current?.abort();
    tableRequest.current = undefined;
    setLoadingTables(false);
    return () => { tableRequest.current?.abort(); };
  }, [sourceKey]);
  useEffect(() => {
    previewRequest.current?.abort();
    previewRequest.current = undefined;
    setRunning(false);
    return () => { previewRequest.current?.abort(); };
  }, [resultKey]);

  const loadTables = async () => {
    if (!source || tableRequest.current) return;
    const request = new AbortController();
    tableRequest.current = request;
    setLoadingTables(true);
    setStatus(undefined);
    try {
      const response = await api.checkConnection({ ...source, role: "source" }, request.signal);
      if (request.signal.aborted || live.current.sourceKey !== sourceKey) return;
      if (response.status !== "verified" || response.tables === undefined)
        throw new Error("This source did not return a verified table catalog.");
      const available = await selectedSourceTables(source, response.tables, api, request.signal);
      if (request.signal.aborted || live.current.sourceKey !== sourceKey) return;
      setCatalog({ key: sourceKey, tables: available, shared: verifiedCatalog?.tables });
      setStatus({ key: sourceKey, text: available.length ? "Choose a table to preview. No destination writes are performed." : "No available tables were found." });
    } catch (error) {
      if (!request.signal.aborted && live.current.sourceKey === sourceKey)
        setStatus({ key: sourceKey, text: error instanceof Error ? error.message : String(error), error: true });
    } finally {
      if (tableRequest.current === request) { tableRequest.current = undefined; setLoadingTables(false); }
    }
  };

  const run = async () => {
    if (!source || !table || previewRequest.current) return;
    const row_limit = Number(rowLimit);
    if (!/^\d+$/.test(rowLimit) || !Number.isSafeInteger(row_limit) || row_limit <= 0) {
      setStatus({ key: resultKey, text: "Sample rows must be a positive integer.", error: true });
      return;
    }
    const max_sample_bytes = Number(limits.sampleMiB) * 1024 * 1024;
    const memory_limit_bytes = Number(limits.memoryMiB) * 1024 * 1024;
    const timeout_ms = Number(limits.timeoutSeconds) * 1000;
    if (Object.values(limits).some(value => !/^\d+$/.test(value)) ||
        [max_sample_bytes, memory_limit_bytes, timeout_ms].some(value => !Number.isSafeInteger(value) || value <= 0)) {
      setStatus({ key: resultKey, text: "Preview limits must be positive integers within the supported range.", error: true });
      return;
    }
    const request = new AbortController();
    previewRequest.current = request;
    setRunning(true);
    setStatus(undefined);
    try {
      const value = await api.previewTransforms({
        source, table, row_limit, middlewares: entries, through_step: index,
        max_sample_bytes, memory_limit_bytes, timeout_ms,
      }, request.signal);
      if (request.signal.aborted || live.current.resultKey !== resultKey) return;
      setResult({ key: resultKey, value });
      setTab("after");
      setStatus({ key: resultKey, text: value.applied
        ? `Applied step ${index + 1}. Preview uses up to ${row_limit} source rows, not the full table.`
        : `Step ${index + 1} does not match this table; it passes through unchanged.` });
    } catch (error) {
      if (!request.signal.aborted && live.current.resultKey === resultKey)
        setStatus({ key: resultKey, text: error instanceof Error ? error.message : String(error), error: true });
    } finally {
      if (previewRequest.current === request) { previewRequest.current = undefined; setRunning(false); }
    }
  };

  const current = result?.key === resultKey ? result.value : undefined;
  const frame = current?.[tab];
  const feedback = status?.key === resultKey || status?.key === sourceKey ? status : undefined;
  const note = source ? "Table-row sample only; transport / CDC metadata is unavailable. Preview never writes to the destination."
    : "Select a source that supports table preview to load sample rows.";
  return <section class="transform-preview-content" aria-label={`Preview data for transform ${index + 1}`}>
    <div class="transform-preview-controls">
      <label for={`${id}-table`}><span>Sample table</span>
        <SelectControl id={`${id}-table`} value={table ? JSON.stringify(table) : ""} placeholder="Choose a table"
          loading={loadingTables} disabled={!source || tables.length === 0 || running} clearable={false}
          options={tables.map(value => ({ value: JSON.stringify(value), label: qualifiedName(value) }))}
          onChange={value => {
            const chosen = tables.find(candidate => JSON.stringify(candidate) === value);
            if (chosen) setSelected({ key: sourceKey, table: chosen });
          }} />
      </label>
      <label><span>Sample rows</span><AutofillResistantInput type="number" min={1} step={1} value={rowLimit}
        disabled={!source || running} onInput={event => setRowLimit(event.currentTarget.value)} /></label>
      <Button pending={loadingTables} disabled={!source || running} onClick={() => { void loadTables(); }}>Load tables</Button>
      <Button variant="primary" pending={running} disabled={!source || !table || loadingTables}
        onClick={() => { void run(); }}>Run preview</Button>
    </div>
    <div class="transform-preview-limits">
      <Button variant="plain" class="middleware-preview-toggle" aria-expanded={limitsOpen} aria-controls={`${id}-limits`}
        title="Preview only. Exceeding a limit fails preview; values are never silently truncated. SQL memory limits tracked engine allocations and retained results, not total process memory. Timeout cancels asynchronous work; it is not CPU isolation. Table samples do not contain generated transport or CDC metadata."
        onClick={() => setLimitsOpen(!limitsOpen)}>
        <span class={`middleware-chevron ${limitsOpen ? "open" : ""}`} aria-hidden="true" />Preview limits
      </Button>
      {limitsOpen && <div class="transform-preview-budget-fields" id={`${id}-limits`}>
        {([ ["sampleMiB", "Source sample (MiB)"], ["memoryMiB", "SQL memory (MiB)"], ["timeoutSeconds", "Timeout (seconds)"] ] as const).map(([key, label]) =>
          <label key={key}><span>{label}</span><AutofillResistantInput type="number" min={1} step={1}
            value={limits[key]} disabled={!source || running}
            onInput={event => setLimits({ ...limits, [key]: event.currentTarget.value })} /></label>)}
      </div>}
    </div>
    <p class={`transform-preview-status ${feedback?.error ? "error" : ""}`} role="status" aria-live="polite" aria-atomic="true">
      {running ? "Reading source rows and applying preceding transforms…" : loadingTables ? "Loading source tables…" : feedback?.text ?? note}
    </p>
    <div class="transform-preview-tabs editor-view-tabs" role="tablist" aria-label="Transform preview view">
      {(["before", "after"] as const).map(value => <Button variant="plain" key={value} role="tab" id={`${id}-${value}`}
        aria-selected={tab === value} aria-controls={`${id}-data`} tabIndex={tab === value ? 0 : -1}
        class={tab === value ? "active" : ""} onClick={() => setTab(value)}
        onKeyDown={event => {
          if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
          event.preventDefault();
          const next = event.key === "Home" ? "before" : event.key === "End" ? "after" : tab === "before" ? "after" : "before";
          setTab(next); document.getElementById(`${id}-${next}`)?.focus();
        }}>{value === "before" ? "Before step" : "After step"}</Button>)}
      <span class="transform-preview-row-count">{frame ? `${frame.rows.length} rows` : ""}</span>
    </div>
    <div class="transform-preview-output" role="tabpanel" id={`${id}-data`} aria-labelledby={`${id}-${tab}`} aria-busy={running} tabIndex={0}>
      {frame ? <PreviewTable frame={frame} /> : <p>Run preview to see the table before and after this step.</p>}
    </div>
  </section>;
}

function PreviewTable({ frame }: { frame: TransformPreviewFrame }) {
  return <table>
    <thead><tr>{frame.columns.map(column => <th key={column.name} scope="col">{column.name}<small>{column.arrow_type}</small></th>)}</tr></thead>
    <tbody>{frame.rows.map((row, index) => <tr key={index}>{frame.columns.map(column =>
      <td key={column.name}>{row[column.name] === null ? <span class="muted">NULL</span> : row[column.name]}</td>)}</tr>)}</tbody>
  </table>;
}
