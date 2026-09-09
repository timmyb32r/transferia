import { useEffect, useId, useRef, useState } from "preact/hooks";

import { useControlPlane } from "../../bootstrap/ApplicationServicesProvider";
import type { TableIdentity, TableLineage, TransformPreviewFrame, TransformPreviewResult, TransformPreviewSource } from "../../generated/apiContract";
import type { JsonValue } from "../../types";
import { AutofillResistantInput } from "../../ui/AutofillResistantField";
import { Button } from "../../ui/Button";
import { SelectControl } from "../../ui/SelectControl";
import { qualifiedName } from "../tableSelection/model";
import { useSourceMetadataContext } from "../../delivery/sourceMetadata";

export function TransformPreview({ entries, index, source, matchedTables, lineage }: {
  entries: JsonValue[]; index: number; source: TransformPreviewSource | undefined;
  matchedTables: TableIdentity[] | undefined;
  lineage?: TableLineage[] | undefined;
}) {
  const api = useControlPlane();
  const metadata = useSourceMetadataContext()?.metadata;
  const id = useId();
  const sourceKey = JSON.stringify(source ?? null);
  const [selected, setSelected] = useState<{ key: string; table: TableIdentity }>();
  const [rowLimit, setRowLimit] = useState("20");
  const limits = { sampleMiB: "16", memoryMiB: "256", timeoutSeconds: "30" };
  const [tab, setTab] = useState<"before" | "after">("after");
  const [running, setRunning] = useState(false);
  const [status, setStatus] = useState<{ key: string; text: string; error?: boolean }>();
  const [result, setResult] = useState<{ key: string; value: TransformPreviewResult[] }>();
  const previewRequest = useRef<AbortController>();
  const tables = matchedTables ?? [];
  const table = selected?.key === sourceKey && tables.some(candidate => candidate.namespace === selected.table.namespace && candidate.name === selected.table.name)
    ? selected.table : undefined;
  const allTables = selected === undefined || selected.key !== sourceKey;
  const sampleTables = allTables ? tables : table ? [table] : [];
  const sourceTables = (candidate: TableIdentity) => {
    if (lineage === undefined) return [candidate];
    const origins = lineage.filter(item => item.current.namespace === candidate.namespace && item.current.name === candidate.name);
    return origins.map(item => item.source);
  };
  const schemaReady = sampleTables.every(candidate => {
    const originals = sourceTables(candidate);
    return originals.length > 0 && originals.every(original => !metadata || metadata.loaded.some(loaded => loaded.namespace === original.namespace && loaded.name === original.name));
  });
  const resultKey = JSON.stringify([sourceKey, metadata?.id, entries.slice(0, index + 1), index, sampleTables, lineage, rowLimit, limits]);
  const live = useRef({ sourceKey, resultKey });
  live.current = { sourceKey, resultKey };

  useEffect(() => {
    previewRequest.current?.abort();
    previewRequest.current = undefined;
    setRunning(false);
    return () => { previewRequest.current?.abort(); };
  }, [resultKey]);


  const run = async () => {
    if (!source || sampleTables.length === 0 || !schemaReady || previewRequest.current) return;
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
    setResult(undefined);
    let activeTable: TableIdentity | undefined;
    try {
      const value: TransformPreviewResult[] = [];
      // One bounded source read at a time; never fan out across a large catalog.
      for (const candidate of sampleTables) {
        if (request.signal.aborted || live.current.resultKey !== resultKey) return;
        activeTable = candidate;
        const originals = sourceTables(candidate);
        if (!originals.length) throw new Error("Source table mapping is unavailable; refresh table matches");
        for (const original of originals) {
        if (request.signal.aborted || live.current.resultKey !== resultKey) return;
        value.push(await api.previewTransforms({
          metadata_id: metadata?.id ?? null,
          source, table: original, row_limit, middlewares: entries, through_step: index,
          max_sample_bytes, memory_limit_bytes, timeout_ms,
        }, request.signal));
        }
      }
      if (request.signal.aborted || live.current.resultKey !== resultKey) return;
      setResult({ key: resultKey, value });
      setTab("after");
      setStatus({ key: resultKey, text: allTables
        ? `Previewed ${value.length} matched tables, up to ${row_limit} source rows per table.`
        : value[0]?.applied
        ? `Applied step ${index + 1}. Preview uses up to ${row_limit} source rows, not the full table.`
        : `Step ${index + 1} does not match this table; it passes through unchanged.` });
    } catch (error) {
      if (!request.signal.aborted && live.current.resultKey === resultKey)
        setStatus({ key: resultKey, text: `${activeTable ? `${qualifiedName(activeTable)}: ` : ""}${error instanceof Error ? error.message : String(error)}`, error: true });
    } finally {
      if (previewRequest.current === request) { previewRequest.current = undefined; setRunning(false); }
    }
  };

  const current = result?.key === resultKey ? result.value : undefined;
  const frames = current?.map(value => value[tab]);
  const feedback = status?.key === resultKey || status?.key === sourceKey ? status : undefined;
  const note = source ? "Table-row sample only; transport / CDC metadata is unavailable. Preview never writes to the destination."
    : "Select a source that supports table preview to load sample rows.";
  return <section class="transform-preview-content" aria-label={`Preview data for transform ${index + 1}`}>
    <div class="transform-preview-controls">
      <label for={`${id}-table`}><span>Sample table</span>
        <SelectControl id={`${id}-table`} value={allTables ? "all" : table ? JSON.stringify(table) : ""} placeholder="Choose a table"
          disabled={!source || tables.length === 0 || running} clearable={false}
          options={[{ value: "all", label: "All matched tables" }, ...tables.map(value => ({ value: JSON.stringify(value), label: qualifiedName(value) }))]}
          onChange={value => {
            if (value === "all") { setSelected(undefined); return; }
            const chosen = tables.find(candidate => JSON.stringify(candidate) === value);
            if (chosen) setSelected({ key: sourceKey, table: chosen });
          }} />
      </label>
      <label title="Maximum source rows per sampled table"><span>Sample rows</span><AutofillResistantInput type="number" min={1} step={1} value={rowLimit}
        disabled={!source || running} onInput={event => setRowLimit(event.currentTarget.value)} /></label>
      <Button variant="primary" pending={running} disabled={!source || sampleTables.length === 0 || !schemaReady}
        title={schemaReady ? "Read sample rows" : "Load this transform's schemas first"}
        onClick={() => { void run(); }}>Run preview</Button>
    </div>
    <p class={`transform-preview-status ${feedback?.error ? "error" : ""}`} role="status" aria-live="polite" aria-atomic="true">
      {running ? "Validating transform schemas and reading source rows…" : feedback?.text ?? note}
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
      <span class="transform-preview-row-count">{frames ? `${frames.reduce((count, frame) => count + frame.rows.length, 0)} rows` : ""}</span>
    </div>
    <div class="transform-preview-output" role="tabpanel" id={`${id}-data`} aria-labelledby={`${id}-${tab}`} aria-busy={running} tabIndex={0}>
      {frames ? frames.map((frame, index) => <section key={index}>
        {allTables && <h4>{qualifiedName({ namespace: frame.table.namespace ?? "", name: frame.table.name })}</h4>}
        <PreviewTable frame={frame} />
      </section>) : <p>Run preview to see the table before and after this step.</p>}
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
