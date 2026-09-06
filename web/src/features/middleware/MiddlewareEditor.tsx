import { useId, useRef, useState } from "preact/hooks";

import { isObject } from "../../schema/value";
import type { JsonObject, JsonValue } from "../../types";
import { Button } from "../../ui/Button";
import { AutofillResistantInput, AutofillResistantTextarea } from "../../ui/AutofillResistantField";
import { SelectControl } from "../../ui/SelectControl";
import { DragHandleIcon, TrashIcon } from "../../ui/icons";
import { TablePatternInput } from "../tableSelection/TablePatternInput";
import { TransformPreview } from "./TransformPreview";
import { TransformTableScope } from "./TransformTableScope";
import type { TransformPreviewSource } from "../../generated/apiContract";

const ACTIONS = [
  { value: "datafusion", label: "SQL" },
  { value: "filter", label: "String filter" },
];
const DEFAULT_TABLES: JsonObject = { include: "*", include_mode: "glob", exclude_mode: "glob" };
const SCOPE_HELP = "Default: glob / wildcard. * matches any number of characters; ? matches one. The .* button enables regular expressions independently for each field. Match the complete current table name (namespace.table for namespaced sources). Each step sees the name and columns produced by previous steps. Exclude applies only to this step. A table that does not match passes through unchanged; matching tables must have the columns this transform requires.";

function action(entry: JsonObject): string | undefined {
  const keys = Object.keys(entry).filter(key => key !== "tables");
  return keys.length === 1 ? keys[0] : undefined;
}

function summary(kind: string | undefined, raw: JsonObject): string {
  if (kind === "datafusion") return typeof raw.sql === "string" ? raw.sql.replace(/\s+/g, " ") : "Configure SQL";
  if (kind === "filter") return `${typeof raw.field === "string" && raw.field ? raw.field : "Column"} = ${JSON.stringify(raw.value ?? "")}`;
  return "Edit unsupported configuration in YAML";
}

export function MiddlewareEditor({ value, disabled, onChange, source }: {
  value: JsonValue; disabled: boolean; onChange: (value: JsonValue) => void;
  source?: TransformPreviewSource | undefined;
}) {
  const entries = Array.isArray(value) ? value : [];
  const sequence = useRef(0);
  const identity = useRef<{ fingerprint: string; ids: number[] }>({ fingerprint: "", ids: [] });
  const fingerprint = JSON.stringify(entries);
  if (identity.current.fingerprint !== fingerprint) {
    identity.current = { fingerprint, ids: entries.map(() => ++sequence.current) };
  }
  const ids = identity.current.ids;
  const [newStep, setNewStep] = useState<number>();
  const drag = useRef<number>();
  const commit = (next: JsonValue[], nextIds: number[]) => {
    if (disabled) return;
    identity.current = { fingerprint: JSON.stringify(next), ids: nextIds };
    onChange(next);
  };
  const move = (from: number, to: number) => {
    if (disabled || from === to || from < 0 || to < 0 || to >= entries.length) return;
    const next = [...entries], nextIds = [...ids];
    const [entry] = next.splice(from, 1), [id] = nextIds.splice(from, 1);
    if (entry === undefined || id === undefined) return;
    next.splice(to, 0, entry); nextIds.splice(to, 0, id);
    commit(next, nextIds);
  };
  return <section class="middleware-editor" aria-label="Transforms">
    <header class="middleware-heading">
      <h2>Transforms <span class="middleware-count">{entries.length}</span></h2>
      <span class="middleware-order-hint">Applied top to bottom</span>
    </header>
    {entries.length === 0 && <p class="middleware-empty">No transforms. Rows pass through unchanged.</p>}
    <div class="middleware-list">
      {entries.map((entry, index) => <TransformStrip key={ids[index]} entry={entry}
        entries={entries} source={source}
        index={index} disabled={disabled} initiallyOpen={ids[index] === newStep}
        onChange={next => commit(entries.map((current, offset) => offset === index ? next : current), ids)}
        onClone={() => {
          const next = [...entries], nextIds = [...ids];
          next.splice(index + 1, 0, structuredClone(entry));
          nextIds.splice(index + 1, 0, ++sequence.current);
          commit(next, nextIds);
        }}
        onDelete={() => {
          if (!window.confirm(`Delete transform ${index + 1}?`)) return;
          commit(entries.filter((_, offset) => offset !== index), ids.filter((_, offset) => offset !== index));
        }}
        onDragStart={() => { drag.current = ids[index]; }}
        onDragEnd={() => { drag.current = undefined; }}
        onDrop={() => {
          const from = ids.indexOf(drag.current ?? -1);
          drag.current = undefined;
          move(from, index);
        }}
      />)}
    </div>
    <Button class="middleware-add" disabled={disabled} aria-label="Add transform" onClick={() => {
      const id = ++sequence.current;
      setNewStep(id);
      commit([...entries, { tables: { ...DEFAULT_TABLES }, datafusion: { sql: "SELECT * FROM input" } }], [...ids, id]);
    }}><span aria-hidden="true">+</span> Add transform</Button>
  </section>;
}

function TransformStrip({ entry, entries, source, index, disabled, initiallyOpen, onChange, onClone, onDelete, onDragStart, onDragEnd, onDrop }: {
  entry: JsonValue; index: number; disabled: boolean; initiallyOpen: boolean;
  entries: JsonValue[]; source: TransformPreviewSource | undefined;
  onChange: (entry: JsonValue) => void; onClone: () => void; onDelete: () => void;
  onDragStart: () => void; onDragEnd: () => void; onDrop: () => void;
}) {
  const [expanded, setExpanded] = useState(initiallyOpen);
  const [preview, setPreview] = useState(false);
  const id = useId();
  const object = isObject(entry) ? entry : {};
  const kind = action(object);
  const known = ACTIONS.some(option => option.value === kind);
  const raw = kind !== undefined && isObject(object[kind]) ? object[kind] : {};
  const tables = isObject(object.tables) ? object.tables : DEFAULT_TABLES;
  const include = typeof tables.include === "string" ? tables.include : "";
  const exclude = typeof tables.exclude === "string" ? tables.exclude : "";
  const updateTables = (next: JsonObject) => onChange({ ...object, tables: { ...tables, ...next } });
  const updateRaw = (next: JsonObject) => { if (kind) onChange({ ...object, [kind]: { ...raw, ...next } }); };
  const title = ACTIONS.find(option => option.value === kind)?.label ?? kind ?? "Invalid transform";
  return <article class={`middleware-strip ${expanded ? "expanded" : ""}`}
    onDragOver={event => { if (!disabled) event.preventDefault(); }}
    onDrop={event => { event.preventDefault(); if (!disabled) onDrop(); }}>
    <div class="middleware-strip-heading">
      <Button variant="plain" shape="icon" class="middleware-drag" disabled={disabled} draggable={!disabled}
        aria-label={`Reorder transform ${index + 1}`} title="Drag to reorder"
        onDragStart={event => {
          event.dataTransfer?.setData("text/plain", String(index));
          if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
          onDragStart();
        }} onDragEnd={onDragEnd}><DragHandleIcon /></Button>
      <Button variant="plain" class="middleware-strip-toggle" aria-expanded={expanded} aria-controls={`${id}-settings`}
        aria-label={`${expanded ? "Collapse" : "Expand"} transform ${index + 1}`}
        onClick={() => setExpanded(!expanded)}>
        <span class="middleware-step-number">{index + 1}</span>
        <span class="middleware-strip-description">
          <span class="middleware-strip-title">{title}</span>
          <span class="middleware-strip-summary" title={summary(kind, raw)}>{summary(kind, raw)}</span>
        </span>
        <span class="middleware-scope-summary" title={`Include: ${include || "(empty)"}${exclude ? `; exclude: ${exclude}` : ""}`}>
          <span>{include === "*" && !exclude ? "All tables" : `${exclude ? "Include: " : ""}${include || "Include required"}`}</span>
          {exclude && <span>Exclude: {exclude}</span>}
        </span>
      </Button>
      <div class="middleware-strip-actions">
        <Button class="middleware-clone" disabled={disabled} aria-label={`Clone transform ${index + 1}`} title="Clone transform with its Include / Exclude" onClick={onClone}>
          <span class="ui-icon copy-icon" aria-hidden="true" /><span>Clone</span>
        </Button>
        <Button variant="plain" shape="icon" disabled={disabled} aria-label={`Delete transform ${index + 1}`} title="Delete transform" onClick={onDelete}><TrashIcon /></Button>
      </div>
    </div>
    {expanded && <div class="middleware-strip-body" id={`${id}-settings`}>
      {!known ? <p role="alert">This transform cannot be edited here. Open YAML to correct its configuration.</p> : <>
        <TransformTableScope id={id} index={index} rule={{ include, exclude: exclude || null,
          include_mode: tables.include_mode === "regex" ? "regex" : "glob",
          exclude_mode: tables.exclude_mode === "regex" ? "regex" : "glob" }}>
        <div class="middleware-scope-fields">
          {(["include", "exclude"] as const).map(field => {
            const label = field === "include" ? "Include" : "Exclude";
            const mode = tables[`${field}_mode`] === "regex" ? "regex" : "glob";
            return <div class="middleware-pattern-field" key={field}>
              <label for={`${id}-${field}`}>{label}{field === "exclude" && <small class="optional">(optional)</small>}
                <span class="help" tabIndex={0} title={SCOPE_HELP} aria-label={`${label}: ${SCOPE_HELP}`}>?</span>
              </label>
              <TablePatternInput id={`${id}-${field}`} label={label} value={field === "include" ? include : exclude}
                mode={mode} disabled={disabled} required={field === "include"} invalid={false}
                onChange={value => updateTables({ [field]: value || (field === "exclude" ? null : "") })}
                onModeChange={value => updateTables({ [`${field}_mode`]: value })} />
            </div>;
          })}
        </div>
        </TransformTableScope>
        <div class="middleware-action-field">
          <label for={`${id}-action`}>Transformation</label>
          <SelectControl id={`${id}-action`} value={kind ?? ""} placeholder="Select transformation" clearable={false}
            options={ACTIONS} disabled={disabled} onChange={next => {
              const { [kind ?? ""]: _previous, ...rest } = object;
              onChange({ ...rest, [next]: next === "datafusion" ? { sql: "SELECT * FROM input" } : { field: "", value: "" } });
            }} />
        </div>
        {kind === "filter" ? <div class="middleware-filter-fields">
          <label><span>Column</span><AutofillResistantInput type="text" value={typeof raw.field === "string" ? raw.field : ""}
            disabled={disabled} onInput={event => updateRaw({ field: event.currentTarget.value })} /></label>
          <label><span>Equals</span><AutofillResistantInput type="text" value={typeof raw.value === "string" ? raw.value : ""}
            disabled={disabled} onInput={event => updateRaw({ value: event.currentTarget.value })} /></label>
        </div> : <label class="middleware-sql-field"><span>SQL over table <code>input</code></span>
          <AutofillResistantTextarea value={typeof raw.sql === "string" ? raw.sql : ""} disabled={disabled}
            onInput={event => updateRaw({ sql: event.currentTarget.value })} />
        </label>}
      </>}
      <div class="middleware-preview">
        <Button variant="plain" class="middleware-preview-toggle" aria-label={`Preview transform ${index + 1}`}
          aria-expanded={preview} aria-controls={`${id}-preview`} onClick={() => setPreview(!preview)}>
          <span class={`middleware-chevron ${preview ? "open" : ""}`} aria-hidden="true" />Preview
          <span class="middleware-preview-hint">Before / after this step</span>
        </Button>
        {preview && <div id={`${id}-preview`}><TransformPreview entries={entries} index={index} source={source} /></div>}
      </div>
    </div>}
  </article>;
}
