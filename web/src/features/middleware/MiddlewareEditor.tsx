import { flushSync } from "preact/compat";
import { useId, useMemo, useRef, useState } from "preact/hooks";

import { isObject } from "../../schema/value";
import type { JsonObject, JsonValue } from "../../types";
import { Button } from "../../ui/Button";
import { CopyIcon } from "../../ui/CopyButton";
import { AutofillResistantInput } from "../../ui/AutofillResistantField";
import { SqlEditor } from "../../ui/SqlEditor";
import { SelectControl } from "../../ui/SelectControl";
import { DragHandleIcon, TrashIcon } from "../../ui/icons";
import { exactPattern } from "../tableSelection/model";
import { TransformPreview } from "./TransformPreview";
import { TransformTableScope, useTransformMatches } from "./TransformTableScope";
import { TransformSchemaLoader } from "./TransformSchemaLoader";
import { TransformNameAction } from "./TransformNameAction";
import { TableCatalogContext, useTableCatalog } from "../../schema/tableCatalog";
import { InstantTooltip } from "../../ui/InstantTooltip";
import type { TransformPreviewSource } from "../../generated/apiContract";

const ACTIONS = [
  { value: "datafusion", label: "SQL" },
  { value: "filter", label: "String filter" },
  { value: "rename_table", label: "Rename table" },
];
const DEFAULT_TABLES: JsonObject = { include: "*", include_mode: "glob", exclude_mode: "glob" };

function action(entry: JsonObject): string | undefined {
  const keys = Object.keys(entry).filter(key => key !== "tables" && key !== "name");
  return keys.length === 1 ? keys[0] : undefined;
}

function summary(kind: string | undefined, raw: JsonObject): string {
  if (kind === "datafusion") return typeof raw.sql === "string" ? raw.sql.replace(/\s+/g, " ") : "Configure SQL";
  if (kind === "filter") return `${typeof raw.field === "string" && raw.field ? raw.field : "Column"} = ${JSON.stringify(raw.value ?? "")}`;
  if (kind === "rename_table") return raw.mode === "regex"
    ? `${typeof raw.pattern === "string" ? raw.pattern : ""} → ${typeof raw.replacement === "string" ? raw.replacement : ""}`
    : `→ ${typeof raw.name === "string" && raw.name ? raw.name : "New table name"}`;
  return "Edit unsupported configuration in YAML";
}

export function MiddlewareEditor({ value, disabled, onChange, source, catalogUnavailableReason }: {
  value: JsonValue; disabled: boolean; onChange: (value: JsonValue) => void;
  source?: TransformPreviewSource | undefined;
  catalogUnavailableReason?: string | undefined;
}) {
  const entries = Array.isArray(value) ? value : [];
  const catalog = useTableCatalog();
  const needsCatalog = (source !== undefined || catalogUnavailableReason !== undefined) && catalog === undefined;
  const unavailableReason = catalogUnavailableReason ?? "Use Discover tables in Tables first to obtain the available table list.";
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
        entries={entries} source={source} needsCatalog={needsCatalog} catalogUnavailableReason={unavailableReason}
        index={index} disabled={disabled} initiallyOpen={ids[index] === newStep}
        onChange={next => commit(entries.map((current, offset) => offset === index ? next : current), ids)}
        onClone={() => {
          if (needsCatalog) return;
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
    <InstantTooltip class="middleware-add-hint" content={needsCatalog
      ? unavailableReason : "Add transform"}>
    <Button class="middleware-add" disabled={disabled || needsCatalog} aria-label="Add transform" onClick={() => {
      if (needsCatalog) return;
      const id = ++sequence.current;
      setNewStep(id);
      commit([...entries, { tables: { ...DEFAULT_TABLES } }], [...ids, id]);
    }}><span aria-hidden="true">+</span> Add transform</Button>
    </InstantTooltip>
  </section>;
}

function TransformStrip({ entry, entries, source, needsCatalog, catalogUnavailableReason, index, disabled, initiallyOpen, onChange, onClone, onDelete, onDragStart, onDragEnd, onDrop }: {
  entry: JsonValue; index: number; disabled: boolean; initiallyOpen: boolean;
  entries: JsonValue[]; source: TransformPreviewSource | undefined;
  needsCatalog: boolean; catalogUnavailableReason: string;
  onChange: (entry: JsonValue) => void; onClone: () => void; onDelete: () => void;
  onDragStart: () => void; onDragEnd: () => void; onDrop: () => void;
}) {
  const [expanded, setExpanded] = useState(initiallyOpen);
  const [preview, setPreview] = useState(false);
  const id = useId();
  const object = isObject(entry) ? entry : {};
  const kind = action(object);
  const known = ACTIONS.some(option => option.value === kind);
  const unselected = isObject(entry) && Object.keys(object).every(key => key === "tables" || key === "name");
  const name = typeof object.name === "string" ? object.name : "";
  const raw = kind !== undefined && isObject(object[kind]) ? object[kind] : {};
  const tables = isObject(object.tables) ? object.tables : DEFAULT_TABLES;
  const include = typeof tables.include === "string" ? tables.include : "";
  const exclude = typeof tables.exclude === "string" ? tables.exclude : "";
  const catalog = useTableCatalog();
  const preceding = entries.slice(0, index);
  // Known row-only actions preserve identities. Do not delay browsing their
  // input catalog; identity-changing or unknown actions require server projection.
  const projectsNames = preceding.some(item => isObject(item) && Object.keys(item)
    .some(key => !["tables", "name", "filter", "datafusion"].includes(key)));
  const matches = useTransformMatches({ include, exclude: exclude || null,
    include_mode: tables.include_mode === "regex" ? "regex" : "glob",
    exclude_mode: tables.exclude_mode === "regex" ? "regex" : "glob" }, expanded, projectsNames ? preceding : []);
  const scopedCatalog = useMemo(() => {
    if (!catalog || (projectsNames && !matches?.lineage)) return undefined;
    if (!projectsNames || !matches?.lineage) return catalog;
    const current = new Map(matches.lineage.map(item => [JSON.stringify([item.source.namespace, item.source.name]), item.current]));
    const projected = (table: { namespace: string; name: string }) => current.get(JSON.stringify([table.namespace, table.name]));
    return { ...catalog, tables: matches.lineage.map(item => item.current),
      metadata: catalog.metadata ? { ...catalog.metadata,
        loaded: catalog.metadata.loaded.flatMap(table => { const next = projected(table); return next ? [next] : []; }),
        errors: catalog.metadata.errors.flatMap(error => { const next = projected(error.table); return next ? [{ ...error, table: next }] : []; }),
      } : undefined };
  }, [catalog, projectsNames, matches?.lineage]);
  const updateTables = (next: JsonObject) => onChange({ ...object, tables: { ...tables, ...next } });
  const updateRaw = (next: JsonObject) => { if (kind) onChange({ ...object, [kind]: { ...raw, ...next } }); };
  const title = unselected ? "Not selected" : ACTIONS.find(option => option.value === kind)?.label ?? kind ?? "Invalid transform";
  const description = unselected ? "Choose a transformation" : summary(kind, raw);
  return <article class={`middleware-strip ${expanded ? "expanded" : ""}${unselected && !disabled ? " required-incomplete" : ""}`}
    data-required-guidance="structural"
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
        data-required-control={unselected && !expanded && !disabled ? true : undefined}
        aria-label={`${expanded ? "Collapse" : "Expand"} transform ${index + 1}`}
        onClick={event => {
          const root = event.currentTarget.ownerDocument.documentElement;
          const value = root.style.getPropertyValue("overflow-anchor");
          const priority = root.style.getPropertyPriority("overflow-anchor");
          // The browser may anchor to Pipeline settings below this strip. Keep
          // this commit at the current scroll offset, not at the page bottom.
          root.style.setProperty("overflow-anchor", "none");
          try {
            event.currentTarget.focus({ preventScroll: true });
            flushSync(() => setExpanded(current => !current));
          } finally {
            // Flush the expanded layout before re-enabling native anchoring.
            void root.scrollHeight;
            if (value) root.style.setProperty("overflow-anchor", value, priority);
            else root.style.removeProperty("overflow-anchor");
          }
        }}>
        <span class="middleware-step-number">{index + 1}</span>
        <span class="middleware-strip-description">
          <span class="middleware-strip-title-line">
            <span class="middleware-strip-title" title={name || title}>{name || title}</span>
            {name && <span class="middleware-strip-type" title={title}>{title}</span>}
          </span>
          <span class="middleware-strip-summary" title={description}>{description}</span>
        </span>
        <span class="middleware-scope-summary" title={`Include: ${include || "(empty)"}${exclude ? `; exclude: ${exclude}` : ""}`}>
          <span>{include === "*" && !exclude ? "All tables" : `${exclude ? "Include: " : ""}${include || "Include required"}`}</span>
          {exclude && <span>Exclude: {exclude}</span>}
        </span>
      </Button>
      <div class="middleware-strip-actions">
        <Button variant="plain" class="middleware-clone copy-action copy-action-framed" disabled={disabled || needsCatalog} aria-label={`Clone transform ${index + 1}`} title="Clone transform with its Include / Exclude" onClick={onClone}>
          <CopyIcon /><span>Clone</span>
        </Button>
        <Button variant="plain" shape="icon" disabled={disabled} aria-label={`Delete transform ${index + 1}`} title="Delete transform" onClick={onDelete}><TrashIcon /></Button>
        <TransformNameAction name={name} index={index} disabled={disabled || !isObject(entry)} onSave={next => {
          const { name: _previous, ...rest } = object;
          onChange(next === "" ? rest : { ...rest, name: next });
        }} />
      </div>
    </div>
    {expanded && <div class="middleware-strip-body" id={`${id}-settings`}>
      {!known && !unselected ? <p role="alert">This transform cannot be edited here. Open YAML to correct its configuration.</p> : <>
        <TableCatalogContext.Provider value={scopedCatalog}><TransformTableScope id={id} index={index} matches={matches}
          catalogUnavailableReason={catalog ? "Updating table names from preceding transforms…" : catalogUnavailableReason}
          rule={{ include, exclude, include_mode: tables.include_mode === "regex" ? "regex" : "glob",
            exclude_mode: tables.exclude_mode === "regex" ? "regex" : "glob" }} disabled={disabled}
          onChange={patch => updateTables(patch as JsonObject)}
          onUseTable={disabled ? undefined : table => updateTables({ include: exactPattern(table, tables.include_mode === "regex" ? "regex" : "glob") })} /></TableCatalogContext.Provider>
        <div class={`middleware-action-field${unselected && !disabled ? " required-incomplete" : ""}`}>
          <label for={`${id}-action`}>Transformation</label>
          <SelectControl id={`${id}-action`} value={kind ?? ""} placeholder="Select transformation"
            options={ACTIONS} disabled={disabled} onChange={next => {
              const { [kind ?? ""]: _previous, ...rest } = object;
              onChange(next ? { ...rest, [next]: next === "datafusion" ? { sql: "SELECT * FROM input" }
                : next === "rename_table" ? { mode: "exact", name: "" } : { field: "", value: "" } } : rest);
            }} />
        </div>
        {kind === "filter" ? <div class="middleware-filter-fields">
          <label><span>Column</span><AutofillResistantInput type="text" value={typeof raw.field === "string" ? raw.field : ""}
            disabled={disabled} onInput={event => updateRaw({ field: event.currentTarget.value })} /></label>
          <label><span>Equals</span><AutofillResistantInput type="text" value={typeof raw.value === "string" ? raw.value : ""}
            disabled={disabled} onInput={event => updateRaw({ value: event.currentTarget.value })} /></label>
        </div> : kind === "datafusion" ? <label class="middleware-sql-field"><span>SQL over table <code>input</code></span>
          <SqlEditor value={typeof raw.sql === "string" ? raw.sql : ""} disabled={disabled}
            onChange={sql => updateRaw({ sql })} />
        </label> : kind === "rename_table" ? <div class="middleware-rename-fields">
          <label for={`${id}-rename-mode`}>Rename mode</label><SelectControl id={`${id}-rename-mode`} placeholder="Choose rename mode" value={raw.mode === "regex" ? "regex" : "exact"} clearable={false}
            disabled={disabled} options={[{ value: "exact", label: "Exact name" }, { value: "regex", label: "Regex replacement" }]}
            onChange={mode => onChange({ ...object, rename_table: mode === "regex"
              ? { mode, pattern: "", replacement: "" } : { mode, name: "" } })} />
          {raw.mode === "regex" ? <>
            <label><span>Pattern</span><AutofillResistantInput type="text" required value={typeof raw.pattern === "string" ? raw.pattern : ""}
              disabled={disabled} onInput={event => updateRaw({ pattern: event.currentTarget.value })} /></label>
            <label><span>Replacement</span><AutofillResistantInput type="text" value={typeof raw.replacement === "string" ? raw.replacement : ""}
              disabled={disabled} onInput={event => updateRaw({ replacement: event.currentTarget.value })} /></label>
          </> : <label><span>New table name</span><AutofillResistantInput type="text" required value={typeof raw.name === "string" ? raw.name : ""}
            disabled={disabled} onInput={event => updateRaw({ name: event.currentTarget.value })} /></label>}
          <p class="muted">Only the table name changes; namespace stays unchanged. Regex replaces all matches; names with no match stay unchanged.
            Use $1 or {"${name}"} for captures, $$ for a literal dollar. Unknown or unmatched captures and empty output names fail validation.</p>
        </div> : null}
      </>}
      <div class="middleware-preview">
        {source && <TransformSchemaLoader tables={matches?.sourceTables} source={source} disabled={disabled} />}
        <Button variant="plain" class="middleware-preview-toggle" aria-label={`Preview transform ${index + 1}`}
          disabled={unselected} title={unselected ? "Select a transformation first" : undefined}
          aria-expanded={preview && !unselected} aria-controls={`${id}-preview`} onClick={() => setPreview(!preview)}>
          <span class={`middleware-chevron ${preview ? "open" : ""}`} aria-hidden="true" />Preview
          <span class="middleware-preview-hint">Before / after this step</span>
        </Button>
        {preview && !unselected && <div id={`${id}-preview`}><TransformPreview entries={entries} index={index} source={source} matchedTables={matches?.tables} lineage={matches?.lineage} /></div>}
      </div>
    </div>}
  </article>;
}
