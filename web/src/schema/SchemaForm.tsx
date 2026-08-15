import { createContext, Fragment, type ComponentChildren } from "preact";
import { useContext, useEffect, useMemo, useRef, useState } from "preact/hooks";

import type { JsonObject, JsonValue } from "../types";
import {
  branchMatches,
  createValue,
  humanize,
  type CompiledNode,
} from "./compiler";
import {
  closestArrowType,
  isStringArrowType,
  parsePartitionIds,
} from "./formLogic";

interface SchemaFormProps {
  node: CompiledNode;
  value: JsonValue;
  disabled?: boolean;
  parserSelectionOnly?: boolean;
  onChange: (value: JsonValue) => void;
}

const ParserSelectionContext = createContext(false);

export function SchemaForm({
  node,
  value,
  disabled = false,
  parserSelectionOnly = false,
  onChange,
}: SchemaFormProps) {
  return (
    <ParserSelectionContext.Provider value={parserSelectionOnly}>
      <NodeEditor
        node={node}
        value={value}
        disabled={disabled}
        onChange={onChange}
      />
    </ParserSelectionContext.Provider>
  );
}

export function ParserDetailsForm({
  node,
  value,
  disabled = false,
  onChange,
}: SchemaFormProps) {
  if (node.kind !== "object") return null;
  const parserEntry = Object.entries(node.properties).find(
    ([, child]) => child.xUi.widget === "parser",
  );
  if (parserEntry === undefined) return null;
  const [name, parserNode] = parserEntry;
  if (parserNode.kind !== "union") return null;
  const object = isObject(value) ? value : {};
  const parserValue = object[name];
  const selected =
    parserValue === undefined
      ? undefined
      : parserNode.branches.find((branch) =>
          branchMatches(branch, parserValue),
        );
  if (
    selected === undefined ||
    selected.constant !== undefined ||
    !nodeHasEditableContent(selected.node)
  )
    return null;
  return (
    <section class="parser-details-card">
      <div class="section-heading">
        <div>
          <small>PARSER</small>
          <h2>{selected.label} configuration</h2>
        </div>
      </div>
      <NodeEditor
        node={selected.node}
        value={parserValue ?? createValue(selected.node)}
        disabled={disabled}
        onChange={(next) => onChange({ ...object, [name]: next })}
      />
    </section>
  );
}

function DisclosureSummary({ children }: { children: ComponentChildren }) {
  return (
    <summary
      onClick={(event) => {
        if (event.detail > 0) {
          const summary = event.currentTarget;
          queueMicrotask(() => summary.blur());
        }
      }}
    >
      {children}
    </summary>
  );
}

function NodeEditor({ node, value, disabled, onChange }: SchemaFormProps) {
  const isDisabled = disabled ?? false;
  const parserSelectionOnly = useContext(ParserSelectionContext);
  if (node.kind === "object" && isJsonParserContainer(node))
    return (
      <JsonParserEditor
        node={node}
        value={value}
        disabled={isDisabled}
        onChange={onChange}
      />
    );
  if (node.kind === "object" && node.xUi.widget === "system_columns")
    return (
      <SystemColumnsEditor
        node={node}
        value={value}
        disabled={isDisabled}
        onChange={onChange}
      />
    );
  if (node.kind === "array" && node.xUi.widget === "partition_ranges")
    return (
      <PartitionRangesInput
        value={value}
        disabled={isDisabled}
        onChange={onChange}
      />
    );
  switch (node.kind) {
    case "object": {
      const object = isObject(value) ? value : {};
      const hasColumnMappings = Object.values(node.properties).some(
        (child) => child.xUi.widget === "column_mappings",
      );
      const visible = Object.entries(node.properties).filter(
        ([name, child]) =>
          !(hasColumnMappings && name === "keys") &&
          child.xUi.widget !== "hidden" &&
          !(
            ["type", "action"].includes(name) &&
            child.kind === "string" &&
            child.enumValues?.length === 1
          ),
      );
      const regular = visible.filter(
        ([, child]) =>
          child.xUi.section !== "advanced" &&
          child.xUi.section !== "system_columns",
      );
      const advanced = visible.filter(
        ([, child]) => child.xUi.section === "advanced",
      );
      const systemColumns = visible.filter(
        ([, child]) => child.xUi.section === "system_columns",
      );
      return (
        <div class="schema-object">
          {regular.map(([name, child]) =>
            child.xUi.widget === "column_mappings" && child.kind === "array" ? (
              <ColumnMappingsEditor
                key={name}
                node={child.item}
                value={Array.isArray(object[name]) ? object[name] : []}
                keys={stringArray(object.keys)}
                additionalKeyOptions={[]}
                disabled={isDisabled}
                onChange={(columns, keys) =>
                  onChange({
                    ...object,
                    [name]: columns,
                    keys,
                  })
                }
              />
            ) : (
              <PropertyEditor
                key={name}
                name={name}
                node={child}
                required={node.required.has(name)}
                value={object[name]}
                disabled={isDisabled}
                onChange={(next) => onChange({ ...object, [name]: next })}
              />
            ),
          )}
          {advanced.length > 0 && (
            <details class="foldout">
              <DisclosureSummary>Advanced settings</DisclosureSummary>
              <div class="foldout-content">
                {advanced.map(([name, child]) => (
                  <PropertyEditor
                    key={name}
                    name={name}
                    node={child}
                    required={node.required.has(name)}
                    value={object[name]}
                    disabled={isDisabled}
                    onChange={(next) => onChange({ ...object, [name]: next })}
                  />
                ))}
              </div>
            </details>
          )}
          {systemColumns.length > 0 && (
            <details class="foldout system-columns">
              <DisclosureSummary>Add system columns</DisclosureSummary>
              <div class="foldout-content">
                {systemColumns.map(([name, child]) => (
                  <PropertyEditor
                    key={name}
                    name={name}
                    node={child}
                    required={node.required.has(name)}
                    value={object[name]}
                    disabled={isDisabled}
                    onChange={(next) => onChange({ ...object, [name]: next })}
                  />
                ))}
              </div>
            </details>
          )}
        </div>
      );
    }
    case "array": {
      const items = Array.isArray(value) ? value : [];
      return (
        <div class="array-editor">
          {items.map((item, index) => (
            <div class="array-row" key={index}>
              <span class="array-index">{index + 1}</span>
              <div class="array-value">
                <NodeEditor
                  node={node.item}
                  value={item}
                  disabled={isDisabled}
                  onChange={(next) => {
                    const copy = [...items];
                    copy[index] = next;
                    onChange(copy);
                  }}
                />
              </div>
              <button
                class="icon-button danger"
                type="button"
                title="Remove"
                disabled={isDisabled}
                onClick={() =>
                  onChange(items.filter((_, itemIndex) => itemIndex !== index))
                }
              >
                <TrashIcon />
              </button>
            </div>
          ))}
          <button
            class="icon-button add"
            type="button"
            title="Add"
            disabled={isDisabled}
            onClick={() => onChange([...items, createValue(node.item)])}
          >
            +
          </button>
        </div>
      );
    }
    case "union": {
      const selected = node.branches.findIndex((branch) =>
        branchMatches(branch, value),
      );
      return (
        <div class="union-editor">
          <SelectControl
            value={selected < 0 ? "" : String(selected)}
            disabled={isDisabled}
            placeholder="Not selected"
            options={node.branches.map((branch, index) => ({
              value: String(index),
              label: branch.label,
            }))}
            onChange={(raw) => {
              const branch = node.branches[Number(raw)];
              if (branch === undefined) return;
              onChange(branch.constant ?? createValue(branch.node));
            }}
          />
          {(!parserSelectionOnly || node.xUi.widget !== "parser") &&
            selected >= 0 &&
            node.branches[selected]!.constant === undefined &&
            nodeHasEditableContent(node.branches[selected]!.node) && (
              <div class="nested-section">
                <NodeEditor
                  node={node.branches[selected]!.node}
                  value={value}
                  disabled={isDisabled}
                  onChange={onChange}
                />
              </div>
            )}
        </div>
      );
    }
    case "nullable": {
      const enabled = value !== null && value !== undefined;
      return (
        <div class="nullable-control">
          <label class="toggle">
            <input
              type="checkbox"
              aria-label="Enable optional settings"
              checked={enabled}
              disabled={isDisabled}
              onChange={(event) =>
                onChange(
                  event.currentTarget.checked ? createValue(node.inner) : null,
                )
              }
            />
          </label>
          {enabled && (
            <div class="nested-section">
              <NodeEditor
                node={node.inner}
                value={value}
                disabled={isDisabled}
                onChange={onChange}
              />
            </div>
          )}
        </div>
      );
    }
    case "boolean":
      return (
        <label class="toggle">
          <input
            type="checkbox"
            checked={value === true}
            disabled={isDisabled}
            onChange={(event) => onChange(event.currentTarget.checked)}
          />
        </label>
      );
    case "number":
      if (node.xUi.widget === "byte_size")
        return (
          <ByteSizeInput
            value={typeof value === "number" ? value : 0}
            disabled={isDisabled}
            onChange={onChange}
          />
        );
      return (
        <input
          type="number"
          value={typeof value === "number" ? value : ""}
          min={node.minimum}
          max={node.maximum}
          step={node.integer ? 1 : "any"}
          disabled={isDisabled}
          onInput={(event) => {
            const parsed = Number(event.currentTarget.value);
            if (Number.isFinite(parsed)) onChange(parsed);
          }}
        />
      );
    case "string":
      if (node.enumValues !== undefined) {
        return (
          <SelectControl
            value={typeof value === "string" ? value : ""}
            disabled={isDisabled}
            placeholder="Not selected"
            options={node.enumValues.map((option) => ({
              value: String(option),
              label: uiLabel(node, String(option)),
            }))}
            onChange={onChange}
          />
        );
      }
      return node.xUi.widget === "password" ? (
        <PasswordInput
          value={typeof value === "string" ? value : ""}
          disabled={isDisabled}
          onChange={onChange}
        />
      ) : (
        <input
          type="text"
          value={typeof value === "string" ? value : ""}
          disabled={isDisabled}
          onInput={(event) => onChange(event.currentTarget.value)}
        />
      );
  }
}

function isJsonParserContainer(
  node: Extract<CompiledNode, { kind: "object" }>,
): boolean {
  const common = node.properties.common;
  const parser = node.properties.json_parser;
  return (
    common?.kind === "object" &&
    parser?.kind === "object" &&
    parser.properties.columns?.kind === "array" &&
    parser.properties.columns.xUi.widget === "column_mappings"
  );
}

function JsonParserEditor({
  node,
  value,
  disabled,
  onChange,
}: {
  node: Extract<CompiledNode, { kind: "object" }>;
  value: JsonValue;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const object = isObject(value) ? value : {};
  const commonNode = node.properties.common;
  const parserNode = node.properties.json_parser;
  if (commonNode?.kind !== "object" || parserNode?.kind !== "object")
    return null;
  const common = isObject(object.common) ? object.common : {};
  const parser = isObject(object.json_parser) ? object.json_parser : {};
  const columnsNode = parserNode.properties.columns;
  if (columnsNode?.kind !== "array") return null;
  const updateCommon = (name: string, next: JsonValue) =>
    onChange({ ...object, common: { ...common, [name]: next } });
  const updateParser = (name: string, next: JsonValue) =>
    onChange({ ...object, json_parser: { ...parser, [name]: next } });
  const systemColumns = isObject(common.system_columns)
    ? Object.values(common.system_columns).filter(
        (name): name is string => typeof name === "string" && name !== "",
      )
    : [];
  const commonFields = Object.entries(commonNode.properties).filter(
    ([name]) => name !== "table_naming" && name !== "system_columns",
  );
  const parserFields = Object.entries(parserNode.properties).filter(
    ([name]) => !["json_framing", "columns", "keys"].includes(name),
  );
  return (
    <div class="schema-object json-parser-editor">
      <section class="parser-common-section">
        <h3>Parser settings</h3>
        {commonNode.properties.table_naming && (
          <PropertyEditor
            name="table_naming"
            node={commonNode.properties.table_naming}
            required={commonNode.required.has("table_naming")}
            value={common.table_naming}
            disabled={disabled}
            onChange={(next) => updateCommon("table_naming", next)}
          />
        )}
        {parserNode.properties.json_framing && (
          <PropertyEditor
            name="json_framing"
            node={parserNode.properties.json_framing}
            required={parserNode.required.has("json_framing")}
            value={parser.json_framing}
            disabled={disabled}
            onChange={(next) => updateParser("json_framing", next)}
          />
        )}
        {commonFields.map(([name, child]) => (
          <PropertyEditor
            key={name}
            name={name}
            node={child}
            required={commonNode.required.has(name)}
            value={common[name]}
            disabled={disabled}
            onChange={(next) => updateCommon(name, next)}
          />
        ))}
      </section>
      <ColumnMappingsEditor
        node={columnsNode.item}
        value={Array.isArray(parser.columns) ? parser.columns : []}
        keys={stringArray(parser.keys)}
        additionalKeyOptions={systemColumns}
        {...(commonNode.properties.system_columns === undefined
          ? {}
          : {
              systemColumns: {
                node: commonNode.properties.system_columns,
                value: common.system_columns,
                onChange: (next: JsonValue) =>
                  updateCommon("system_columns", next),
              },
            })}
        disabled={disabled}
        onChange={(columns, keys) =>
          onChange({
            ...object,
            json_parser: { ...parser, columns, keys },
          })
        }
      />
      {parserFields.length > 0 && (
        <section class="parser-secondary-section">
          <h3>Parsing behavior</h3>
          <div class="schema-object">
            {parserFields.map(([name, child]) => (
              <PropertyEditor
                key={name}
                name={name}
                node={child}
                required={parserNode.required.has(name)}
                value={parser[name]}
                disabled={disabled}
                onChange={(next) => updateParser(name, next)}
              />
            ))}
          </div>
        </section>
      )}
    </div>
  );
}

interface PropertyEditorProps {
  name: string;
  node: CompiledNode;
  required: boolean;
  value: JsonValue | undefined;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}

function PropertyEditor({
  name,
  node,
  required,
  value,
  disabled,
  onChange,
}: PropertyEditorProps) {
  const effective = value ?? createValue(node);
  const identifier = `field-${name.replaceAll("_", "-")}`;
  const controlWidth = controlWidthClass(name, node);
  if (node.xUi.widget === "parser_common")
    return (
      <section class="parser-common-section">
        <h3>{node.title ?? "Parser settings"}</h3>
        <NodeEditor
          node={node}
          value={effective}
          disabled={disabled}
          onChange={onChange}
        />
      </section>
    );
  if (node.xUi.widget === "system_columns")
    return (
      <details class="foldout system-columns unified-system-columns">
        <DisclosureSummary>Add system columns</DisclosureSummary>
        <div class="foldout-content">
          <NodeEditor
            node={node}
            value={effective}
            disabled={disabled}
            onChange={onChange}
          />
        </div>
      </details>
    );
  if (node.kind === "array" && (name === "topics" || name === "hosts"))
    return (
      <div class="form-row form-row-wide compact-array-field">
        <label class="field-label">
          <span>
            {node.title ?? humanize(name)}
            {!required && <small class="optional">(optional)</small>}
          </span>
        </label>
        <CompactArrayEditor
          name={name}
          node={node}
          value={Array.isArray(value) ? value : []}
          disabled={disabled}
          onChange={onChange}
        />
      </div>
    );
  return (
    <div
      class={`form-row ${node.kind === "object" || (node.kind === "array" && node.xUi.widget !== "partition_ranges") || node.xUi.widget === "parser" ? "form-row-wide" : ""} ${node.kind === "nullable" ? "form-row-nullable" : ""} ${controlWidth}`}
    >
      <label class="field-label" for={identifier}>
        <span>
          {node.title ?? humanize(name)}
          {!required && <small class="optional">(optional)</small>}
        </span>
        {node.description &&
          name !== "json_parser" &&
          node.xUi.widget !== "parser" && (
            <span class="help" tabindex={0} data-tooltip={node.description}>
              ?
            </span>
          )}
      </label>
      <div class="field-control" id={identifier}>
        <NodeEditor
          node={node}
          value={effective}
          disabled={disabled}
          onChange={onChange}
        />
      </div>
    </div>
  );
}

function controlWidthClass(name: string, node: CompiledNode): string {
  if (node.xUi.widget === "parser") return "control-width-parser";
  if (name === "auth") return "control-width-auth";
  if (name === "json_framing") return "control-width-medium";
  if (name === "table_naming") return "control-width-table-name";
  if (
    node.kind === "union" ||
    (node.kind === "string" && node.enumValues !== undefined)
  )
    return "control-width-enum";
  return "";
}

interface SelectControlProps {
  value: string;
  placeholder: string;
  options: Array<{ value: string; label: string }>;
  disabled?: boolean;
  searchable?: boolean;
  onChange: (value: string) => void;
}

export function SelectControl({
  value,
  placeholder,
  options,
  disabled = false,
  searchable = false,
  onChange,
}: SelectControlProps) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const root = useRef<HTMLDivElement>(null);
  const selected = options.find((option) => option.value === value);
  const filtered = useMemo(
    () =>
      options.filter((option) =>
        option.label.toLowerCase().includes(query.toLowerCase()),
      ),
    [options, query],
  );
  useEffect(() => {
    if (!open) return;
    const closeOutside = (event: MouseEvent) => {
      if (!root.current?.contains(event.target as Node)) {
        setOpen(false);
        setQuery("");
      }
    };
    document.addEventListener("mousedown", closeOutside);
    return () => document.removeEventListener("mousedown", closeOutside);
  }, [open]);
  return (
    <div ref={root} class={`select ${open ? "open" : ""}`}>
      <button
        type="button"
        class="select-trigger"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        onPointerDown={dismissActiveTextSelection}
        onClick={() => setOpen((current) => !current)}
      >
        <span class={selected === undefined ? "placeholder" : ""}>
          {selected?.label ?? placeholder}
        </span>
        <svg
          class="chevron"
          viewBox="0 0 16 16"
          aria-hidden="true"
          focusable="false"
        >
          <path d="m3.5 6 4.5 4 4.5-4" />
        </svg>
      </button>
      {open && (
        <div class="select-menu">
          {searchable && (
            <input
              class="select-search"
              type="search"
              placeholder="Search"
              value={query}
              onInput={(event) => setQuery(event.currentTarget.value)}
            />
          )}
          <div role="listbox">
            {filtered.map((option) => (
              <button
                type="button"
                role="option"
                aria-selected={option.value === value}
                class="select-option"
                onPointerDown={dismissActiveTextSelection}
                onClick={() => {
                  onChange(option.value);
                  setOpen(false);
                  setQuery("");
                }}
              >
                {option.label}
              </button>
            ))}
            {filtered.length === 0 && (
              <div class="select-empty">No matches</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function MultiSelectControl({
  values,
  placeholder,
  options,
  disabled,
  onChange,
}: {
  values: string[];
  placeholder: string;
  options: Array<{ value: string; label: string }>;
  disabled: boolean;
  onChange: (values: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const closeOutside = (event: MouseEvent) => {
      if (!root.current?.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", closeOutside);
    return () => document.removeEventListener("mousedown", closeOutside);
  }, [open]);
  const labels = values.map(
    (value) => options.find((option) => option.value === value)?.label ?? value,
  );
  return (
    <div ref={root} class={`select multi-select ${open ? "open" : ""}`}>
      <button
        type="button"
        class="select-trigger"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        onPointerDown={dismissActiveTextSelection}
        onClick={() => setOpen((current) => !current)}
      >
        <span class={labels.length === 0 ? "placeholder" : ""}>
          {labels.length === 0 ? placeholder : labels.join(", ")}
        </span>
        <svg class="chevron" viewBox="0 0 16 16" aria-hidden="true">
          <path d="m3.5 6 4.5 4 4.5-4" />
        </svg>
      </button>
      {open && (
        <div class="select-menu" role="listbox" aria-multiselectable="true">
          {options.map((option) => {
            const selected = values.includes(option.value);
            return (
              <button
                type="button"
                role="option"
                aria-selected={selected}
                class="select-option multi-select-option"
                onPointerDown={dismissActiveTextSelection}
                onClick={() =>
                  onChange(
                    selected
                      ? values.filter((value) => value !== option.value)
                      : [...values, option.value],
                  )
                }
              >
                <span class={`multi-check ${selected ? "checked" : ""}`}>
                  {selected ? "✓" : ""}
                </span>
                {option.label}
              </button>
            );
          })}
          {options.length === 0 && (
            <div class="select-empty">Add output columns first</div>
          )}
        </div>
      )}
    </div>
  );
}

function dismissActiveTextSelection(): void {
  const active = document.activeElement;
  if (
    active instanceof HTMLInputElement ||
    active instanceof HTMLTextAreaElement
  ) {
    const caret = active.selectionEnd;
    if (caret !== null) active.setSelectionRange(caret, caret);
    active.blur();
  }
  window.getSelection()?.removeAllRanges();
}

function ColumnMappingsEditor({
  node,
  value,
  keys,
  additionalKeyOptions,
  systemColumns,
  disabled,
  onChange,
}: {
  node: CompiledNode;
  value: JsonValue[];
  keys: string[];
  additionalKeyOptions: string[];
  systemColumns?: {
    node: CompiledNode;
    value: JsonValue | undefined;
    onChange: (value: JsonValue) => void;
  };
  disabled: boolean;
  onChange: (columns: JsonValue[], keys: string[]) => void;
}) {
  const [expandedSettings, setExpandedSettings] = useState<Set<number>>(
    () => new Set(),
  );
  const [selectedRows, setSelectedRows] = useState<Set<number>>(
    () => new Set(),
  );
  const [systemColumnsOpen, setSystemColumnsOpen] = useState(false);
  useEffect(() => {
    setSelectedRows((current) => {
      const next = new Set(
        [...current].filter((index) => index < value.length),
      );
      return next.size === current.size ? current : next;
    });
  }, [value.length]);
  if (node.kind !== "object")
    return (
      <NodeEditor
        node={{ kind: "array", item: node, xUi: {} }}
        value={value}
        disabled={disabled}
        onChange={(next) => onChange(Array.isArray(next) ? next : [], keys)}
      />
    );
  const updateColumn = (index: number, next: JsonObject) => {
    const previous = isObject(value[index]) ? value[index] : {};
    const oldName =
      typeof previous.column_name === "string" ? previous.column_name : "";
    const newName =
      typeof next.column_name === "string" ? next.column_name : "";
    const oldJsonType = previous.json_data_type;
    if (
      typeof next.json_data_type === "string" &&
      next.json_data_type !== oldJsonType
    ) {
      next = {
        ...next,
        arrow_type: closestArrowType(next.json_data_type),
      };
    }
    if (!isStringArrowType(next.arrow_type))
      next = { ...next, low_cardinality: false };
    if (
      newName !== oldName &&
      (previous.jsonpath === "" || previous.jsonpath === `$.${oldName}`)
    )
      next = { ...next, jsonpath: newName === "" ? "" : `$.${newName}` };
    const columns = [...value];
    columns[index] = next;
    const nextKeys =
      newName === oldName
        ? keys
        : keys.map((key) => (key === oldName ? newName : key)).filter(Boolean);
    onChange(columns, nextKeys);
  };
  const toggleSettings = (index: number) =>
    setExpandedSettings((current) => {
      const next = new Set(current);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });
  const duplicateColumn = (index: number) => {
    const columns = [...value];
    columns.splice(index + 1, 0, structuredClone(value[index]!));
    setExpandedSettings(new Set());
    setSelectedRows(new Set());
    onChange(columns, keys);
  };
  const deleteColumn = (index: number, name: string) => {
    setExpandedSettings(new Set());
    setSelectedRows(new Set());
    onChange(
      value.filter((_, itemIndex) => itemIndex !== index),
      keys.filter((key) => key !== name),
    );
  };
  const toggleRowSelection = (index: number) =>
    setSelectedRows((current) => {
      const next = new Set(current);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });
  const selectAllRows = (selected: boolean) =>
    setSelectedRows(
      selected ? new Set(value.map((_, index) => index)) : new Set(),
    );
  const deleteSelectedRows = () => {
    const deletedNames = new Set(
      [...selectedRows].flatMap((index) => {
        const column = isObject(value[index]) ? value[index] : {};
        return typeof column.column_name === "string" &&
          column.column_name !== ""
          ? [column.column_name]
          : [];
      }),
    );
    setExpandedSettings(new Set());
    setSelectedRows(new Set());
    onChange(
      value.filter((_, index) => !selectedRows.has(index)),
      keys.filter((key) => !deletedNames.has(key)),
    );
  };
  const showLowCardinality = node.properties.low_cardinality !== undefined;
  const allRowsSelected =
    value.length > 0 && selectedRows.size === value.length;
  const someRowsSelected = selectedRows.size > 0;
  return (
    <div class="column-editor">
      <div class="column-editor-heading">
        <div>
          <small>DATA SCHEMA</small>
          <h3>Output columns</h3>
        </div>
        <div class="column-editor-actions">
          {someRowsSelected && (
            <div class="bulk-selection-toolbar" role="status">
              <span>{selectedRows.size} selected</span>
              <button
                type="button"
                class="bulk-delete"
                aria-label={`Delete ${selectedRows.size} selected ${selectedRows.size === 1 ? "column" : "columns"}`}
                title="Delete selected columns"
                disabled={disabled}
                onClick={deleteSelectedRows}
              >
                <TrashIcon />
              </button>
            </div>
          )}
          {systemColumns && (
            <button
              type="button"
              class="inline-disclosure"
              aria-expanded={systemColumnsOpen}
              disabled={disabled}
              onClick={(event) => {
                setSystemColumnsOpen((current) => !current);
                if (event.detail > 0) {
                  const button = event.currentTarget;
                  queueMicrotask(() => button.blur());
                }
              }}
            >
              Add system columns
            </button>
          )}
          <button
            class="add-row-button"
            type="button"
            disabled={disabled}
            onClick={() => onChange([...value, createValue(node)], keys)}
          >
            + Add column
          </button>
        </div>
      </div>
      {systemColumns && systemColumnsOpen && (
        <section class="schema-system-columns-panel">
          <div class="subsection-heading">
            <h4>System columns</h4>
            <button
              type="button"
              aria-label="Close system columns"
              onClick={() => setSystemColumnsOpen(false)}
            >
              ×
            </button>
          </div>
          <NodeEditor
            node={systemColumns.node}
            value={systemColumns.value ?? createValue(systemColumns.node)}
            disabled={disabled}
            onChange={systemColumns.onChange}
          />
        </section>
      )}
      <div class="table-shell">
        <table class="config-table column-table">
          <thead>
            <tr>
              <th class="selection-column">
                <IndeterminateCheckbox
                  ariaLabel="Select all output columns"
                  checked={allRowsSelected}
                  indeterminate={someRowsSelected && !allRowsSelected}
                  disabled={disabled || value.length === 0}
                  onChange={selectAllRows}
                />
              </th>
              <th>Column</th>
              <th>JSON path</th>
              <th>JSON type</th>
              <th>Arrow type</th>
              <th class="flag-column">Not null</th>
              {showLowCardinality && (
                <th class="flag-column">Low cardinality</th>
              )}
              <th class="actions-column">Actions</th>
            </tr>
          </thead>
          <tbody>
            {value.map((raw, index) => {
              const column = isObject(raw) ? raw : {};
              const name =
                typeof column.column_name === "string"
                  ? column.column_name
                  : "";
              const mainFields = [
                "column_name",
                "jsonpath",
                "json_data_type",
                "arrow_type",
              ];
              const extraFields = Object.entries(node.properties).filter(
                ([field]) =>
                  ![...mainFields, "nullable", "low_cardinality"].includes(
                    field,
                  ),
              );
              const hasCustomSettings = extraFields.some(
                ([field, child]) =>
                  column[field] !== undefined &&
                  !jsonValuesEqual(column[field]!, createValue(child)),
              );
              const settingsExpanded = expandedSettings.has(index);
              const selected = selectedRows.has(index);
              return (
                <Fragment key={index}>
                  <tr class={`config-table-row ${selected ? "selected" : ""}`}>
                    <td class="selection-column">
                      <input
                        type="checkbox"
                        aria-label={`Select output column ${index + 1}`}
                        checked={selected}
                        disabled={disabled}
                        onChange={() => toggleRowSelection(index)}
                      />
                    </td>
                    {mainFields.map((field) => (
                      <td key={field}>
                        {node.properties[field] && (
                          <NodeEditor
                            node={node.properties[field]!}
                            value={
                              column[field] ??
                              createValue(node.properties[field]!)
                            }
                            disabled={disabled}
                            onChange={(next) =>
                              updateColumn(index, {
                                ...column,
                                [field]: next,
                              })
                            }
                          />
                        )}
                      </td>
                    ))}
                    <td class="flag-column">
                      <input
                        type="checkbox"
                        aria-label={`Column ${index + 1} not null`}
                        disabled={disabled}
                        checked={column.nullable !== true}
                        onChange={(event) =>
                          updateColumn(index, {
                            ...column,
                            nullable: !event.currentTarget.checked,
                          })
                        }
                      />
                    </td>
                    {showLowCardinality && (
                      <td
                        class={`flag-column tooltip-host ${isStringArrowType(column.arrow_type) ? "" : "disabled"}`}
                        data-tooltip="Low cardinality is meaningful only for string values"
                      >
                        <input
                          type="checkbox"
                          aria-label={`Column ${index + 1} low cardinality`}
                          disabled={
                            disabled || !isStringArrowType(column.arrow_type)
                          }
                          checked={column.low_cardinality === true}
                          onChange={(event) =>
                            updateColumn(index, {
                              ...column,
                              low_cardinality: event.currentTarget.checked,
                            })
                          }
                        />
                      </td>
                    )}
                    <td class="actions-column">
                      <ColumnActions
                        row={index + 1}
                        disabled={disabled}
                        hasSettings={extraFields.length > 0}
                        hasCustomSettings={hasCustomSettings}
                        settingsExpanded={settingsExpanded}
                        onSettings={() => toggleSettings(index)}
                        onDuplicate={() => duplicateColumn(index)}
                        onDelete={() => deleteColumn(index, name)}
                      />
                    </td>
                  </tr>
                  {extraFields.length > 0 && settingsExpanded && (
                    <tr class="table-details-row">
                      <td />
                      <td colSpan={showLowCardinality ? 7 : 6}>
                        <section class="column-details">
                          <div class="column-details-heading">
                            <strong>Advanced column settings</strong>
                            <button
                              type="button"
                              aria-label={`Close column ${index + 1} settings`}
                              onClick={() => toggleSettings(index)}
                            >
                              ×
                            </button>
                          </div>
                          <div class="schema-object">
                            {extraFields.map(([field, child]) => (
                              <PropertyEditor
                                name={field}
                                node={child}
                                required={node.required.has(field)}
                                value={column[field]}
                                disabled={disabled}
                                onChange={(next) =>
                                  updateColumn(index, {
                                    ...column,
                                    [field]: next,
                                  })
                                }
                              />
                            ))}
                          </div>
                        </section>
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      {value.length === 0 && (
        <p class="empty-columns">
          Add the first output column to define the parsed data schema.
        </p>
      )}
      <div class="column-keys">
        <span class="field-label">
          Keys <small class="optional">(optional)</small>
        </span>
        <MultiSelectControl
          values={keys}
          disabled={disabled}
          placeholder="Not selected"
          options={uniqueStrings([
            ...value.flatMap((raw) => {
              const column = isObject(raw) ? raw : {};
              return typeof column.column_name === "string" &&
                column.column_name !== ""
                ? [column.column_name]
                : [];
            }),
            ...additionalKeyOptions,
            ...keys,
          ]).map((name) => ({ value: name, label: name }))}
          onChange={(next) => onChange(value, next)}
        />
      </div>
    </div>
  );
}

function IndeterminateCheckbox({
  ariaLabel,
  checked,
  indeterminate,
  disabled,
  onChange,
}: {
  ariaLabel: string;
  checked: boolean;
  indeterminate: boolean;
  disabled: boolean;
  onChange: (checked: boolean) => void;
}) {
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (input.current) input.current.indeterminate = indeterminate;
  }, [indeterminate]);
  return (
    <input
      ref={input}
      type="checkbox"
      aria-label={ariaLabel}
      checked={checked}
      disabled={disabled}
      onChange={(event) => onChange(event.currentTarget.checked)}
    />
  );
}

function ColumnActions({
  row,
  disabled,
  hasSettings,
  hasCustomSettings,
  settingsExpanded,
  onSettings,
  onDuplicate,
  onDelete,
}: {
  row: number;
  disabled: boolean;
  hasSettings: boolean;
  hasCustomSettings: boolean;
  settingsExpanded: boolean;
  onSettings: () => void;
  onDuplicate: () => void;
  onDelete: () => void;
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const closeOutside = (event: MouseEvent) => {
      if (!root.current?.contains(event.target as Node)) setOpen(false);
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", closeOutside);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("mousedown", closeOutside);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, [open]);
  const run = (action: () => void) => {
    setOpen(false);
    action();
  };
  return (
    <div class={`row-actions ${open ? "open" : ""}`} ref={root}>
      <button
        class="row-action"
        type="button"
        aria-label={`Column ${row} actions`}
        aria-haspopup="menu"
        aria-expanded={open}
        disabled={disabled}
        onClick={() => setOpen((current) => !current)}
      >
        <span aria-hidden="true">⋯</span>
        {hasCustomSettings && (
          <span class="custom-settings-dot" title="Custom column settings" />
        )}
      </button>
      {open && (
        <div class="row-actions-menu" role="menu">
          {hasSettings && (
            <button
              type="button"
              role="menuitem"
              onClick={() => run(onSettings)}
            >
              Column settings{settingsExpanded ? " ✓" : ""}
            </button>
          )}
          <button
            type="button"
            role="menuitem"
            onClick={() => run(onDuplicate)}
          >
            Duplicate
          </button>
          <button
            class="danger"
            type="button"
            role="menuitem"
            onClick={() => run(onDelete)}
          >
            Delete
          </button>
        </div>
      )}
    </div>
  );
}

function jsonValuesEqual(left: JsonValue, right: JsonValue): boolean {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) && Array.isArray(right))
    return (
      left.length === right.length &&
      left.every((value, index) => jsonValuesEqual(value, right[index]!))
    );
  if (isObject(left) && isObject(right)) {
    const leftKeys = Object.keys(left);
    const rightKeys = Object.keys(right);
    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every(
        (key) => key in right && jsonValuesEqual(left[key]!, right[key]!),
      )
    );
  }
  return false;
}

function CompactArrayEditor({
  name,
  node,
  value,
  disabled,
  onChange,
}: {
  name: "topics" | "hosts";
  node: Extract<CompiledNode, { kind: "array" }>;
  value: JsonValue[];
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const fields =
    node.item.kind === "object"
      ? Object.entries(node.item.properties).filter(
          ([, child]) => child.xUi.widget !== "hidden",
        )
      : [];
  const singular = name === "topics" ? "topic" : "host";
  const updateItem = (index: number, next: JsonValue) => {
    const items = [...value];
    items[index] = next;
    onChange(items);
  };
  return (
    <div class="compact-array-editor">
      <div class="table-shell">
        <table class="config-table compact-array-table">
          <thead>
            <tr>
              <th class="row-number" aria-label="Row" />
              {fields.length > 0 ? (
                fields.map(([field, child]) => (
                  <th key={field}>{child.title ?? humanize(field)}</th>
                ))
              ) : (
                <th>{humanize(singular)}</th>
              )}
              <th class="actions-column">Actions</th>
            </tr>
          </thead>
          <tbody>
            {value.map((item, index) => {
              const object = isObject(item) ? item : {};
              return (
                <tr class="config-table-row" key={index}>
                  <td class="row-number">{index + 1}</td>
                  {fields.length > 0 ? (
                    fields.map(([field, child]) => (
                      <td key={field}>
                        <NodeEditor
                          node={child}
                          value={object[field] ?? createValue(child)}
                          disabled={disabled}
                          onChange={(next) =>
                            updateItem(index, { ...object, [field]: next })
                          }
                        />
                      </td>
                    ))
                  ) : (
                    <td>
                      <NodeEditor
                        node={node.item}
                        value={item}
                        disabled={disabled}
                        onChange={(next) => updateItem(index, next)}
                      />
                    </td>
                  )}
                  <td class="actions-column">
                    <button
                      class="row-action danger"
                      type="button"
                      title={`Remove ${singular}`}
                      aria-label={`Remove ${singular} ${index + 1}`}
                      disabled={disabled}
                      onClick={() =>
                        onChange(
                          value.filter((_, itemIndex) => itemIndex !== index),
                        )
                      }
                    >
                      <TrashIcon />
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <button
        class="add-row-button"
        type="button"
        disabled={disabled}
        onClick={() => onChange([...value, createValue(node.item)])}
      >
        + Add {singular}
      </button>
    </div>
  );
}

function PasswordInput({
  value,
  disabled,
  onChange,
}: {
  value: string;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const [visible, setVisible] = useState(false);
  return (
    <div class="password-control">
      <input
        type={visible ? "text" : "password"}
        value={value}
        disabled={disabled}
        onInput={(event) => onChange(event.currentTarget.value)}
      />
      <button
        type="button"
        class="password-reveal"
        aria-label={visible ? "Hide secret" : "Show secret"}
        aria-pressed={visible}
        disabled={disabled}
        onClick={() => setVisible((current) => !current)}
      >
        {visible ? <EyeOffIcon /> : <EyeIcon />}
      </button>
    </div>
  );
}

const SYSTEM_COLUMN_DEFAULTS: Record<string, string> = {
  topic: "_system_topic",
  partition: "_system_partition",
  offset: "_system_offset",
  message_index: "_system_message_index",
  write_timestamp_ms: "_system_write_timestamp_ms",
};

function SystemColumnsEditor({
  node,
  value,
  disabled,
  onChange,
}: {
  node: Extract<CompiledNode, { kind: "object" }>;
  value: JsonValue;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const object = isObject(value) ? value : {};
  return (
    <div class="system-column-list">
      {Object.keys(node.properties).map((name) => {
        const configured = typeof object[name] === "string";
        const columnName = configured
          ? String(object[name])
          : (SYSTEM_COLUMN_DEFAULTS[name] ?? `_system_${name}`);
        return (
          <div class="system-column-row" key={name}>
            <span class="system-column-label">
              {humanize(name)}
              {node.properties[name]?.description && (
                <span
                  class="help"
                  tabindex={0}
                  data-tooltip={node.properties[name]!.description}
                >
                  ?
                </span>
              )}
            </span>
            <input
              type="checkbox"
              checked={configured}
              disabled={disabled}
              aria-label={`Include ${humanize(name)}`}
              onChange={(event) =>
                onChange({
                  ...object,
                  [name]: event.currentTarget.checked ? columnName : null,
                })
              }
            />
            <input
              type="text"
              value={columnName}
              disabled={disabled || !configured}
              aria-label={`${humanize(name)} column name`}
              onInput={(event) =>
                onChange({ ...object, [name]: event.currentTarget.value })
              }
            />
          </div>
        );
      })}
    </div>
  );
}

const BYTE_UNITS = [
  { label: "B", factor: 1 },
  { label: "KiB", factor: 1024 },
  { label: "MiB", factor: 1024 * 1024 },
  { label: "GiB", factor: 1024 * 1024 * 1024 },
] as const;

function ByteSizeInput({
  value,
  disabled,
  onChange,
}: {
  value: number;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const [unitIndex, setUnitIndex] = useState(() => bestByteUnit(value));
  const unit = BYTE_UNITS[unitIndex]!;
  return (
    <div class="byte-size-input">
      <input
        type="number"
        min={0}
        step="any"
        value={value / unit.factor}
        disabled={disabled}
        onInput={(event) => {
          const bytes = Number(event.currentTarget.value) * unit.factor;
          if (Number.isSafeInteger(bytes) && bytes >= 0) onChange(bytes);
        }}
      />
      <SelectControl
        value={String(unitIndex)}
        placeholder="Unit"
        disabled={disabled}
        options={BYTE_UNITS.map((candidate, index) => ({
          value: String(index),
          label: candidate.label,
        }))}
        onChange={(next) => setUnitIndex(Number(next))}
      />
    </div>
  );
}

function bestByteUnit(value: number): number {
  for (let index = BYTE_UNITS.length - 1; index > 0; index -= 1) {
    if (
      value >= BYTE_UNITS[index]!.factor &&
      value % BYTE_UNITS[index]!.factor === 0
    )
      return index;
  }
  return 0;
}

function nodeHasEditableContent(node: CompiledNode): boolean {
  if (node.kind !== "object") return true;
  return Object.entries(node.properties).some(
    ([name, child]) =>
      child.xUi.widget !== "hidden" &&
      !(
        ["type", "action"].includes(name) &&
        child.kind === "string" &&
        child.enumValues?.length === 1
      ),
  );
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values)];
}

function TrashIcon() {
  return (
    <svg
      class="trash-icon"
      viewBox="0 0 16 16"
      fill="currentColor"
      stroke="none"
      aria-hidden="true"
    >
      <path
        fill="currentColor"
        fill-rule="evenodd"
        clip-rule="evenodd"
        d="M9 2H7a.5.5 0 0 0-.5.5V3h3v-.5A.5.5 0 0 0 9 2m2 1v-.5a2 2 0 0 0-2-2H7a2 2 0 0 0-2 2V3H2.251a.75.75 0 0 0 0 1.5h.312l.317 7.625A3 3 0 0 0 5.878 15h4.245a3 3 0 0 0 2.997-2.875l.318-7.625h.312a.75.75 0 0 0 0-1.5zm.936 1.5H4.064l.315 7.562A1.5 1.5 0 0 0 5.878 13.5h4.245a1.5 1.5 0 0 0 1.498-1.438zm-6.186 2v5a.75.75 0 0 0 1.5 0v-5a.75.75 0 0 0-1.5 0m3.75-.75a.75.75 0 0 1 .75.75v5a.75.75 0 0 1-1.5 0v-5a.75.75 0 0 1 .75-.75"
      />
    </svg>
  );
}

function EyeIcon() {
  return (
    <svg class="eye-icon" viewBox="0 0 16 16" aria-hidden="true">
      <path
        fill="currentColor"
        fill-rule="evenodd"
        clip-rule="evenodd"
        d="M1.87 8.515 1.641 8l.229-.515a6.708 6.708 0 0 1 12.26 0l.228.515-.229.515a6.708 6.708 0 0 1-12.259 0M.5 6.876l-.26.585a1.33 1.33 0 0 0 0 1.079l.26.584a8.208 8.208 0 0 0 15 0l.26-.584a1.33 1.33 0 0 0 0-1.08l-.26-.584a8.208 8.208 0 0 0-15 0M9.5 8a1.5 1.5 0 1 1-3 0 1.5 1.5 0 0 1 3 0M11 8a3 3 0 1 1-6 0 3 3 0 0 1 6 0"
      />
    </svg>
  );
}

function EyeOffIcon() {
  return (
    <svg class="eye-icon eye-off-icon" viewBox="0 0 16 16" aria-hidden="true">
      <path
        fill="currentColor"
        fill-rule="evenodd"
        clip-rule="evenodd"
        d="M1.87 8.515 1.641 8l.229-.515a6.708 6.708 0 0 1 12.26 0l.228.515-.229.515a6.708 6.708 0 0 1-12.259 0M.5 6.876l-.26.585a1.33 1.33 0 0 0 0 1.079l.26.584a8.208 8.208 0 0 0 15 0l.26-.584a1.33 1.33 0 0 0 0-1.08l-.26-.584a8.208 8.208 0 0 0-15 0M9.5 8a1.5 1.5 0 1 1-3 0 1.5 1.5 0 0 1 3 0M11 8a3 3 0 1 1-6 0 3 3 0 0 1 6 0"
      />
      <path class="eye-off-slash" d="M2 2l12 12" />
    </svg>
  );
}

function PartitionRangesInput({
  value,
  disabled,
  onChange,
}: {
  value: JsonValue;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const canonical = formatPartitionIds(value);
  const [raw, setRaw] = useState(canonical);
  const [error, setError] = useState<string>();
  useEffect(() => setRaw(canonical), [canonical]);
  return (
    <div class="validated-input">
      <input
        type="text"
        inputMode="numeric"
        placeholder="e.g. 1-5,7"
        value={raw}
        disabled={disabled}
        aria-invalid={error !== undefined}
        onInput={(event) => {
          const next = event.currentTarget.value;
          setRaw(next);
          const parsed = parsePartitionIds(next);
          setError(parsed.error);
          if (parsed.value !== undefined) onChange(parsed.value);
        }}
      />
      {error && <small class="validation-error">{error}</small>}
    </div>
  );
}

function formatPartitionIds(value: JsonValue): string {
  return Array.isArray(value) && value.every((item) => typeof item === "number")
    ? value.join(",")
    : "";
}

function uiLabel(node: CompiledNode, value: string): string {
  const labels = node.xUi.labels;
  return isObject(labels) && typeof labels[value] === "string"
    ? labels[value]
    : humanize(value);
}

function isObject(value: JsonValue | undefined): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringArray(value: JsonValue | undefined): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}
