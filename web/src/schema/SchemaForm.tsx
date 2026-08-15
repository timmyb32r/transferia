import { useEffect, useMemo, useRef, useState } from "preact/hooks";

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
  onChange: (value: JsonValue) => void;
}

export function SchemaForm({
  node,
  value,
  disabled = false,
  onChange,
}: SchemaFormProps) {
  return (
    <NodeEditor
      node={node}
      value={value}
      disabled={disabled}
      onChange={onChange}
    />
  );
}

function NodeEditor({ node, value, disabled, onChange }: SchemaFormProps) {
  const isDisabled = disabled ?? false;
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
              <summary>Advanced settings</summary>
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
              <summary>Add system columns</summary>
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
          {selected >= 0 &&
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
      {commonNode.properties.system_columns && (
        <PropertyEditor
          name="system_columns"
          node={commonNode.properties.system_columns}
          required={commonNode.required.has("system_columns")}
          value={common.system_columns}
          disabled={disabled}
          onChange={(next) => updateCommon("system_columns", next)}
        />
      )}
      <ColumnMappingsEditor
        node={columnsNode.item}
        value={Array.isArray(parser.columns) ? parser.columns : []}
        keys={stringArray(parser.keys)}
        additionalKeyOptions={systemColumns}
        disabled={disabled}
        onChange={(columns, keys) =>
          onChange({
            ...object,
            json_parser: { ...parser, columns, keys },
          })
        }
      />
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
        <summary>Add system columns</summary>
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
  return (
    <div
      class={`form-row ${node.kind === "object" || (node.kind === "array" && node.xUi.widget !== "partition_ranges") || node.xUi.widget === "parser" ? "form-row-wide" : ""} ${node.kind === "nullable" ? "form-row-nullable" : ""}`}
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

function ColumnMappingsEditor({
  node,
  value,
  keys,
  additionalKeyOptions,
  disabled,
  onChange,
}: {
  node: CompiledNode;
  value: JsonValue[];
  keys: string[];
  additionalKeyOptions: string[];
  disabled: boolean;
  onChange: (columns: JsonValue[], keys: string[]) => void;
}) {
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
  return (
    <div class="column-editor">
      <div class="column-editor-heading">
        <div>
          <small>DATA SCHEMA</small>
          <h3>Output columns</h3>
        </div>
        <button
          class="icon-button add"
          type="button"
          title="Add column"
          disabled={disabled}
          onClick={() => onChange([...value, createValue(node)], keys)}
        >
          +
        </button>
      </div>
      {value.map((raw, index) => {
        const column = isObject(raw) ? raw : {};
        const name =
          typeof column.column_name === "string" ? column.column_name : "";
        const mainFields = [
          "column_name",
          "jsonpath",
          "json_data_type",
          "arrow_type",
        ];
        const extraFields = Object.entries(node.properties).filter(
          ([field]) =>
            ![...mainFields, "nullable", "low_cardinality"].includes(field),
        );
        return (
          <div class="column-card" key={index}>
            <div class="column-number">{index + 1}</div>
            <div
              class={`column-main ${node.properties.low_cardinality ? "with-low-cardinality" : ""}`}
            >
              {mainFields.map((field) =>
                node.properties[field] === undefined ? null : (
                  <label>
                    <span>
                      {field === "column_name"
                        ? "Column name"
                        : field === "jsonpath"
                          ? "Path"
                          : humanize(field)}
                    </span>
                    <NodeEditor
                      node={node.properties[field]!}
                      value={
                        column[field] ?? createValue(node.properties[field]!)
                      }
                      disabled={disabled}
                      onChange={(next) =>
                        updateColumn(index, { ...column, [field]: next })
                      }
                    />
                  </label>
                ),
              )}
              <label class="column-flag">
                <span>Not null</span>
                <input
                  type="checkbox"
                  disabled={disabled}
                  checked={column.nullable !== true}
                  onChange={(event) =>
                    updateColumn(index, {
                      ...column,
                      nullable: !event.currentTarget.checked,
                    })
                  }
                />
              </label>
              {node.properties.low_cardinality && (
                <label
                  class={`column-flag tooltip-host ${isStringArrowType(column.arrow_type) ? "" : "disabled"}`}
                  data-tooltip="Low cardinality is meaningful only for string values"
                >
                  <span>Low cardinality</span>
                  <input
                    type="checkbox"
                    disabled={disabled || !isStringArrowType(column.arrow_type)}
                    checked={column.low_cardinality === true}
                    onChange={(event) =>
                      updateColumn(index, {
                        ...column,
                        low_cardinality: event.currentTarget.checked,
                      })
                    }
                  />
                </label>
              )}
              <button
                class="icon-button danger column-remove"
                type="button"
                title="Remove column"
                disabled={disabled}
                onClick={() =>
                  onChange(
                    value.filter((_, itemIndex) => itemIndex !== index),
                    keys.filter((key) => key !== name),
                  )
                }
              >
                <TrashIcon />
              </button>
            </div>
            {extraFields.length > 0 && (
              <details class="column-details">
                <summary>Column settings</summary>
                <div class="schema-object">
                  {extraFields.map(([field, child]) => (
                    <PropertyEditor
                      name={field}
                      node={child}
                      required={node.required.has(field)}
                      value={column[field]}
                      disabled={disabled}
                      onChange={(next) =>
                        updateColumn(index, { ...column, [field]: next })
                      }
                    />
                  ))}
                </div>
              </details>
            )}
          </div>
        );
      })}
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
    <svg class="trash-icon" viewBox="0 0 16 16" aria-hidden="true">
      <path d="M3.5 5.25h9M6 3.5h4M5 5.25l.5 7.25h5l.5-7.25M6.75 7.25v3.5M9.25 7.25v3.5" />
    </svg>
  );
}

function EyeIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true">
      <path d="M1.75 8s2.15-3.25 6.25-3.25S14.25 8 14.25 8 12.1 11.25 8 11.25 1.75 8 1.75 8Z" />
      <circle cx="8" cy="8" r="1.75" />
    </svg>
  );
}

function EyeOffIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true">
      <path d="M2 2l12 12M5.15 5.1A6.7 6.7 0 0 1 8 4.5c4.1 0 6.25 3.5 6.25 3.5a9 9 0 0 1-2.05 2.25M9.45 11.35A7 7 0 0 1 8 11.5C3.9 11.5 1.75 8 1.75 8a9 9 0 0 1 1.5-1.85" />
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
