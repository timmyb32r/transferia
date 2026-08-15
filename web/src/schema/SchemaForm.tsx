import { useEffect, useMemo, useState } from "preact/hooks";

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
          !(hasColumnMappings && name === "primary_key") &&
          child.xUi.widget !== "hidden" &&
          !(
            name === "type" &&
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
                primaryKey={stringArray(object.primary_key)}
                disabled={isDisabled}
                onChange={(columns, primaryKey) =>
                  onChange({
                    ...object,
                    [name]: columns,
                    primary_key: primaryKey,
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
              <summary>System columns</summary>
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
                ×
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
          {selected >= 0 && (
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
        <div>
          <label class="toggle">
            <input
              type="checkbox"
              checked={enabled}
              disabled={isDisabled}
              onChange={(event) =>
                onChange(
                  event.currentTarget.checked ? createValue(node.inner) : null,
                )
              }
            />
            <span>Enabled</span>
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
          <span>{value === true ? "On" : "Off"}</span>
        </label>
      );
    case "number":
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
  return (
    <div
      class={`form-row ${node.kind === "object" || node.kind === "array" || node.xUi.widget === "parser" ? "form-row-wide" : ""}`}
    >
      <label class="field-label" for={identifier}>
        <span>
          {node.title ?? humanize(name)}
          {!required && <small class="optional">(optional)</small>}
        </span>
        {node.description && (
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
  const selected = options.find((option) => option.value === value);
  const filtered = useMemo(
    () =>
      options.filter((option) =>
        option.label.toLowerCase().includes(query.toLowerCase()),
      ),
    [options, query],
  );
  return (
    <div class={`select ${open ? "open" : ""}`}>
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

function ColumnMappingsEditor({
  node,
  value,
  primaryKey,
  disabled,
  onChange,
}: {
  node: CompiledNode;
  value: JsonValue[];
  primaryKey: string[];
  disabled: boolean;
  onChange: (columns: JsonValue[], primaryKey: string[]) => void;
}) {
  if (node.kind !== "object")
    return (
      <NodeEditor
        node={{ kind: "array", item: node, xUi: {} }}
        value={value}
        disabled={disabled}
        onChange={(next) =>
          onChange(Array.isArray(next) ? next : [], primaryKey)
        }
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
    const keys =
      newName === oldName
        ? primaryKey
        : primaryKey
            .map((key) => (key === oldName ? newName : key))
            .filter(Boolean);
    onChange(columns, keys);
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
          onClick={() => onChange([...value, createValue(node)], primaryKey)}
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
                <span>Key</span>
                <input
                  type="checkbox"
                  disabled={disabled || name === ""}
                  checked={primaryKey.includes(name)}
                  onChange={(event) =>
                    onChange(
                      value,
                      event.currentTarget.checked
                        ? [...primaryKey, name]
                        : primaryKey.filter((key) => key !== name),
                    )
                  }
                />
              </label>
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
                    primaryKey.filter((key) => key !== name),
                  )
                }
              >
                ×
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
        <svg viewBox="0 0 16 16" aria-hidden="true">
          <path d="M1.5 8s2.3-4 6.5-4 6.5 4 6.5 4-2.3 4-6.5 4S1.5 8 1.5 8Z" />
          <circle cx="8" cy="8" r="1.8" />
        </svg>
      </button>
    </div>
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
