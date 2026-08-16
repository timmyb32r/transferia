import { createContext, type ComponentChildren } from "preact";
import { useContext, useEffect, useMemo, useRef, useState } from "preact/hooks";

import type { JsonObject, JsonValue } from "../types";
import { SelectControl } from "../ui/SelectControl";
export { SelectControl } from "../ui/SelectControl";
import { ColumnMappingsEditor } from "./ColumnMappingsEditor";
import {
  branchMatches,
  createValue,
  humanize,
  type CompiledNode,
} from "./compiler";
import {
  ByteSizeInput,
  PartitionRangesInput,
  PasswordInput,
  SystemColumnsEditor,
  TrashIcon,
  useStableRowIds,
} from "./controls";
import { DynamicSelectControl } from "./DynamicSelectControl";
import { reconcileSystemColumnKeys } from "./formLogic";
import { isObject, stringArray } from "./value";

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
    <>
      <div class="source-parser-bridge" aria-hidden="true" />
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
    </>
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
  const partitionRanges = partitionRangesProperty(node);
  const configuredPartitionRanges = hasConfiguredPartitionRanges(
    value,
    partitionRanges,
  );
  const [partitionRangesVisible, setPartitionRangesVisible] = useState(
    () => configuredPartitionRanges,
  );
  const previouslyConfiguredPartitionRanges = useRef(configuredPartitionRanges);
  useEffect(() => {
    if (partitionRanges === undefined) {
      setPartitionRangesVisible(false);
    } else if (configuredPartitionRanges) {
      setPartitionRangesVisible(true);
    } else if (previouslyConfiguredPartitionRanges.current) {
      setPartitionRangesVisible(false);
    }
    previouslyConfiguredPartitionRanges.current = configuredPartitionRanges;
  }, [
    configuredPartitionRanges,
    partitionRanges?.arrayName,
    partitionRanges?.fieldName,
  ]);
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
                NodeEditor={NodeEditor}
                PropertyEditor={PropertyEditor}
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
                showPartitionRanges={partitionRangesVisible}
                onChange={(next) => onChange({ ...object, [name]: next })}
              />
            ),
          )}
          {(advanced.length > 0 || partitionRanges !== undefined) && (
            <details class="foldout">
              <DisclosureSummary>Advanced settings</DisclosureSummary>
              <div class="foldout-content">
                {partitionRanges !== undefined && (
                  <div class="form-row partition-mode-control">
                    <span class="field-label">Specify partitions</span>
                    <label class="toggle">
                      <input
                        type="checkbox"
                        aria-label="Specify partitions"
                        checked={partitionRangesVisible}
                        disabled={isDisabled}
                        onChange={(event) => {
                          const visible = event.currentTarget.checked;
                          setPartitionRangesVisible(visible);
                          if (!visible) {
                            onChange(
                              clearConfiguredPartitionRanges(
                                object,
                                partitionRanges,
                              ),
                            );
                          }
                        }}
                      />
                    </label>
                  </div>
                )}
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
            value={typeof value === "number" ? value : null}
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
            const raw = event.currentTarget.value;
            if (raw === "") {
              onChange(null);
              return;
            }
            const parsed = Number(raw);
            if (Number.isFinite(parsed)) onChange(parsed);
          }}
        />
      );
    case "string":
      if (typeof node.xUi.dynamic_options === "string") {
        return (
          <DynamicSelectControl
            source={node.xUi.dynamic_options}
            value={typeof value === "string" ? value : ""}
            disabled={isDisabled}
            onChange={onChange}
          />
        );
      }
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

interface PartitionRangesProperty {
  arrayName: string;
  fieldName: string;
}

function partitionRangesProperty(
  node: CompiledNode,
): PartitionRangesProperty | undefined {
  if (node.kind !== "object") return undefined;
  for (const [arrayName, property] of Object.entries(node.properties)) {
    if (property.kind !== "array" || property.item.kind !== "object") continue;
    const field = Object.entries(property.item.properties).find(
      ([, child]) => child.xUi.widget === "partition_ranges",
    );
    if (field !== undefined) return { arrayName, fieldName: field[0] };
  }
  return undefined;
}

function hasConfiguredPartitionRanges(
  value: JsonValue,
  property: PartitionRangesProperty | undefined,
): boolean {
  if (property === undefined || !isObject(value)) return false;
  const items = value[property.arrayName];
  return (
    Array.isArray(items) &&
    items.some((item) => {
      if (!isObject(item)) return false;
      const ranges = item[property.fieldName];
      return Array.isArray(ranges) && ranges.length > 0;
    })
  );
}

function clearConfiguredPartitionRanges(
  object: JsonObject,
  property: PartitionRangesProperty,
): JsonObject {
  const items = object[property.arrayName];
  if (!Array.isArray(items)) return object;
  return {
    ...object,
    [property.arrayName]: items.map((item) =>
      isObject(item) ? { ...item, [property.fieldName]: [] } : item,
    ),
  };
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
                  onChange({
                    ...object,
                    common: { ...common, system_columns: next },
                    json_parser: {
                      ...parser,
                      keys: reconcileSystemColumnKeys(
                        common.system_columns,
                        next,
                        stringArray(parser.keys),
                      ),
                    },
                  }),
              },
            })}
        disabled={disabled}
        NodeEditor={NodeEditor}
        PropertyEditor={PropertyEditor}
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
  showPartitionRanges?: boolean;
  onChange: (value: JsonValue) => void;
}

function PropertyEditor({
  name,
  node,
  required,
  value,
  disabled,
  showPartitionRanges = true,
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
          showPartitionRanges={showPartitionRanges}
          onChange={onChange}
        />
      </div>
    );
  return (
    <div
      class={`form-row ${node.kind === "object" || (node.kind === "array" && node.xUi.widget !== "partition_ranges") || node.xUi.widget === "parser" ? "form-row-wide" : ""} ${node.kind === "nullable" ? "form-row-nullable" : ""} ${name === "installation" ? "form-row-installation" : ""} ${controlWidth}`}
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
  if (name === "installation") return "control-width-installation";
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

function CompactArrayEditor({
  name,
  node,
  value,
  disabled,
  showPartitionRanges,
  onChange,
}: {
  name: "topics" | "hosts";
  node: Extract<CompiledNode, { kind: "array" }>;
  value: JsonValue[];
  disabled: boolean;
  showPartitionRanges: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const rowIds = useStableRowIds(value.length);
  const fields =
    node.item.kind === "object"
      ? Object.entries(node.item.properties).filter(
          ([, child]) =>
            child.xUi.widget !== "hidden" &&
            (showPartitionRanges || child.xUi.widget !== "partition_ranges"),
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
                  <th key={field}>
                    {child.title ?? humanize(field)}
                    {child.xUi.widget === "partition_ranges" && (
                      <small class="optional">(optional)</small>
                    )}
                  </th>
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
                <tr class="config-table-row" key={rowIds.values[index]}>
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
                      onClick={() => {
                        rowIds.remove(index);
                        onChange(
                          value.filter((_, itemIndex) => itemIndex !== index),
                        );
                      }}
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
        onClick={() => {
          rowIds.insert(value.length);
          onChange([...value, createValue(node.item)]);
        }}
      >
        + Add {singular}
      </button>
    </div>
  );
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

function uiLabel(node: CompiledNode, value: string): string {
  const labels = node.xUi.labels;
  return isObject(labels) && typeof labels[value] === "string"
    ? labels[value]
    : humanize(value);
}
