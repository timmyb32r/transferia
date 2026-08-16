import { createContext, type ComponentChildren } from "preact";
import { useContext, useEffect, useMemo, useRef, useState } from "preact/hooks";

import type { JsonObject, JsonValue } from "../types";
import { SelectControl } from "../ui/SelectControl";
export { SelectControl } from "../ui/SelectControl";
import { CompactArrayEditor } from "./CompactArrayEditor";
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
} from "./controls";
import { DynamicSelectControl } from "./DynamicSelectControl";
import type { NodeEditorProps, PropertyEditorProps } from "./editorTypes";
import { isJsonParserContainer, JsonParserEditor } from "./JsonParserEditor";
import {
  clearConfiguredPartitionRanges,
  hasConfiguredPartitionRanges,
  partitionRangesProperty,
} from "./partitionRanges";
import { isObject, stringArray } from "./value";

interface SchemaFormProps extends NodeEditorProps {
  parserSelectionOnly?: boolean;
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
        path="#"
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

function NodeEditor({
  node,
  value,
  disabled,
  onChange,
  path = "#",
  controlId,
}: SchemaFormProps) {
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
        NodeEditor={NodeEditor}
        PropertyEditor={PropertyEditor}
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
        id={controlId}
        value={value}
        disabled={isDisabled}
        onChange={onChange}
      />
    );
  switch (node.kind) {
    case "object": {
      const object = isObject(value) ? value : {};
      const visible = Object.entries(node.properties).filter(
        ([, child]) =>
          child.xUi.widget !== "column_keys" &&
          child.xUi.widget !== "hidden" &&
          !(child.kind === "string" && child.enumValues?.length === 1),
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
                path={`${path}/${name}`}
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
                    path={`${path}/${name}`}
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
                    path={`${path}/${name}`}
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
                  path={`${path}/${index}`}
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
            id={controlId}
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
                  path={`${path}/branch-${selected}`}
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
                path={`${path}/nullable`}
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
            id={controlId}
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
            id={controlId}
            value={typeof value === "number" ? value : null}
            disabled={isDisabled}
            onChange={onChange}
          />
        );
      return (
        <input
          id={controlId}
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
            id={controlId}
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
            id={controlId}
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
          id={controlId}
          value={typeof value === "string" ? value : ""}
          disabled={isDisabled}
          onChange={onChange}
        />
      ) : (
        <input
          id={controlId}
          type="text"
          value={typeof value === "string" ? value : ""}
          disabled={isDisabled}
          onInput={(event) => onChange(event.currentTarget.value)}
        />
      );
  }
}

function PropertyEditor({
  name,
  node,
  required,
  value,
  disabled,
  showPartitionRanges = true,
  onChange,
  path = `#/${name}`,
}: PropertyEditorProps) {
  const effective = value ?? createValue(node);
  const identifier = `field-${path.replace(/[^a-zA-Z0-9_-]/g, "-")}`;
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
  if (node.kind === "array" && node.xUi.widget === "compact_array")
    return (
      <div class="form-row form-row-wide compact-array-field">
        <label class="field-label">
          <span>
            {node.title ?? humanize(name)}
            {!required && <small class="optional">(optional)</small>}
          </span>
        </label>
        <CompactArrayEditor
          node={node}
          value={Array.isArray(value) ? value : []}
          disabled={disabled}
          showPartitionRanges={showPartitionRanges}
          onChange={onChange}
          NodeEditor={NodeEditor}
        />
      </div>
    );
  return (
    <div
      class={`form-row ${node.kind === "object" || (node.kind === "array" && node.xUi.widget !== "partition_ranges") || node.xUi.widget === "parser" ? "form-row-wide" : ""} ${node.kind === "nullable" ? "form-row-nullable" : ""} ${node.xUi.control_width === "installation" ? "form-row-installation" : ""} ${controlWidth}`}
    >
      <label
        class="field-label"
        for={isDirectlyLabelled(node) ? identifier : undefined}
      >
        <span>
          {node.title ?? humanize(name)}
          {!required && <small class="optional">(optional)</small>}
        </span>
        {node.description && node.xUi.widget !== "parser" && (
          <span class="help" tabindex={0} data-tooltip={node.description}>
            ?
          </span>
        )}
      </label>
      <div class="field-control">
        <NodeEditor
          node={node}
          value={effective}
          disabled={disabled}
          onChange={onChange}
          path={path}
          controlId={identifier}
        />
      </div>
    </div>
  );
}

function isDirectlyLabelled(node: CompiledNode): boolean {
  return ["string", "number", "boolean", "union"].includes(node.kind);
}

function controlWidthClass(_name: string, node: CompiledNode): string {
  if (node.xUi.control_width === "installation")
    return "control-width-installation";
  if (node.xUi.widget === "parser") return "control-width-parser";
  if (node.xUi.control_width === "auth") return "control-width-auth";
  if (node.xUi.control_width === "medium") return "control-width-medium";
  if (node.xUi.control_width === "table_name")
    return "control-width-table-name";
  if (
    node.kind === "union" ||
    (node.kind === "string" && node.enumValues !== undefined)
  )
    return "control-width-enum";
  return "";
}

function nodeHasEditableContent(node: CompiledNode): boolean {
  if (node.kind !== "object") return true;
  return Object.entries(node.properties).some(
    ([, child]) =>
      child.xUi.widget !== "hidden" &&
      !(child.kind === "string" && child.enumValues?.length === 1),
  );
}

function uiLabel(node: CompiledNode, value: string): string {
  const labels = node.xUi.labels;
  return isObject(labels) && typeof labels[value] === "string"
    ? labels[value]
    : humanize(value);
}
