import { createContext, Fragment, type ComponentChildren } from "preact";
import { useContext, useEffect, useMemo, useRef, useState } from "preact/hooks";

import type { JsonObject, JsonValue } from "../types";
import { Button } from "../ui/Button";
import { Disclosure } from "../ui/Disclosure";
import { FormField } from "../ui/FormField";
import { SelectControl } from "../ui/SelectControl";
export { SelectControl } from "../ui/SelectControl";
import {
  branchMatches,
  createValue,
  humanize,
  isComplete,
  type CompiledNode,
} from "./compiler";
import { TrashIcon } from "../ui/icons";
import { DynamicSelectControl } from "./DynamicSelectControl";
import type { NodeEditorProps, PropertyEditorProps } from "./editorTypes";
import {
  clearConfiguredPartitionRanges,
  hasConfiguredPartitionRanges,
  partitionRangesProperty,
} from "./partitionRanges";
import { isObject, jsonPointer } from "./value";
import {
  isHiddenProperty,
  renderNodeWidget,
  renderPropertyWidget,
} from "./widgetRenderers";

interface SchemaFormProps extends NodeEditorProps {
  parserSelectionOnly?: boolean;
  showRequiredErrors?: boolean;
  optionOverrides?: Record<string, string[]>;
  connectionAction?: ComponentChildren;
}

const ParserSelectionContext = createContext(false);
const RequiredErrorsContext = createContext(false);
const RootValueContext = createContext<JsonValue>({});
const OptionOverridesContext = createContext<Record<string, string[]>>({});

export function SchemaForm({
  node,
  value,
  disabled = false,
  parserSelectionOnly = false,
  showRequiredErrors = false,
  optionOverrides = {},
  connectionAction,
  onChange,
}: SchemaFormProps) {
  return (
    <RootValueContext.Provider value={value}>
      <OptionOverridesContext.Provider value={optionOverrides}>
        <RequiredErrorsContext.Provider value={showRequiredErrors}>
          <ParserSelectionContext.Provider value={parserSelectionOnly}>
            <NodeEditor
              node={node}
              value={value}
              disabled={disabled}
              connectionAction={connectionAction}
              onChange={onChange}
              path="#"
            />
          </ParserSelectionContext.Provider>
        </RequiredErrorsContext.Provider>
      </OptionOverridesContext.Provider>
    </RootValueContext.Provider>
  );
}

export function ParserDetailsForm({
  node,
  value,
  disabled = false,
  showRequiredErrors = false,
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
    <RootValueContext.Provider value={value}>
    <RequiredErrorsContext.Provider value={showRequiredErrors}>
      <div class="source-parser-bridge" aria-hidden="true" />
      <section class="parser-details-card">
        <div class="section-heading">
          <h2>{selected.label} settings</h2>
        </div>
        <NodeEditor
          node={selected.node}
          value={parserValue ?? createValue(selected.node)}
          disabled={disabled}
          onChange={(next) => onChange({ ...object, [name]: next })}
        />
      </section>
    </RequiredErrorsContext.Provider>
    </RootValueContext.Provider>
  );
}

function NodeEditor({
  node,
  value,
  disabled,
  onChange,
  path = "#",
  controlId,
  connectionAction,
}: SchemaFormProps) {
  const isDisabled = disabled ?? false;
  const parserSelectionOnly = useContext(ParserSelectionContext);
  const rootValue = useContext(RootValueContext);
  const optionOverrides = useContext(OptionOverridesContext);
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
  const customWidget = renderNodeWidget(
    { node, value, disabled: isDisabled, onChange, path, controlId },
    { NodeEditor, PropertyEditor },
  );
  if (customWidget !== undefined) return <>{customWidget}</>;
  switch (node.kind) {
    case "object": {
      const object = isObject(value) ? value : {};
      const visible = Object.entries(node.properties).filter(
        ([, child]) => !isHiddenProperty(child),
      );
      const regular = visible.filter(
        ([, child]) =>
          child.xUi.section !== "advanced" &&
          child.xUi.section !== "system_columns" &&
          child.xUi.section !== "shard_group",
      );
      const advanced = visible.filter(
        ([, child]) => child.xUi.section === "advanced",
      );
      const systemColumns = visible.filter(
        ([, child]) => child.xUi.section === "system_columns",
      );
      const shardGroup = visible.filter(
        ([, child]) => child.xUi.section === "shard_group",
      );
      const connectionActionFollowsSecret = regular.some(
        ([, child]) => child.xUi.widget === "password",
      );
      const connectionActionPrecedesParser =
        !connectionActionFollowsSecret &&
        regular.some(([, child]) => child.xUi.widget === "parser");
      return (
        <div class="schema-object">
          {regular.map(([name, child]) => (
            <Fragment key={name}>
              {connectionActionPrecedesParser &&
                child.xUi.widget === "parser" &&
                connectionAction}
              <PropertyEditor
                name={name}
                node={child}
                required={node.required.has(name)}
                value={object[name]}
                disabled={isDisabled}
                showPartitionRanges={partitionRangesVisible}
                parentValue={object}
                onParentChange={onChange}
                path={`${path}/${name}`}
                onChange={(next) => onChange({ ...object, [name]: next })}
              />
              {child.xUi.widget === "password" && connectionAction}
            </Fragment>
          ))}
          {!connectionActionFollowsSecret &&
            !connectionActionPrecedesParser &&
            connectionAction}
          {shardGroup.length > 0 && (
            <Disclosure label="Shard group" class="shard-group-settings">
              {shardGroup.map(([name, child]) => (
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
            </Disclosure>
          )}
          {systemColumns.length > 0 && (
            <Disclosure label="Add system columns" class="system-columns">
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
            </Disclosure>
          )}
          {(advanced.length > 0 || partitionRanges !== undefined) && (
            <Disclosure label="Advanced settings">
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
            </Disclosure>
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
              <Button
                shape="icon"
                class="danger"
                title="Remove"
                disabled={isDisabled}
                onClick={() =>
                  onChange(items.filter((_, itemIndex) => itemIndex !== index))
                }
              >
                <TrashIcon />
              </Button>
            </div>
          ))}
          <Button
            shape="icon"
            class="add"
            title="Add"
            disabled={isDisabled}
            onClick={() => onChange([...items, createValue(node.item)])}
          >
            +
          </Button>
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
      if (optionOverrides[path] !== undefined) {
        const current = typeof value === "string" ? value : "";
        const choices = optionOverrides[path]!;
        const options = choices.includes(current)
          ? choices
          : current === ""
            ? choices
            : [current, ...choices];
        return (
          <SelectControl
            searchable
            id={controlId}
            value={current}
            disabled={isDisabled}
            placeholder="Not selected"
            options={options.map((option) => ({ value: option, label: option }))}
            onChange={onChange}
          />
        );
      }
      if (typeof node.xUi.dynamic_options === "string") {
        const dependencyPointers = node.xUi.dynamic_options_dependencies;
        const dependencies =
          isObject(dependencyPointers) &&
          Object.values(dependencyPointers).every(
            (pointer) => typeof pointer === "string",
          )
            ? Object.fromEntries(
                Object.entries(dependencyPointers).flatMap(([name, pointer]) => {
                  const dependency = jsonPointer(rootValue, pointer as string);
                  return typeof dependency === "string" && dependency !== ""
                    ? [[name, dependency]]
                    : [];
                }),
              )
            : {};
        if (
          isObject(dependencyPointers) &&
          Object.keys(dependencies).length !== Object.keys(dependencyPointers).length
        ) {
          return (
            <input
              id={controlId}
              type="text"
              value={typeof value === "string" ? value : ""}
              disabled={isDisabled}
              onInput={(event) => onChange(event.currentTarget.value)}
            />
          );
        }
        return (
          <DynamicSelectControl
            id={controlId}
            source={node.xUi.dynamic_options}
            dependencies={dependencies}
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
      return (
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
  parentValue,
  onParentChange,
  onChange,
  path = `#/${name}`,
}: PropertyEditorProps) {
  const showRequiredErrors = useContext(RequiredErrorsContext);
  const missingRequired =
    showRequiredErrors && required && !isComplete(node, value);
  const effective = value ?? createValue(node);
  const identifier = `field-${path.replace(/[^a-zA-Z0-9_-]/g, "-")}`;
  const controlWidth = controlWidthClass(name, node);
  const customWidget = renderPropertyWidget(
    {
      name,
      node,
      required,
      value,
      effectiveValue: effective,
      disabled,
      showPartitionRanges,
      parentValue,
      onParentChange,
      onChange,
      path,
      controlId: identifier,
    },
    { NodeEditor, PropertyEditor },
  );
  if (customWidget !== undefined)
    return (
      <div class={missingRequired ? "required-missing" : undefined}>
        {customWidget}
      </div>
    );
  const classes = `${node.kind === "object" || (node.kind === "array" && node.xUi.widget !== "partition_ranges") || node.xUi.widget === "parser" ? "form-row-wide" : ""} ${node.kind === "nullable" ? "form-row-nullable" : ""} ${node.xUi.control_width === "installation" ? "form-row-installation" : ""} ${controlWidth}`;
  return (
    <FormField
      label={node.title ?? humanize(name)}
      optional={!required}
      description={node.xUi.widget === "parser" ? undefined : node.description}
      controlId={isDirectlyLabelled(node) ? identifier : undefined}
      class={`${classes}${missingRequired ? " required-missing" : ""}`}
    >
      <NodeEditor
        node={node}
        value={effective}
        disabled={disabled}
        onChange={onChange}
        path={path}
        controlId={identifier}
      />
    </FormField>
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
    ([, child]) => !isHiddenProperty(child),
  );
}

function uiLabel(node: CompiledNode, value: string): string {
  const labels = node.xUi.labels;
  return isObject(labels) && typeof labels[value] === "string"
    ? labels[value]
    : humanize(value);
}
