import { createContext, type ComponentChildren } from "preact";
import { useContext } from "preact/hooks";

import type { JsonObject, JsonValue } from "../types";
import { FormField } from "../ui/FormField";
import { SelectControl } from "../ui/SelectControl";
import {
  createValue,
  branchMatches,
  humanize,
  isComplete,
  type CompiledNode,
} from "./compiler";
import { ArrayNodeEditor } from "./ArrayNodeEditor";
import { DynamicSelectControl } from "./DynamicSelectControl";
import { DynamicPathControl } from "./DynamicPathControl";
import { draftValue } from "./draft";
import { UnionNodeEditor } from "./UnionNodeEditor";
import { VariantDetailsCard } from "./VariantDetailsCard";
import type { NodeEditorProps, PropertyEditorProps } from "./editorTypes";
import { ObjectNodeEditor } from "./ObjectNodeEditor";
import { isObject, jsonPointer } from "./value";
import { useWidgetRegistry } from "./widgetRegistry";

export interface SchemaFormProps extends NodeEditorProps {
  variantUi?: VariantUi;
  showRequiredErrors?: boolean;
  optionOverrides?: Record<string, string[]>;
  connectionAction?: ComponentChildren;
}

export interface VariantUi {
  selectionOnly?: readonly string[];
  actions?: Readonly<Record<string, ComponentChildren>>;
  onSelected?: (widget: string) => void;
}

const VariantUiContext = createContext<VariantUi>({});
const RequiredErrorsContext = createContext(false);
const RootValueContext = createContext<JsonValue>({});
const OptionOverridesContext = createContext<Record<string, string[]>>({});

export function SchemaForm({
  node,
  value,
  disabled = false,
  variantUi = {},
  showRequiredErrors = false,
  optionOverrides = {},
  connectionAction,
  onChange,
}: SchemaFormProps) {
  return (
    <RootValueContext.Provider value={value}>
      <OptionOverridesContext.Provider value={optionOverrides}>
        <RequiredErrorsContext.Provider value={showRequiredErrors}>
          <VariantUiContext.Provider value={variantUi}>
            <NodeEditor
              node={node}
              value={value}
              disabled={disabled}
              connectionAction={connectionAction}
              onChange={onChange}
              path="#"
            />
          </VariantUiContext.Provider>
        </RequiredErrorsContext.Provider>
      </OptionOverridesContext.Provider>
    </RootValueContext.Provider>
  );
}

export function VariantDetailsForm({
  node,
  value,
  disabled = false,
  showRequiredErrors = false,
  onChange,
  widget,
  bridgeClass,
  cardClass,
}: SchemaFormProps & {
  widget: string;
  bridgeClass: string;
  cardClass: string;
}) {
  const widgets = useWidgetRegistry();
  return (
    <RootValueContext.Provider value={value}>
      <RequiredErrorsContext.Provider value={showRequiredErrors}>
        <VariantDetailsCard
          node={node}
          value={value}
          disabled={disabled}
          widget={widget}
          bridgeClass={bridgeClass}
          cardClass={cardClass}
          widgets={widgets}
          NodeEditor={NodeEditor}
          onChange={onChange}
        />
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
  const widgets = useWidgetRegistry();
  const isDisabled = disabled ?? false;
  const variantUi = useContext(VariantUiContext);
  const rootValue = useContext(RootValueContext);
  const optionOverrides = useContext(OptionOverridesContext);
  const customWidget = widgets.renderNode(
    { node, value, disabled: isDisabled, onChange, path, controlId },
    { NodeEditor, PropertyEditor },
  );
  if (customWidget !== undefined) return <>{customWidget}</>;
  switch (node.kind) {
    case "object": {
      const object = isObject(value) ? value : {};
      return (
        <ObjectNodeEditor
          node={node}
          value={object}
          disabled={isDisabled}
          path={path}
          connectionAction={connectionAction}
          widgets={widgets}
          PropertyEditor={PropertyEditor}
          onChange={onChange}
        />
      );
    }
    case "array": {
      return (
        <ArrayNodeEditor
          node={node}
          value={value}
          disabled={isDisabled}
          path={path}
          NodeEditor={NodeEditor}
          onChange={onChange}
        />
      );
    }
    case "union": {
      return (
        <UnionNodeEditor
          node={node}
          value={value}
          disabled={isDisabled}
          path={path}
          controlId={controlId}
          variantUi={variantUi}
          widgets={widgets}
          NodeEditor={NodeEditor}
          onChange={onChange}
        />
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
          name={`transferia-${controlId ?? path}`}
          autoComplete="off"
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
        return withExternalLink(
          node,
          current,
          <SelectControl
            searchable
            id={controlId}
            value={current}
            disabled={isDisabled}
            placeholder="Not selected"
            options={options.map((option) => ({
              value: option,
              label: option,
            }))}
            onChange={onChange}
          />,
        );
      }
      if (typeof node.xUi.dynamic_options === "string") {
        const DynamicControl =
          node.xUi.dynamic_options_control === "path"
            ? DynamicPathControl
            : DynamicSelectControl;
        const dependencyPointers = node.xUi.dynamic_options_dependencies;
        const pathControl = node.xUi.dynamic_options_control === "path";
        const dependencies =
          isObject(dependencyPointers) &&
          Object.values(dependencyPointers).every(
            (pointer) => typeof pointer === "string",
          )
            ? Object.fromEntries(
                Object.entries(dependencyPointers).flatMap(
                  ([name, pointer]) => {
                    const dependency = jsonPointer(
                      rootValue,
                      pointer as string,
                    );
                    if (typeof dependency === "string" && dependency !== "")
                      return [[name, dependency]];
                    return pathControl ? [[name, ""]] : [];
                  },
                ),
              )
            : {};
        if (
          !pathControl &&
          isObject(dependencyPointers) &&
          Object.keys(dependencies).length !==
            Object.keys(dependencyPointers).length
        ) {
          return withExternalLink(
            node,
            typeof value === "string" ? value : "",
            <input
              id={controlId}
              type="text"
              name={`transferia-${controlId ?? path}`}
              autoComplete="off"
              value={typeof value === "string" ? value : ""}
              disabled={isDisabled}
              onInput={(event) => onChange(event.currentTarget.value)}
            />,
          );
        }
        return withExternalLink(
          node,
          typeof value === "string" ? value : "",
          <DynamicControl
            id={controlId}
            source={node.xUi.dynamic_options}
            dependencies={dependencies}
            value={typeof value === "string" ? value : ""}
            disabled={isDisabled}
            onChange={onChange}
          />,
        );
      }
      if (node.enumValues !== undefined) {
        return withExternalLink(
          node,
          typeof value === "string" ? value : "",
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
          />,
        );
      }
      return withExternalLink(
        node,
        typeof value === "string" ? value : "",
        <input
          id={controlId}
          type="text"
          name={`transferia-${controlId ?? path}`}
          autoComplete="off"
          value={typeof value === "string" ? value : ""}
          disabled={isDisabled}
          onInput={(event) => onChange(event.currentTarget.value)}
        />,
      );
  }
}

function withExternalLink(
  node: CompiledNode,
  value: string,
  control: ComponentChildren,
): ComponentChildren {
  const template = node.xUi.external_link_template;
  if (typeof template !== "string") return control;
  const encodedValue = value
    .replace(/^\/+/, "")
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  const href = template.replace("{value}", encodedValue);
  return (
    <div class="linked-control">
      {control}
      {value !== "" && (
        <a
          class="external-link-button"
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          aria-label="Open in external console"
          title="Open in external console"
        >
          <svg viewBox="0 0 16 16" aria-hidden="true">
            <path d="M9.25 2a.75.75 0 0 1 .75-.75h3.25a1.5 1.5 0 0 1 1.5 1.5V6a.75.75 0 0 1-1.5 0V3.81L8.53 8.53a.75.75 0 0 1-1.06-1.06l4.72-4.72H10A.75.75 0 0 1 9.25 2ZM3.5 3.25h3a.75.75 0 0 1 0 1.5h-3a.75.75 0 0 0-.75.75v7c0 .414.336.75.75.75h7a.75.75 0 0 0 .75-.75v-3a.75.75 0 0 1 1.5 0v3a2.25 2.25 0 0 1-2.25 2.25h-7a2.25 2.25 0 0 1-2.25-2.25v-7A2.25 2.25 0 0 1 3.5 3.25Z" />
          </svg>
        </a>
      )}
    </div>
  );
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
  const widgets = useWidgetRegistry();
  const showRequiredErrors = useContext(RequiredErrorsContext);
  const variantUi = useContext(VariantUiContext);
  const selectionOnly =
    node.xUi.widget !== undefined &&
    variantUi.selectionOnly?.includes(node.xUi.widget) === true;
  const selectionComplete =
    node.kind === "union" &&
    node.branches.some((branch) => branchMatches(branch, value ?? null));
  const incompleteRequired =
    required && (selectionOnly ? !selectionComplete : !isComplete(node, value));
  const missingRequired = showRequiredErrors && incompleteRequired;
  const guidanceClass =
    !disabled && incompleteRequired ? "required-incomplete" : "";
  const effective = draftValue(node, value);
  const identifier = `field-${path.replace(/[^a-zA-Z0-9_-]/g, "-")}`;
  const presentation = widgets.presentation(node);
  const controlWidth = controlWidthClass(
    name,
    node,
    presentation?.controlWidth,
  );
  const customWidget = widgets.renderProperty(
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
      <div
        class={[guidanceClass, missingRequired ? "required-missing" : ""]
          .filter(Boolean)
          .join(" ")}
      >
        {customWidget}
      </div>
    );
  const classes = `${node.kind === "object" || (node.kind === "array" && node.xUi.widget !== "partition_ranges") || presentation?.wide || node.xUi.control_width === "full" ? "form-row-wide" : ""} ${node.kind === "nullable" ? "form-row-nullable" : ""} ${node.xUi.control_width === "installation" ? "form-row-installation" : ""} ${controlWidth}`;
  return (
    <FormField
      label={node.title ?? humanize(name)}
      optional={!required}
      description={presentation?.hideDescription ? undefined : node.description}
      controlId={isDirectlyLabelled(node) ? identifier : undefined}
      class={`${classes}${guidanceClass ? ` ${guidanceClass}` : ""}${missingRequired ? " required-missing" : ""}`}
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

function controlWidthClass(
  _name: string,
  node: CompiledNode,
  widgetWidth?: string,
): string {
  if (widgetWidth !== undefined) return `control-width-${widgetWidth}`;
  if (node.xUi.control_width === "installation")
    return "control-width-installation";
  if (node.xUi.control_width === "auth") return "control-width-auth";
  if (node.xUi.control_width === "medium") return "control-width-medium";
  if (node.xUi.control_width === "table_name")
    return "control-width-table-name";
  if (node.xUi.control_width === "full") return "control-width-full";
  if (
    node.kind === "union" ||
    (node.kind === "string" && node.enumValues !== undefined)
  )
    return "control-width-enum";
  return "";
}

function uiLabel(node: CompiledNode, value: string): string {
  const labels = node.xUi.labels;
  return isObject(labels) && typeof labels[value] === "string"
    ? labels[value]
    : humanize(value);
}
