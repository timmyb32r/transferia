import { createContext, type ComponentChildren } from "preact";
import { useContext, useEffect, useState } from "preact/hooks";

import type { JsonObject, JsonValue } from "../types";
import { AutofillResistantInput } from "../ui/AutofillResistantField";
import { FormField } from "../ui/FormField";
import { ExternalLinkIcon } from "../ui/icons";
import { SelectControl } from "../ui/SelectControl";
import {
  createValue,
  branchMatches,
  firstCompletionIssue,
  humanize,
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
          NodeEditor={NodeEditor}
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
      return (
        <NullableNodeEditor
          node={node}
          value={value}
          disabled={isDisabled}
          path={path}
          onChange={onChange}
        />
      );
    }
    case "boolean":
      return (
        <label class="toggle">
          <AutofillResistantInput
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
        <AutofillResistantInput
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
        return withExternalLink(
          node,
          current,
          rootValue,
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
            rootValue,
            <AutofillResistantInput
              id={controlId}
              type="text"
              value={typeof value === "string" ? value : ""}
              disabled={isDisabled}
              onInput={(event) => onChange(event.currentTarget.value)}
            />,
          );
        }
        return withExternalLink(
          node,
          typeof value === "string" ? value : "",
          rootValue,
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
          rootValue,
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
        rootValue,
        <AutofillResistantInput
          id={controlId}
          type="text"
          value={typeof value === "string" ? value : ""}
          disabled={isDisabled}
          onInput={(event) => onChange(event.currentTarget.value)}
        />,
      );
  }
}

function NullableNodeEditor({
  node,
  value,
  disabled,
  path,
  onChange,
}: {
  node: Extract<CompiledNode, { kind: "nullable" }>;
  value: JsonValue;
  disabled: boolean;
  path: string;
  onChange: (value: JsonValue) => void;
}) {
  const configured = value !== null && value !== undefined;
  const [pendingEmptyPath, setPendingEmptyPath] = useState<string>();
  const enabled = configured || pendingEmptyPath === path;

  useEffect(() => {
    if (configured) setPendingEmptyPath(undefined);
  }, [configured]);
  useEffect(() => setPendingEmptyPath(undefined), [path]);

  return (
    <div class="nullable-control">
      <label class="toggle">
        <AutofillResistantInput
          type="checkbox"
          aria-label="Enable optional settings"
          checked={enabled}
          disabled={disabled}
          onChange={(event) => {
            if (!event.currentTarget.checked) {
              setPendingEmptyPath(undefined);
              onChange(null);
              return;
            }
            const initial = createValue(node.inner);
            if (initial === null) setPendingEmptyPath(path);
            else onChange(initial);
          }}
        />
      </label>
      {enabled && (
        <div class="nested-section">
          <NodeEditor
            node={node.inner}
            value={configured ? value : createValue(node.inner)}
            disabled={disabled}
            path={`${path}/nullable`}
            onChange={onChange}
          />
        </div>
      )}
    </div>
  );
}

function withExternalLink(
  node: CompiledNode,
  value: string,
  rootValue: JsonValue,
  control: ComponentChildren,
): ComponentChildren {
  const template = node.xUi.external_link_template;
  if (typeof template !== "string") return control;
  const encodedValue = value
    .replace(/^\/+/, "")
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  let href = template.replace("{value}", encodedValue);
  for (const [name, pointer] of Object.entries(
    node.xUi.external_link_dependencies ?? {},
  )) {
    const dependency = jsonPointer(rootValue, pointer);
    if (typeof dependency !== "string" || dependency === "") return control;
    href = href.replace(`{${name}}`, encodeURIComponent(dependency));
  }
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
          <ExternalLinkIcon />
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
    node.xUi.defer_variant_details === true ||
    (node.xUi.widget !== undefined &&
      variantUi.selectionOnly?.includes(node.xUi.widget) === true);
  const selectionComplete =
    node.kind === "union" &&
    node.branches.some((branch) => branchMatches(branch, value ?? null));
  const issue = selectionOnly
    ? undefined
    : firstCompletionIssue(node, value, required, path);
  const incompleteField = selectionOnly
    ? (required || value !== undefined) && !selectionComplete
    : issue !== undefined && !issue.hidden;
  const visibleFieldError = showRequiredErrors && incompleteField;
  const guidanceClass =
    !disabled && incompleteField ? "required-incomplete" : "";
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
        class={[guidanceClass, visibleFieldError ? "required-missing" : ""]
          .filter(Boolean)
          .join(" ")}
      >
        {customWidget}
      </div>
    );
  const wideRow =
    node.kind === "object" ||
    (node.kind === "array" && node.xUi.widget !== "partition_ranges") ||
    presentation?.wide ||
    node.xUi.control_width === "full";
  const classes = `${wideRow ? "form-row-wide" : ""} ${node.kind === "nullable" ? "form-row-nullable" : ""} ${node.xUi.control_width === "installation" ? "form-row-installation" : ""} ${controlWidth}`;
  return (
    <FormField
      label={node.title ?? humanize(name)}
      optional={!required}
      description={presentation?.hideDescription ? undefined : node.description}
      controlId={isDirectlyLabelled(node) ? identifier : undefined}
      class={`${classes}${guidanceClass ? ` ${guidanceClass}` : ""}${visibleFieldError ? " required-missing" : ""}`}
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
  if (node.xUi.control_width === "routing") return "control-width-routing";
  if (node.xUi.control_width === "wide") return "control-width-wide";
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
