import type { ComponentChildren } from "preact";

import type { JsonObject, JsonValue } from "../json";
import { Disclosure } from "../ui/Disclosure";
import { FormField } from "../ui/FormField";
import { ColumnMappingsEditor } from "./ColumnMappingsEditor";
import { CompactArrayEditor } from "./CompactArrayEditor";
import type { CompiledNode } from "./compiler";
import { humanize } from "./compiler";
import {
  ByteSizeInput,
  PartitionRangesInput,
  PasswordInput,
  SystemColumnsEditor,
} from "./controls";
import type {
  NodeEditorComponent,
  NodeEditorProps,
  PropertyEditorComponent,
  PropertyEditorProps,
} from "./editorTypes";
import { JsonParserEditor } from "./JsonParserEditor";
import { MiddlewareEditor } from "./MiddlewareEditor";
import { isObject, stringArray } from "./value";
import { isWidgetName, type WidgetName } from "./widgetDefinitions";

interface EditorServices {
  NodeEditor: NodeEditorComponent;
  PropertyEditor: PropertyEditorComponent;
}

interface NodeWidgetContext extends NodeEditorProps {
  disabled: boolean;
}

interface PropertyWidgetContext extends PropertyEditorProps {
  effectiveValue: JsonValue;
  controlId: string;
  parentValue?: JsonObject | undefined;
  onParentChange?: ((value: JsonObject) => void) | undefined;
}

type NodeWidgetRenderer = (
  context: NodeWidgetContext,
  services: EditorServices,
) => ComponentChildren;

type PropertyWidgetRenderer = (
  context: PropertyWidgetContext,
  services: EditorServices,
) => ComponentChildren;

const NODE_RENDERERS: Partial<Record<WidgetName, NodeWidgetRenderer>> = {
  json_parser: (context, services) => {
    if (context.node.kind !== "object") return null;
    return (
      <JsonParserEditor
        {...context}
        node={context.node}
        NodeEditor={services.NodeEditor}
        PropertyEditor={services.PropertyEditor}
      />
    );
  },
  middlewares: (context) => (
    <MiddlewareEditor
      value={context.value}
      disabled={context.disabled}
      onChange={context.onChange}
    />
  ),
  system_columns: (context) => {
    if (context.node.kind !== "object") return null;
    return (
      <SystemColumnsEditor
        node={context.node}
        value={context.value}
        disabled={context.disabled}
        onChange={context.onChange}
      />
    );
  },
  partition_ranges: (context) => {
    if (context.node.kind !== "array") return null;
    return (
      <PartitionRangesInput
        id={context.controlId}
        value={context.value}
        disabled={context.disabled}
        onChange={context.onChange}
      />
    );
  },
  byte_size: (context) => {
    if (context.node.kind === "string")
      return (
        <input
          id={context.controlId}
          type="text"
          autoComplete="off"
          value={typeof context.value === "string" ? context.value : ""}
          disabled={context.disabled}
          onInput={(event) => context.onChange(event.currentTarget.value)}
        />
      );
    if (context.node.kind !== "number") return null;
    return (
      <ByteSizeInput
        id={context.controlId}
        value={typeof context.value === "number" ? context.value : null}
        disabled={context.disabled}
        onChange={context.onChange}
      />
    );
  },
  password: (context) => {
    if (context.node.kind !== "string") return null;
    return (
      <PasswordInput
        id={context.controlId}
        value={typeof context.value === "string" ? context.value : ""}
        disabled={context.disabled}
        onChange={context.onChange}
      />
    );
  },
  sql: (context) => {
    if (context.node.kind !== "string") return null;
    return (
      <textarea
        id={context.controlId}
        autoComplete="off"
        class="sql-input"
        spellcheck={false}
        value={typeof context.value === "string" ? context.value : ""}
        disabled={context.disabled}
        onInput={(event) => context.onChange(event.currentTarget.value)}
      />
    );
  },
};

const PROPERTY_RENDERERS: Partial<Record<WidgetName, PropertyWidgetRenderer>> =
  {
    parser_common: (context, services) => (
      <section class="parser-common-section">
        <h3>{context.node.title ?? "Parser settings"}</h3>
        <services.NodeEditor
          node={context.node}
          value={context.effectiveValue}
          disabled={context.disabled}
          onChange={context.onChange}
        />
      </section>
    ),
    system_columns: (context, services) => (
      <Disclosure
        label="Add system columns"
        class="system-columns unified-system-columns"
      >
        <services.NodeEditor
          node={context.node}
          value={context.effectiveValue}
          disabled={context.disabled}
          onChange={context.onChange}
        />
      </Disclosure>
    ),
    compact_array: (context, services) => {
      if (context.node.kind !== "array") return null;
      return (
        <FormField
          label={context.node.title ?? humanize(context.name)}
          optional={!context.required}
          class="form-row-wide compact-array-field"
        >
          <CompactArrayEditor
            node={context.node}
            value={Array.isArray(context.value) ? context.value : []}
            disabled={context.disabled}
            showPartitionRanges={context.showPartitionRanges ?? true}
            onChange={context.onChange}
            NodeEditor={services.NodeEditor}
          />
        </FormField>
      );
    },
    column_mappings: (context, services) => {
      if (
        context.node.kind !== "array" ||
        context.parentValue === undefined ||
        context.onParentChange === undefined
      )
        return null;
      return (
        <ColumnMappingsEditor
          node={context.node.item}
          value={Array.isArray(context.value) ? context.value : []}
          keys={stringArray(context.parentValue.keys)}
          additionalKeyOptions={[]}
          disabled={context.disabled}
          NodeEditor={services.NodeEditor}
          PropertyEditor={services.PropertyEditor}
          onChange={(columns, keys) =>
            context.onParentChange?.({
              ...context.parentValue,
              [context.name]: columns,
              keys,
            })
          }
        />
      );
    },
  };

export function renderNodeWidget(
  context: NodeWidgetContext,
  services: EditorServices,
): ComponentChildren | undefined {
  const widget = context.node.xUi.widget;
  if (!isWidgetName(widget)) return undefined;
  return NODE_RENDERERS[widget]?.(context, services);
}

export function renderPropertyWidget(
  context: PropertyWidgetContext,
  services: EditorServices,
): ComponentChildren | undefined {
  const widget = context.node.xUi.widget;
  if (!isWidgetName(widget)) return undefined;
  return PROPERTY_RENDERERS[widget]?.(context, services);
}

export function isHiddenProperty(node: CompiledNode): boolean {
  return (
    node.xUi.widget === "hidden" ||
    node.xUi.widget === "column_keys" ||
    (node.kind === "string" && node.enumValues?.length === 1)
  );
}
