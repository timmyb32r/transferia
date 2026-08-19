import { createContext, type ComponentChildren } from "preact";
import { useContext } from "preact/hooks";

import type { JsonObject, JsonValue } from "../json";
import type { CompiledNode } from "./compiler";
import type {
  NodeEditorComponent,
  NodeEditorProps,
  PropertyEditorComponent,
  PropertyEditorProps,
} from "./editorTypes";
import type { WidgetContracts } from "./widgetDefinitions";

export interface EditorServices {
  NodeEditor: NodeEditorComponent;
  PropertyEditor: PropertyEditorComponent;
}

export interface NodeWidgetContext extends NodeEditorProps {
  disabled: boolean;
}

export interface PropertyWidgetContext extends PropertyEditorProps {
  effectiveValue: JsonValue;
  controlId: string;
  parentValue?: JsonObject | undefined;
  onParentChange?: ((value: JsonObject) => void) | undefined;
}

export interface WidgetRegistry extends WidgetContracts {
  renderNode(
    context: NodeWidgetContext,
    services: EditorServices,
  ): ComponentChildren | undefined;
  renderProperty(
    context: PropertyWidgetContext,
    services: EditorServices,
  ): ComponentChildren | undefined;
  isHidden(node: CompiledNode): boolean;
}

const WidgetRegistryContext = createContext<WidgetRegistry | undefined>(
  undefined,
);

export function WidgetRegistryProvider({
  registry,
  children,
}: {
  registry: WidgetRegistry;
  children: ComponentChildren;
}) {
  return (
    <WidgetRegistryContext.Provider value={registry}>
      {children}
    </WidgetRegistryContext.Provider>
  );
}

export function useWidgetRegistry(): WidgetRegistry {
  const registry = useContext(WidgetRegistryContext);
  if (registry === undefined)
    throw new Error(
      "Widget registry is unavailable: register form features at the composition root",
    );
  return registry;
}

export function hasEditableContent(
  node: CompiledNode,
  registry: WidgetRegistry,
): boolean {
  if (node.kind !== "object") return true;
  return Object.values(node.properties).some(
    (child) => !registry.isHidden(child),
  );
}
