import type { ComponentChildren } from "preact";

import type { CompiledNode } from "./compiler";
import type {
  EditorServices,
  NodeWidgetContext,
  PropertyWidgetContext,
  WidgetRegistry,
} from "./widgetRegistry";
import type { WidgetDefinition } from "./widgetDefinitions";

export interface WidgetPlugin extends WidgetDefinition {
  name: string;
  node?: (
    context: NodeWidgetContext,
    services: EditorServices,
  ) => ComponentChildren;
  property?: (
    context: PropertyWidgetContext,
    services: EditorServices,
  ) => ComponentChildren;
  hidden?: boolean;
}

export function createWidgetRegistry(
  plugins: readonly WidgetPlugin[],
): WidgetRegistry {
  const byName = new Map<string, WidgetPlugin>();
  for (const plugin of plugins) {
    if (byName.has(plugin.name))
      throw new Error(`Widget ${plugin.name} is registered more than once`);
    if (uses(plugin, "node") && plugin.node === undefined)
      throw new Error(`Widget ${plugin.name} declares a missing node renderer`);
    if (uses(plugin, "property") && plugin.property === undefined)
      throw new Error(
        `Widget ${plugin.name} declares a missing property renderer`,
      );
    byName.set(plugin.name, plugin);
  }
  const selected = (node: CompiledNode) =>
    node.xUi.widget === undefined ? undefined : byName.get(node.xUi.widget);
  return {
    definition: (name) =>
      typeof name === "string" ? byName.get(name) : undefined,
    renderNode: (context, services) =>
      selected(context.node)?.node?.(context, services),
    renderProperty: (context, services) =>
      selected(context.node)?.property?.(context, services),
    isHidden: (node) =>
      selected(node)?.hidden === true ||
      (node.kind === "string" && node.enumValues?.length === 1),
    presentation: (node) => selected(node),
  };
}

function uses(plugin: WidgetPlugin, position: "node" | "property"): boolean {
  return plugin.renderer === position || plugin.renderer === "both";
}
