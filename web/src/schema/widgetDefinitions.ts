export type SchemaNodeKind =
  | "string"
  | "number"
  | "boolean"
  | "object"
  | "array"
  | "union"
  | "nullable";

export type WidgetRendererPosition = "generic" | "node" | "property" | "both";

export interface WidgetDefinition {
  kinds: readonly SchemaNodeKind[];
  renderer: WidgetRendererPosition;
}

export interface WidgetContracts {
  definition(name: unknown): WidgetDefinition | undefined;
}

export const NO_WIDGETS: WidgetContracts = {
  definition: () => undefined,
};

export function widgetSupportsKind(
  contracts: WidgetContracts,
  widget: string,
  kind: SchemaNodeKind,
): boolean {
  return contracts.definition(widget)?.kinds.includes(kind) ?? false;
}

export function widgetUsesRenderer(
  definition: WidgetDefinition,
  position: "node" | "property",
): boolean {
  return definition.renderer === position || definition.renderer === "both";
}
