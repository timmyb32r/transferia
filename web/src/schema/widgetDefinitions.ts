export type SchemaNodeKind =
  | "string"
  | "number"
  | "boolean"
  | "object"
  | "array"
  | "union"
  | "nullable";

type WidgetRendererPosition = "generic" | "node" | "property" | "both";

interface WidgetDefinition {
  kinds: readonly SchemaNodeKind[];
  renderer: WidgetRendererPosition;
}

export const WIDGET_DEFINITIONS = {
  byte_size: { kinds: ["number", "string"], renderer: "node" },
  column_keys: { kinds: ["array"], renderer: "generic" },
  column_mappings: { kinds: ["array"], renderer: "property" },
  compact_array: { kinds: ["array"], renderer: "property" },
  duration: { kinds: ["string"], renderer: "generic" },
  hidden: {
    kinds: [
      "string",
      "number",
      "boolean",
      "object",
      "array",
      "union",
      "nullable",
    ],
    renderer: "generic",
  },
  json_parser: { kinds: ["object"], renderer: "node" },
  middlewares: { kinds: ["array"], renderer: "node" },
  parser: { kinds: ["object", "union"], renderer: "generic" },
  parser_common: { kinds: ["object"], renderer: "property" },
  partition_ranges: { kinds: ["array"], renderer: "node" },
  password: { kinds: ["string"], renderer: "node" },
  select: { kinds: ["string"], renderer: "generic" },
  serializer: { kinds: ["object", "union"], renderer: "generic" },
  sql: { kinds: ["string"], renderer: "node" },
  system_columns: { kinds: ["object"], renderer: "both" },
} as const satisfies Record<string, WidgetDefinition>;

export type WidgetName = keyof typeof WIDGET_DEFINITIONS;

export function isWidgetName(value: unknown): value is WidgetName {
  return typeof value === "string" && value in WIDGET_DEFINITIONS;
}

export function widgetSupportsKind(
  widget: WidgetName,
  kind: SchemaNodeKind,
): boolean {
  return (
    WIDGET_DEFINITIONS[widget].kinds as readonly SchemaNodeKind[]
  ).includes(kind);
}

export function widgetUsesRenderer(
  widget: WidgetName,
  position: "node" | "property",
): boolean {
  const renderer = WIDGET_DEFINITIONS[widget].renderer;
  return renderer === position || renderer === "both";
}
