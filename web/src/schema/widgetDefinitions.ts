export type SchemaNodeKind =
  | "string"
  | "number"
  | "boolean"
  | "object"
  | "array"
  | "union"
  | "nullable";

export const WIDGET_DEFINITIONS = {
  byte_size: ["number", "string"],
  column_keys: ["array"],
  column_mappings: ["array"],
  compact_array: ["array"],
  duration: ["string"],
  hidden: [
    "string",
    "number",
    "boolean",
    "object",
    "array",
    "union",
    "nullable",
  ],
  json_parser: ["object"],
  parser: ["object", "union"],
  parser_common: ["object"],
  partition_ranges: ["array"],
  password: ["string"],
  select: ["string"],
  system_columns: ["object"],
} as const satisfies Record<string, readonly SchemaNodeKind[]>;

export type WidgetName = keyof typeof WIDGET_DEFINITIONS;

export function isWidgetName(value: unknown): value is WidgetName {
  return typeof value === "string" && value in WIDGET_DEFINITIONS;
}

export function widgetSupportsKind(
  widget: WidgetName,
  kind: SchemaNodeKind,
): boolean {
  return (WIDGET_DEFINITIONS[widget] as readonly SchemaNodeKind[]).includes(
    kind,
  );
}
