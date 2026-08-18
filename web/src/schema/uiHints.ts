import type { JsonSchema, JsonValue } from "../types";
import { isWidgetName, type WidgetName } from "./widgetDefinitions";

export type UiSection = "advanced" | "system_columns" | "shard_group";

export interface UiHints {
  widget?: WidgetName;
  section?: UiSection;
  initial_items?: number;
  dynamic_options?: string;
  dynamic_options_dependencies?: Readonly<Record<string, string>>;
  external_link_template?: string;
  labels?: Readonly<Record<string, string>>;
  options?: readonly JsonValue[];
  control_width?: string;
  item_label?: string;
}

const SUPPORTED_HINTS = new Set<keyof UiHints>([
  "widget",
  "section",
  "initial_items",
  "dynamic_options",
  "dynamic_options_dependencies",
  "external_link_template",
  "labels",
  "options",
  "control_width",
  "item_label",
]);

export function decodeUiHints(
  value: JsonSchema["x-ui"],
  path: string,
  fail: (message: string) => never,
): UiHints {
  if (value === undefined) return {};
  const unknown = Object.keys(value).filter(
    (key) => !SUPPORTED_HINTS.has(key as keyof UiHints),
  );
  if (unknown.length > 0)
    fail(`${path}: unsupported x-ui hints: ${unknown.join(", ")}`);

  const widget = value.widget;
  if (widget !== undefined && !isWidgetName(widget))
    fail(`${path}: unsupported x-ui widget`);

  const section = value.section;
  if (
    section !== undefined &&
    section !== "advanced" &&
    section !== "system_columns" &&
    section !== "shard_group"
  )
    fail(`${path}: unsupported x-ui section`);

  const initialItems = value.initial_items;
  if (
    initialItems !== undefined &&
    (typeof initialItems !== "number" ||
      !Number.isSafeInteger(initialItems) ||
      initialItems < 0)
  )
    fail(`${path}: x-ui initial_items must be a non-negative integer`);

  const dynamicOptions = value.dynamic_options;
  if (dynamicOptions !== undefined && typeof dynamicOptions !== "string")
    fail(`${path}: x-ui dynamic_options must be a string`);

  const dependencies = stringRecord(
    value.dynamic_options_dependencies,
    `${path}: x-ui dynamic_options_dependencies must map names to absolute JSON pointers`,
    fail,
  );
  if (
    dependencies !== undefined &&
    !Object.values(dependencies).every((pointer) => pointer.startsWith("/"))
  )
    fail(
      `${path}: x-ui dynamic_options_dependencies must map names to absolute JSON pointers`,
    );

  const linkTemplate = value.external_link_template;
  if (
    linkTemplate !== undefined &&
    (typeof linkTemplate !== "string" ||
      !linkTemplate.startsWith("https://") ||
      linkTemplate.split("{value}").length !== 2)
  )
    fail(
      `${path}: x-ui external_link_template must be an HTTPS URL containing exactly one {value} placeholder`,
    );

  const labels = stringRecord(
    value.labels,
    `${path}: x-ui labels must map strings to strings`,
    fail,
  );
  const options = value.options;
  if (options !== undefined && !Array.isArray(options))
    fail(`${path}: x-ui options must be an array`);

  const controlWidth = optionalString(
    value.control_width,
    `${path}: x-ui control_width must be a string`,
    fail,
  );
  const itemLabel = optionalString(
    value.item_label,
    `${path}: x-ui item_label must be a string`,
    fail,
  );

  return {
    ...(widget === undefined ? {} : { widget }),
    ...(section === undefined ? {} : { section }),
    ...(initialItems === undefined ? {} : { initial_items: initialItems }),
    ...(dynamicOptions === undefined
      ? {}
      : { dynamic_options: dynamicOptions }),
    ...(dependencies === undefined
      ? {}
      : { dynamic_options_dependencies: dependencies }),
    ...(linkTemplate === undefined
      ? {}
      : { external_link_template: linkTemplate }),
    ...(labels === undefined ? {} : { labels }),
    ...(options === undefined ? {} : { options }),
    ...(controlWidth === undefined ? {} : { control_width: controlWidth }),
    ...(itemLabel === undefined ? {} : { item_label: itemLabel }),
  };
}

function optionalString(
  value: JsonValue | undefined,
  message: string,
  fail: (message: string) => never,
): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string") fail(message);
  return value;
}

function stringRecord(
  value: JsonValue | undefined,
  message: string,
  fail: (message: string) => never,
): Readonly<Record<string, string>> | undefined {
  if (value === undefined) return undefined;
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    !Object.values(value).every((entry) => typeof entry === "string")
  )
    fail(message);
  return value as Record<string, string>;
}
