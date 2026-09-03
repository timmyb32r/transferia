import type { JsonSchema, JsonValue } from "../types";
import type { WidgetContracts } from "./widgetDefinitions";

export type UiSection =
  | "advanced"
  | "advanced_parquet"
  | "system_columns"
  | "shard_group";

export interface UiCapabilityHints {
  component:
    | "source"
    | "destination"
    | "parser"
    | "serializer"
    | "transformer";
  key: string;
  delivery_modes?: readonly ("batch" | "stream" | "batch_and_stream")[];
  record_semantics?: readonly ("append_only" | "changelog")[];
  properties?: readonly string[];
}

export interface UiHints {
  widget?: string;
  section?: UiSection;
  initial_items?: number;
  dynamic_options?: string;
  dynamic_options_dependencies?: Readonly<Record<string, string>>;
  dynamic_options_control?: "path";
  dynamic_options_path_syntax?: "plain" | "double_slash_absolute";
  dynamic_options_entity?: "table" | "topic" | "consumer";
  external_link_template?: string;
  external_link_dependencies?: Readonly<Record<string, string>>;
  labels?: Readonly<Record<string, string>>;
  options?: readonly JsonValue[];
  control_width?: string;
  item_label?: string;
  order?: number;
  reveal_rest_on_selection?: boolean;
  defer_variant_details?: boolean;
  indent_variant_details?: boolean;
  delivery_types?: readonly string[];
  capabilities?: UiCapabilityHints;
}

const SUPPORTED_HINTS = new Set<keyof UiHints>([
  "widget",
  "section",
  "initial_items",
  "dynamic_options",
  "dynamic_options_dependencies",
  "dynamic_options_control",
  "dynamic_options_path_syntax",
  "dynamic_options_entity",
  "external_link_template",
  "external_link_dependencies",
  "labels",
  "options",
  "control_width",
  "item_label",
  "order",
  "reveal_rest_on_selection",
  "defer_variant_details",
  "indent_variant_details",
  "delivery_types",
  "capabilities",
]);

export function decodeUiHints(
  value: JsonSchema["x-ui"],
  path: string,
  fail: (message: string) => never,
  widgets: WidgetContracts,
): UiHints {
  if (value === undefined) return {};
  const unknown = Object.keys(value).filter(
    (key) => !SUPPORTED_HINTS.has(key as keyof UiHints),
  );
  if (unknown.length > 0)
    fail(`${path}: unsupported x-ui hints: ${unknown.join(", ")}`);

  const widget = value.widget;
  if (
    widget !== undefined &&
    (typeof widget !== "string" || widgets.definition(widget) === undefined)
  )
    fail(`${path}: unsupported x-ui widget`);

  const section = value.section;
  if (
    section !== undefined &&
    section !== "advanced" &&
    section !== "advanced_parquet" &&
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
  const dynamicOptionsControl = value.dynamic_options_control;
  if (dynamicOptionsControl !== undefined && dynamicOptionsControl !== "path")
    fail(`${path}: x-ui dynamic_options_control must be path`);
  const dynamicOptionsPathSyntax = value.dynamic_options_path_syntax;
  if (
    dynamicOptionsPathSyntax !== undefined &&
    dynamicOptionsPathSyntax !== "plain" &&
    dynamicOptionsPathSyntax !== "double_slash_absolute"
  )
    fail(`${path}: x-ui dynamic_options_path_syntax is unsupported`);
  const dynamicOptionsEntity = value.dynamic_options_entity;
  if (
    dynamicOptionsEntity !== undefined &&
    dynamicOptionsEntity !== "table" &&
    dynamicOptionsEntity !== "topic" &&
    dynamicOptionsEntity !== "consumer"
  )
    fail(`${path}: x-ui dynamic_options_entity is unsupported`);
  if (
    dynamicOptionsControl === "path" &&
    (dynamicOptionsPathSyntax === undefined || dynamicOptionsEntity === undefined)
  )
    fail(`${path}: path controls must declare path syntax and entity`);
  if (
    dependencies !== undefined &&
    !Object.values(dependencies).every((pointer) => pointer.startsWith("/"))
  )
    fail(
      `${path}: x-ui dynamic_options_dependencies must map names to absolute JSON pointers`,
    );

  const linkDependencies = stringRecord(
    value.external_link_dependencies,
    `${path}: x-ui external_link_dependencies must map names to absolute JSON pointers`,
    fail,
  );
  if (
    linkDependencies !== undefined &&
    (!Object.entries(linkDependencies).every(
      ([name, pointer]) => /^[a-zA-Z0-9_]+$/.test(name) && pointer.startsWith("/"),
    ))
  )
    fail(
      `${path}: x-ui external_link_dependencies must map safe names to absolute JSON pointers`,
    );
  const linkTemplate = value.external_link_template;
  if (
    linkTemplate !== undefined &&
    (typeof linkTemplate !== "string" ||
      !linkTemplate.startsWith("https://") ||
      !validExternalLinkTemplate(linkTemplate, linkDependencies))
  )
    fail(
      `${path}: x-ui external_link_template must be an HTTPS URL containing exactly one declared placeholder each`,
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
  const order = value.order;
  if (
    order !== undefined &&
    (typeof order !== "number" || !Number.isSafeInteger(order))
  )
    fail(`${path}: x-ui order must be a safe integer`);
  const revealRestOnSelection = value.reveal_rest_on_selection;
  if (
    revealRestOnSelection !== undefined &&
    typeof revealRestOnSelection !== "boolean"
  )
    fail(`${path}: x-ui reveal_rest_on_selection must be a boolean`);
  const deferVariantDetails = value.defer_variant_details;
  if (
    deferVariantDetails !== undefined &&
    typeof deferVariantDetails !== "boolean"
  )
    fail(`${path}: x-ui defer_variant_details must be a boolean`);
  const deliveryTypes = value.delivery_types;
  const indentVariantDetails = value.indent_variant_details;
  if (
    indentVariantDetails !== undefined &&
    typeof indentVariantDetails !== "boolean"
  )
    fail(`${path}: x-ui indent_variant_details must be a boolean`);
  if (
    deliveryTypes !== undefined &&
    (!Array.isArray(deliveryTypes) ||
      deliveryTypes.length === 0 ||
      !deliveryTypes.every(
        (deliveryType) =>
          deliveryType === "batch" ||
          deliveryType === "stream" ||
          deliveryType === "batch_and_stream",
      ))
  )
    fail(`${path}: x-ui delivery_types must contain supported delivery types`);
  const capabilities = decodeCapabilities(value.capabilities, path, fail);

  return {
    ...(typeof widget === "string" ? { widget } : {}),
    ...(section === undefined ? {} : { section }),
    ...(initialItems === undefined ? {} : { initial_items: initialItems }),
    ...(dynamicOptions === undefined
      ? {}
      : { dynamic_options: dynamicOptions }),
    ...(dependencies === undefined
      ? {}
      : { dynamic_options_dependencies: dependencies }),
    ...(dynamicOptionsControl === undefined
      ? {}
      : { dynamic_options_control: dynamicOptionsControl }),
    ...(dynamicOptionsPathSyntax === undefined
      ? {}
      : { dynamic_options_path_syntax: dynamicOptionsPathSyntax }),
    ...(dynamicOptionsEntity === undefined
      ? {}
      : { dynamic_options_entity: dynamicOptionsEntity }),
    ...(linkTemplate === undefined
      ? {}
      : { external_link_template: linkTemplate }),
    ...(linkDependencies === undefined
      ? {}
      : { external_link_dependencies: linkDependencies }),
    ...(labels === undefined ? {} : { labels }),
    ...(options === undefined ? {} : { options }),
    ...(controlWidth === undefined ? {} : { control_width: controlWidth }),
    ...(itemLabel === undefined ? {} : { item_label: itemLabel }),
    ...(order === undefined ? {} : { order }),
    ...(revealRestOnSelection === undefined
      ? {}
      : { reveal_rest_on_selection: revealRestOnSelection }),
    ...(deferVariantDetails === undefined
      ? {}
      : { defer_variant_details: deferVariantDetails }),
    ...(indentVariantDetails === undefined
      ? {}
      : { indent_variant_details: indentVariantDetails }),
    ...(deliveryTypes === undefined
      ? {}
      : { delivery_types: deliveryTypes as readonly string[] }),
    ...(capabilities === undefined ? {} : { capabilities }),
  };
}

function decodeCapabilities(
  value: JsonValue | undefined,
  path: string,
  fail: (message: string) => never,
): UiCapabilityHints | undefined {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== "object" || Array.isArray(value))
    fail(`${path}: x-ui capabilities must be an object`);
  const object = value as Record<string, JsonValue>;
  const unknown = Object.keys(object).filter(
    (key) =>
      ![
        "component",
        "key",
        "delivery_modes",
        "record_semantics",
        "properties",
      ].includes(key),
  );
  if (unknown.length > 0)
    fail(`${path}: unsupported x-ui capabilities: ${unknown.join(", ")}`);
  const component = object.component;
  const key = object.key;
  const deliveryModes = object.delivery_modes;
  const recordSemantics = object.record_semantics;
  const properties = object.properties;
  if (
    component !== "source" &&
    component !== "destination" &&
    component !== "parser" &&
    component !== "serializer" &&
    component !== "transformer"
  )
    fail(`${path}: x-ui capabilities component is unsupported`);
  if (typeof key !== "string" || key.length === 0)
    fail(`${path}: x-ui capabilities key must be a non-empty string`);
  if (
    deliveryModes !== undefined &&
    (!Array.isArray(deliveryModes) ||
      !deliveryModes.every(
        (mode) =>
          mode === "batch" || mode === "stream" || mode === "batch_and_stream",
      ))
  )
    fail(`${path}: x-ui capabilities delivery_modes is unsupported`);
  if (
    recordSemantics !== undefined &&
    (!Array.isArray(recordSemantics) ||
      !recordSemantics.every(
        (semantics) => semantics === "append_only" || semantics === "changelog",
      ))
  )
    fail(`${path}: x-ui capabilities record_semantics is unsupported`);
  if (
    properties !== undefined &&
    (!Array.isArray(properties) ||
      !properties.every((property) => typeof property === "string" && property.length > 0))
  )
    fail(`${path}: x-ui capabilities properties must contain non-empty strings`);
  if (component === "source") {
    if (
      deliveryModes === undefined ||
      deliveryModes.length === 0 ||
      new Set(deliveryModes).size !== deliveryModes.length
    )
      fail(`${path}: source capabilities delivery_modes must be non-empty and unique`);
  } else if (deliveryModes !== undefined) {
    fail(`${path}: only source capabilities can declare delivery_modes`);
  }
  if (component === "source" || component === "destination") {
    if (
      recordSemantics === undefined ||
      recordSemantics.length === 0 ||
      new Set(recordSemantics).size !== recordSemantics.length
    )
      fail(`${path}: endpoint record_semantics must be non-empty and unique`);
    if (properties !== undefined)
      fail(`${path}: endpoint capabilities cannot declare component properties`);
  }
  return {
    component,
    key,
    ...(deliveryModes === undefined
      ? {}
      : {
          delivery_modes: deliveryModes as (
            | "batch"
            | "stream"
            | "batch_and_stream"
          )[],
        }),
    ...(recordSemantics === undefined
      ? {}
      : { record_semantics: recordSemantics as ("append_only" | "changelog")[] }),
    ...(properties === undefined ? {} : { properties: properties as string[] }),
  };
}

function validExternalLinkTemplate(
  template: string,
  dependencies: Readonly<Record<string, string>> | undefined,
): boolean {
  const expected = ["value", ...Object.keys(dependencies ?? {})];
  const placeholders = [...template.matchAll(/\{([^{}]+)\}/g)].map(
    (match) => match[1],
  );
  return (
    placeholders.length === expected.length &&
    expected.every(
      (name) => placeholders.filter((placeholder) => placeholder === name).length === 1,
    ) &&
    template.replaceAll(/\{[^{}]+\}/g, "").match(/[{}]/) === null
  );
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
