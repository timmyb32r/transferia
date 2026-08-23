import {
  compileSchema,
  draftSeedError,
  firstCompletionIssue,
  isFieldComplete,
  SchemaContractError,
  type CompletionIssue,
  type CompiledNode,
} from "../schema/compiler";
import type { WidgetContracts } from "../schema/widgetDefinitions";
import type {
  ConnectorDefinition,
  EndpointDefinition,
  JsonObject,
  JsonValue,
  UiCatalog,
} from "../types";

const compiledSchemaCache = new WeakMap<
  WidgetContracts,
  WeakMap<object, CompiledNode>
>();

export function compiledSchema(
  schema: UiCatalog["common_schema"],
  widgets: WidgetContracts,
): CompiledNode {
  let schemas = compiledSchemaCache.get(widgets);
  if (schemas === undefined) {
    schemas = new WeakMap<object, CompiledNode>();
    compiledSchemaCache.set(widgets, schemas);
  }
  const cached = schemas.get(schema);
  if (cached !== undefined) return cached;
  const compiled = compileSchema(schema, widgets);
  schemas.set(schema, compiled);
  return compiled;
}

export function validateCatalogSchemas(
  catalog: UiCatalog,
  widgets: WidgetContracts,
): void {
  const common = compiledSchema(catalog.common_schema, widgets);
  if (common.kind !== "object")
    throw new SchemaContractError("common schema must be an object");
  validateInitial(
    "common",
    common,
    Object.fromEntries(
      Object.entries(catalog.initial).filter(
        ([name]) => common.properties[name] !== undefined,
      ),
    ),
  );
  for (const connector of catalog.connectors) {
    for (const [role, endpoint] of [
      ["source", connector.source],
      ["sink", connector.sink],
    ] as const) {
      if (endpoint === undefined) continue;
      const node = compiledSchema(endpoint.schema, widgets);
      const owner = `${connector.key} ${role}`;
      validateInitial(owner, node, endpoint.initial);
      validateHiddenRequiredDefaults(owner, node);
    }
  }
}

function validateHiddenRequiredDefaults(
  owner: string,
  node: CompiledNode,
  path = "#",
): void {
  if (node.kind === "object") {
    for (const [name, child] of Object.entries(node.properties)) {
      const childPath = `${path}/${escapePointer(name)}`;
      if (
        node.required.has(name) &&
        child.hidden === true &&
        ["string", "number", "boolean"].includes(child.kind) &&
        (child.defaultValue === undefined ||
          !isFieldComplete(child, child.defaultValue, true))
      )
        throw new SchemaContractError(
          `${owner} hidden required field ${childPath} must declare a valid default`,
        );
      validateHiddenRequiredDefaults(owner, child, childPath);
    }
    return;
  }
  if (node.kind === "array") {
    validateHiddenRequiredDefaults(owner, node.item, `${path}/items`);
    return;
  }
  if (node.kind === "nullable") {
    validateHiddenRequiredDefaults(owner, node.inner, `${path}/nullable`);
    return;
  }
  if (node.kind === "union")
    node.branches.forEach((branch, index) =>
      validateHiddenRequiredDefaults(
        owner,
        branch.node,
        `${path}/branch-${index}`,
      ),
    );
}

function escapePointer(value: string): string {
  return value.replaceAll("~", "~0").replaceAll("/", "~1");
}

function validateInitial(
  owner: string,
  node: CompiledNode,
  initial: JsonValue,
): void {
  const error = draftSeedError(node, initial);
  if (error !== undefined)
    throw new SchemaContractError(`${owner} initial value is invalid: ${error}`);
}

export function freshConfig(catalog: UiCatalog): JsonObject {
  const id = crypto.randomUUID();
  return {
    ...structuredClone(catalog.initial),
    delivery_id: `delivery-${id}`,
    durable_storage: {
      type: "local_file",
      path: `.transferia-server/workers/${id}/state`,
    },
    delivery_type: null,
    source: {},
    sink: {},
  };
}

const SPECIAL_CONNECTOR_ORDER = new Map([
  ["data_generator", 0],
  ["discard", 1],
]);

export function orderedEndpointConnectors(
  catalog: UiCatalog,
  role: "source" | "sink",
): ConnectorDefinition[] {
  return catalog.connectors
    .filter((connector) => connector[role] !== undefined)
    .sort((left, right) => {
      const leftSpecial = SPECIAL_CONNECTOR_ORDER.get(left.key);
      const rightSpecial = SPECIAL_CONNECTOR_ORDER.get(right.key);
      if (leftSpecial !== undefined || rightSpecial !== undefined) {
        if (leftSpecial === undefined) return -1;
        if (rightSpecial === undefined) return 1;
        return leftSpecial - rightSpecial;
      }
      return (
        left.title.localeCompare(right.title, "en", { sensitivity: "base" }) ||
        left.key.localeCompare(right.key)
      );
    });
}

export function selectedEndpoints(
  catalog: UiCatalog,
  config: JsonObject,
): {
  sourceKey: string;
  sinkKey: string;
  source: EndpointDefinition | undefined;
  sink: EndpointDefinition | undefined;
  error?: string;
} {
  const sourceKey = singleKey(config.source);
  const sinkKey = singleKey(config.sink);
  const source = catalog.connectors.find(
    (connector) => connector.key === sourceKey,
  )?.source;
  const sink = catalog.connectors.find(
    (connector) => connector.key === sinkKey,
  )?.sink;
  const deliveryType = stringValue(config.delivery_type);
  let error: string | undefined;
  if (deliveryType !== "" && source !== undefined) {
    const required =
      deliveryType === "batch_and_stream"
        ? ["batch", "stream"]
        : [deliveryType];
    const missing = required.filter(
      (mode) => !source.delivery_modes.includes(mode as "batch" | "stream"),
    );
    if (missing.length > 0) {
      const title =
        catalog.connectors.find((connector) => connector.key === sourceKey)
          ?.title ?? sourceKey;
      error = `${title} does not support ${deliveryType.replaceAll("_", " ")} delivery.`;
    }
  }
  return {
    sourceKey,
    sinkKey,
    source,
    sink,
    ...(error === undefined ? {} : { error }),
  };
}

export function configurationReadiness(
  catalog: UiCatalog,
  config: JsonObject,
  widgets: WidgetContracts,
): {
  selection: ReturnType<typeof selectedEndpoints>;
  commonIssue: CompletionIssue | undefined;
  sourceIssue: CompletionIssue | undefined;
  sinkIssue: CompletionIssue | undefined;
  commonComplete: boolean;
  sourceComplete: boolean;
  sinkComplete: boolean;
  sourceReady: boolean;
  complete: boolean;
} {
  const selection = selectedEndpoints(catalog, config);
  const commonIssue = firstCompletionIssue(
    compiledSchema(catalog.common_schema, widgets),
    config,
  );
  const sourceIssue =
    selection.source === undefined
      ? undefined
      : prefixIssue(
          firstCompletionIssue(
            compiledSchema(selection.source.schema, widgets),
            endpointValue(config, "source", selection.sourceKey),
          ),
          `#/source/${escapePointer(selection.sourceKey)}`,
        );
  const sinkIssue =
    selection.sink === undefined
      ? undefined
      : prefixIssue(
          firstCompletionIssue(
            compiledSchema(selection.sink.schema, widgets),
            endpointValue(config, "sink", selection.sinkKey),
          ),
          `#/sink/${escapePointer(selection.sinkKey)}`,
        );
  const commonComplete =
    selection.error === undefined && commonIssue === undefined;
  const sourceComplete =
    selection.source !== undefined && sourceIssue === undefined;
  const sinkComplete =
    selection.sink !== undefined && sinkIssue === undefined;
  const sourceReady = commonComplete && sourceComplete;
  return {
    selection,
    commonIssue,
    sourceIssue,
    sinkIssue,
    commonComplete,
    sourceComplete,
    sinkComplete,
    sourceReady,
    complete: sourceReady && sinkComplete,
  };
}

function prefixIssue(
  issue: CompletionIssue | undefined,
  prefix: string,
): CompletionIssue | undefined {
  if (issue === undefined) return undefined;
  return {
    ...issue,
    path: issue.path === "#" ? prefix : `${prefix}${issue.path.slice(1)}`,
  };
}

export function endpointValue(
  config: JsonObject,
  role: "source" | "sink",
  key: string,
): JsonValue {
  const container = config[role];
  return isObject(container) ? (container[key] ?? {}) : {};
}

export function isObject(value: JsonValue | undefined): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}

function singleKey(value: JsonValue | undefined): string {
  return isObject(value) ? (Object.keys(value)[0] ?? "") : "";
}

export function stringValue(value: JsonValue | undefined): string {
  return typeof value === "string" ? value : "";
}
