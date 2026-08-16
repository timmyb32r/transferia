import { compileSchema, type CompiledNode } from "../schema/compiler";
import type {
  EndpointDefinition,
  JsonObject,
  JsonValue,
  UiCatalog,
} from "../types";

const compiledSchemaCache = new WeakMap<object, CompiledNode>();

export function compiledSchema(
  schema: UiCatalog["common_schema"],
): CompiledNode {
  const cached = compiledSchemaCache.get(schema);
  if (cached !== undefined) return cached;
  const compiled = compileSchema(schema);
  compiledSchemaCache.set(schema, compiled);
  return compiled;
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

export function selectedEndpoints(catalog: UiCatalog, config: JsonObject): {
  sourceKey: string;
  sinkKey: string;
  source: EndpointDefinition | undefined;
  sink: EndpointDefinition | undefined;
  error?: string;
} {
  const sourceKey = singleKey(config.source);
  const sinkKey = singleKey(config.sink);
  const source = catalog.providers.find(
    (provider) => provider.key === sourceKey,
  )?.source;
  const sink = catalog.providers.find(
    (provider) => provider.key === sinkKey,
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
        catalog.providers.find((provider) => provider.key === sourceKey)
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

export function endpointValue(
  config: JsonObject,
  role: "source" | "sink",
  key: string,
): JsonValue {
  const container = config[role];
  return isObject(container) ? (container[key] ?? {}) : {};
}

export function isObject(
  value: JsonValue | undefined,
): value is JsonObject {
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
