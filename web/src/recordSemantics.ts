import { branchMatches, type CompiledNode } from "./schema/compiler";
import { isObject } from "./schema/value";
import type {
  DeliveryMode,
  EndpointDefinition,
  JsonValue,
  RecordSemantics,
} from "./types";

export type DeliveryType = DeliveryMode | "batch_and_stream";

export const DELIVERY_TYPES: readonly DeliveryType[] = [
  "batch",
  "stream",
  "batch_and_stream",
];

export function sourceRecordSemantics(
  endpoint: EndpointDefinition,
  schema: CompiledNode,
  value: JsonValue,
  deliveryType: DeliveryType,
): RecordSemantics[] {
  if (deliveryType === "batch") return ["append_only"];

  const componentSemantics = selectedComponentRecordSemantics(
    schema,
    value,
    "parser",
  );
  const streamSemantics =
    componentSemantics ?? streamEndpointSemantics(endpoint);
  if (deliveryType === "stream") return streamSemantics;
  return uniqueSemantics(["append_only", ...streamSemantics]);
}

export function deliveryTypeModes(
  deliveryType: DeliveryType,
): readonly DeliveryMode[] {
  return deliveryType === "batch_and_stream"
    ? ["batch", "stream"]
    : [deliveryType];
}

export function sourceSupportsDeliveryType(
  endpoint: EndpointDefinition,
  deliveryType: DeliveryType,
): boolean {
  return deliveryTypeModes(deliveryType).every((mode) =>
    endpoint.delivery_modes.includes(mode),
  );
}

export function declaredSourceRecordSemantics(
  endpoint: EndpointDefinition,
  mode: DeliveryMode,
): RecordSemantics[] {
  if (mode === "batch") return ["append_only"];
  return streamEndpointSemantics(endpoint);
}

export function routeSupportsDeliveryType(
  source: EndpointDefinition,
  sink: EndpointDefinition,
  deliveryType: DeliveryType,
  semanticsForMode: (mode: DeliveryMode) => readonly RecordSemantics[] = (
    mode,
  ) => declaredSourceRecordSemantics(source, mode),
): boolean {
  if (!sourceSupportsDeliveryType(source, deliveryType)) return false;
  const accepted = new Set(sink.record_semantics);
  return deliveryTypeModes(deliveryType).every((mode) =>
    semanticsForMode(mode).some((semantics) => accepted.has(semantics)),
  );
}

export function selectedComponentRecordSemantics(
  node: CompiledNode,
  value: JsonValue,
  component: "parser" | "serializer" | "transformer",
): RecordSemantics[] | undefined {
  const capabilities = node.xUi.capabilities;
  if (
    capabilities?.component === component &&
    capabilities.record_semantics !== undefined
  )
    return [...capabilities.record_semantics];

  if (node.kind === "object") {
    const object = isObject(value) ? value : {};
    for (const [name, child] of Object.entries(node.properties)) {
      const semantics = selectedComponentRecordSemantics(
        child,
        object[name] ?? null,
        component,
      );
      if (semantics !== undefined) return semantics;
    }
    return undefined;
  }
  if (node.kind === "union") {
    const branch = node.branches.find((candidate) =>
      branchMatches(candidate, value),
    );
    return branch === undefined
      ? undefined
      : selectedComponentRecordSemantics(branch.node, value, component);
  }
  if (node.kind === "nullable" && value !== null)
    return selectedComponentRecordSemantics(node.inner, value, component);
  if (node.kind === "array" && Array.isArray(value)) {
    for (const item of value) {
      const semantics = selectedComponentRecordSemantics(
        node.item,
        item,
        component,
      );
      if (semantics !== undefined) return semantics;
    }
  }
  return undefined;
}

function streamEndpointSemantics(
  endpoint: EndpointDefinition,
): RecordSemantics[] {
  if (!endpoint.delivery_modes.includes("batch"))
    return [...endpoint.record_semantics];
  const streamSemantics = endpoint.record_semantics.filter(
    (semantics) => semantics !== "append_only",
  );
  return streamSemantics.length > 0 ? streamSemantics : ["append_only"];
}

function uniqueSemantics(
  semantics: readonly RecordSemantics[],
): RecordSemantics[] {
  return [...new Set(semantics)];
}
