import {
  branchMatches,
  SchemaContractError,
  type CompiledNode,
} from "./schema/compiler";
import type { UiCapabilityHints } from "./schema/uiHints";
import { isObject } from "./schema/value";
import type {
  DeliveryMode,
  EndpointDefinition,
  JsonValue,
  RecordSemantics,
} from "./types";

export type DeliveryType = DeliveryMode;

export interface EndpointCapabilities {
  delivery_modes: readonly DeliveryMode[];
  record_semantics: readonly RecordSemantics[];
}

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
  const endpointCapabilities = configuredEndpointCapabilities(
    endpoint,
    schema,
    value,
    "source",
  );
  const componentSemantics = selectedComponentRecordSemantics(
    schema,
    value,
    "parser",
  );
  if (deliveryType === "batch") return componentSemantics ?? ["append_only"];
  const streamSemantics =
    componentSemantics ?? streamEndpointSemantics(endpointCapabilities);
  if (deliveryType === "stream") return streamSemantics;
  return uniqueSemantics(["append_only", ...streamSemantics]);
}

/** Every possible input operation must survive both the sink and its serializer. */
export function acceptsConfiguredRecordSemantics(
  produced: readonly RecordSemantics[],
  sink: EndpointCapabilities,
  schema: CompiledNode,
  value: JsonValue,
): boolean {
  const serializer = selectedComponentRecordSemantics(schema, value, "serializer");
  return produced.length > 0 && produced.every((semantics) =>
    sink.record_semantics.includes(semantics) &&
    (serializer === undefined || serializer.includes(semantics)),
  );
}

export function sourceSupportsDeliveryType(
  endpoint: EndpointCapabilities,
  deliveryType: DeliveryType,
): boolean {
  return endpoint.delivery_modes.includes(deliveryType);
}

export function configuredSourceSupportsDeliveryType(
  endpoint: EndpointDefinition,
  schema: CompiledNode,
  value: JsonValue,
  deliveryType: DeliveryType,
): boolean {
  return sourceSupportsDeliveryType(
    configuredEndpointCapabilities(endpoint, schema, value, "source"),
    deliveryType,
  );
}

export function declaredSourceRecordSemantics(
  endpoint: EndpointCapabilities,
  mode: DeliveryMode,
): RecordSemantics[] {
  if (mode === "batch") return ["append_only"];
  const streamSemantics = streamEndpointSemantics(endpoint);
  return mode === "stream"
    ? streamSemantics
    : uniqueSemantics(["append_only", ...streamSemantics]);
}

export function routeSupportsDeliveryType(
  source: EndpointCapabilities,
  sink: EndpointCapabilities,
  deliveryType: DeliveryType,
  semanticsForMode: (mode: DeliveryMode) => readonly RecordSemantics[] = (
    mode,
  ) => declaredSourceRecordSemantics(source, mode),
): boolean {
  if (!sourceSupportsDeliveryType(source, deliveryType)) return false;
  const accepted = new Set(sink.record_semantics);
  const produced = semanticsForMode(deliveryType);
  return deliveryType === "batch_and_stream"
    ? produced.length > 0 &&
        produced.every((semantics) => accepted.has(semantics))
    : produced.some((semantics) => accepted.has(semantics));
}

export function configuredEndpointCapabilities(
  endpoint: EndpointDefinition,
  schema: CompiledNode,
  value: JsonValue,
  component: "source" | "destination",
): EndpointCapabilities {
  const selected = selectedEndpointCapability(schema, value, component);
  if (selected === undefined) return endpoint;
  const deliveryModes =
    component === "source" ? selected.delivery_modes! : endpoint.delivery_modes;
  const recordSemantics = selected.record_semantics!;
  if (
    deliveryModes.some((mode) => !endpoint.delivery_modes.includes(mode)) ||
    recordSemantics.some(
      (semantics) => !endpoint.record_semantics.includes(semantics),
    )
  )
    throw new SchemaContractError(
      `configured ${component} capabilities must be a subset of the catalog aggregate`,
    );
  return {
    delivery_modes: deliveryModes,
    record_semantics: recordSemantics,
  };
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
  endpoint: EndpointCapabilities,
): RecordSemantics[] {
  if (!endpoint.delivery_modes.includes("batch"))
    return [...endpoint.record_semantics];
  const streamSemantics = endpoint.record_semantics.filter(
    (semantics) => semantics !== "append_only",
  );
  return streamSemantics.length > 0 ? streamSemantics : ["append_only"];
}

interface LocatedEndpointCapability {
  capabilities: UiCapabilityHints;
  depth: number;
}

function selectedEndpointCapability(
  node: CompiledNode,
  value: JsonValue,
  component: "source" | "destination",
): UiCapabilityHints | undefined {
  const candidates = activeEndpointCapabilities(node, value, component, 0);
  if (candidates.length === 0) return undefined;
  const deepest = Math.max(...candidates.map((candidate) => candidate.depth));
  const selected = candidates.filter((candidate) => candidate.depth === deepest);
  const signature = endpointCapabilitySignature(selected[0]!.capabilities);
  if (
    selected.some(
      (candidate) =>
        endpointCapabilitySignature(candidate.capabilities) !== signature,
    )
  )
    throw new SchemaContractError(
      `configuration activates conflicting ${component} capabilities`,
    );
  return selected[0]!.capabilities;
}

function endpointCapabilitySignature(capabilities: UiCapabilityHints): string {
  return JSON.stringify({
    delivery_modes: [...(capabilities.delivery_modes ?? [])].sort(),
    record_semantics: [...(capabilities.record_semantics ?? [])].sort(),
  });
}

function activeEndpointCapabilities(
  node: CompiledNode,
  value: JsonValue,
  component: "source" | "destination",
  depth: number,
): LocatedEndpointCapability[] {
  const own =
    node.xUi.capabilities?.component === component
      ? [{ capabilities: node.xUi.capabilities, depth }]
      : [];
  if (node.kind === "nullable")
    return value === null
      ? own
      : [
          ...own,
          ...activeEndpointCapabilities(node.inner, value, component, depth + 1),
        ];
  if (node.kind === "union") {
    const branch = node.branches.find((candidate) =>
      branchMatches(candidate, value),
    );
    return branch === undefined
      ? own
      : [
          ...own,
          ...activeEndpointCapabilities(
            branch.node,
            value,
            component,
            depth + 1,
          ),
        ];
  }
  if (node.kind === "object" && isObject(value))
    return [
      ...own,
      ...Object.entries(node.properties).flatMap(([name, child]) =>
        Object.hasOwn(value, name)
          ? activeEndpointCapabilities(
              child,
              value[name]!,
              component,
              depth + 1,
            )
          : [],
      ),
    ];
  if (node.kind === "array" && Array.isArray(value))
    return [
      ...own,
      ...value.flatMap((item) =>
        activeEndpointCapabilities(node.item, item, component, depth + 1),
      ),
    ];
  return own;
}

function uniqueSemantics(
  semantics: readonly RecordSemantics[],
): RecordSemantics[] {
  return [...new Set(semantics)];
}
