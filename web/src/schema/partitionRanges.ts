import type { JsonObject, JsonValue } from "../json";
import type { CompiledNode } from "./compiler";
import { isObject } from "./value";

export interface PartitionRangesProperty {
  arrayName: string;
  fieldName: string;
}

export function partitionRangesProperty(
  node: CompiledNode,
): PartitionRangesProperty | undefined {
  if (node.kind !== "object") return undefined;
  for (const [arrayName, property] of Object.entries(node.properties)) {
    if (property.kind !== "array" || property.item.kind !== "object") continue;
    const field = Object.entries(property.item.properties).find(
      ([, child]) => child.xUi.widget === "partition_ranges",
    );
    if (field !== undefined) return { arrayName, fieldName: field[0] };
  }
  return undefined;
}

export function hasConfiguredPartitionRanges(
  value: JsonValue,
  property: PartitionRangesProperty | undefined,
): boolean {
  if (property === undefined || !isObject(value)) return false;
  const items = value[property.arrayName];
  return (
    Array.isArray(items) &&
    items.some((item) => {
      if (!isObject(item)) return false;
      const ranges = item[property.fieldName];
      return Array.isArray(ranges) && ranges.length > 0;
    })
  );
}

export function clearConfiguredPartitionRanges(
  object: JsonObject,
  property: PartitionRangesProperty,
): JsonObject {
  const items = object[property.arrayName];
  if (!Array.isArray(items)) return object;
  return {
    ...object,
    [property.arrayName]: items.map((item) =>
      isObject(item) ? { ...item, [property.fieldName]: [] } : item,
    ),
  };
}
