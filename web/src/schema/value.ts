import type { JsonObject, JsonValue } from "../json";

export function isObject(value: JsonValue | undefined): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function stringArray(value: JsonValue | undefined): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}

export function jsonValuesEqual(left: JsonValue, right: JsonValue): boolean {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length &&
      left.every((value, index) => jsonValuesEqual(value, right[index]!))
    );
  }
  if (isObject(left) && isObject(right)) {
    const leftKeys = Object.keys(left);
    const rightKeys = Object.keys(right);
    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every(
        (key) => key in right && jsonValuesEqual(left[key]!, right[key]!),
      )
    );
  }
  return false;
}

export function uniqueStrings(values: string[]): string[] {
  return [...new Set(values)];
}

export function jsonPointer(value: JsonValue, pointer: string): JsonValue | undefined {
  if (pointer === "") return value;
  if (!pointer.startsWith("/")) return undefined;
  let current: JsonValue | undefined = value;
  for (const encoded of pointer.slice(1).split("/")) {
    const segment = encoded.replaceAll("~1", "/").replaceAll("~0", "~");
    if (Array.isArray(current)) {
      const index = Number(segment);
      if (!Number.isSafeInteger(index) || index < 0) return undefined;
      current = current[index];
    } else if (isObject(current)) {
      current = current[segment];
    } else {
      return undefined;
    }
  }
  return current;
}
