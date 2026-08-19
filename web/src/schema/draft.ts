import type { JsonValue } from "../json";
import { createValue, type CompiledNode } from "./compiler";

/**
 * Produces an editor seed only for a property that is genuinely absent.
 * Explicit null, false, zero, and empty strings are user data and must survive.
 */
export function draftValue(
  node: CompiledNode,
  value: JsonValue | undefined,
): JsonValue {
  return value === undefined ? createValue(node) : value;
}
