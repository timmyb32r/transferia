import type { ErrorObject, ValidateFunction } from "ajv";

import * as generatedValidators from "../generated/apiValidators.generated.js";
import type { ApiContract, ApiContractName } from "../generated/apiContract";

const validators = generatedValidators as Record<string, ValidateFunction>;

export function decodeApi<Name extends ApiContractName>(
  name: Name,
  value: unknown,
  path: string,
): ApiContract[Name] {
  const validate = validators[name];
  if (validate === undefined)
    throw new Error(`Unknown API contract root: ${name}`);
  if (!validate(value)) throw contractError(path, validate.errors ?? []);
  return value as ApiContract[Name];
}

function contractError(path: string, errors: ErrorObject[]): Error {
  const first = errors[0];
  if (first === undefined)
    return new Error(`Invalid control-plane response at ${path}`);
  const parts = first.instancePath
    .split("/")
    .filter(Boolean)
    .map((part) => part.replaceAll("~1", "/").replaceAll("~0", "~"));
  if (first.keyword === "additionalProperties") {
    const property = first.params.additionalProperty;
    if (typeof property === "string") parts.push(property);
  }
  const suffix = parts.length === 0 ? "" : `.${parts.join(".")}`;
  const message =
    first.keyword === "type" && typeof first.params.type === "string"
      ? `expected ${first.params.type}`
      : first.keyword === "additionalProperties"
        ? "unknown field"
        : (first.message ?? first.keyword);
  return new Error(`Invalid control-plane response at ${path}${suffix}: ${message}`);
}
