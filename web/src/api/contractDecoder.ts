import contract from "../../../crates/transferia-server-contracts/contracts/server-api.schema.json";

import type { ApiContract, ApiContractName } from "../generated/apiContract";

type Schema = boolean | Record<string, unknown>;

export function decodeApi<Name extends ApiContractName>(
  name: Name,
  value: unknown,
  path: string,
): ApiContract[Name] {
  const properties = objectValue(contract.properties, "contract.properties");
  const schema = properties[name];
  if (schema === undefined)
    throw new Error(`Unknown API contract root: ${name}`);
  validate(value, schema as Schema, path);
  return value as ApiContract[Name];
}

function validate(value: unknown, rawSchema: Schema, path: string): void {
  if (rawSchema === true) return;
  if (rawSchema === false) invalid(path, "value is forbidden by the contract");
  const schema = rawSchema as Record<string, unknown>;
  if (typeof schema.$ref === "string") {
    validate(value, resolveReference(schema.$ref), path);
    return;
  }
  const choices = Array.isArray(schema.oneOf)
    ? schema.oneOf
    : Array.isArray(schema.anyOf)
      ? schema.anyOf
      : undefined;
  if (choices !== undefined) {
    const effective = schema["x-omit-none"]
      ? choices.filter((choice) => !isObject(choice) || choice.type !== "null")
      : choices;
    const matches = effective.filter((choice) =>
      accepts(value, choice as Schema),
    );
    if (matches.length === 0)
      invalid(path, "value matches no contract variant");
    if (Array.isArray(schema.oneOf) && matches.length !== 1)
      invalid(path, "value matches multiple exclusive contract variants");
    return;
  }
  if ("const" in schema && !deepEqual(value, schema.const))
    invalid(path, `expected ${JSON.stringify(schema.const)}`);
  if (
    Array.isArray(schema.enum) &&
    !schema.enum.some((item) => deepEqual(value, item))
  )
    invalid(path, "value is not in the allowed enum");
  const rawTypes = Array.isArray(schema.type) ? schema.type : [schema.type];
  const types = schema["x-omit-none"]
    ? rawTypes.filter((type) => type !== "null")
    : rawTypes;
  if (types[0] === undefined) return;
  if (!types.some((type) => acceptsType(value, type, schema, path)))
    invalid(path, `expected ${types.join(" or ")}`);
}

function accepts(value: unknown, schema: Schema): boolean {
  try {
    validate(value, schema, "$candidate");
    return true;
  } catch {
    return false;
  }
}

function acceptsType(
  value: unknown,
  type: unknown,
  schema: Record<string, unknown>,
  path: string,
): boolean {
  switch (type) {
    case "null":
      return value === null;
    case "string":
      if (typeof value !== "string") return false;
      if (
        typeof schema.pattern === "string" &&
        !new RegExp(schema.pattern).test(value)
      )
        invalid(path, `string does not match ${schema.pattern}`);
      return true;
    case "boolean":
      return typeof value === "boolean";
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "integer":
      if (typeof value !== "number" || !Number.isSafeInteger(value))
        return false;
      if (typeof schema.minimum === "number" && value < schema.minimum)
        return false;
      if (typeof schema.maximum === "number" && value > schema.maximum)
        return false;
      return true;
    case "array":
      if (!Array.isArray(value)) return false;
      value.forEach((item, index) =>
        validate(item, (schema.items ?? true) as Schema, `${path}[${index}]`),
      );
      return true;
    case "object":
      if (!isObject(value)) return false;
      validateObject(value, schema, path);
      return true;
    default:
      return false;
  }
}

function validateObject(
  value: Record<string, unknown>,
  schema: Record<string, unknown>,
  path: string,
): void {
  const properties = isObject(schema.properties) ? schema.properties : {};
  const required = Array.isArray(schema.required)
    ? schema.required.filter((name): name is string => typeof name === "string")
    : [];
  for (const name of required) {
    if (!(name in value))
      invalid(`${path}.${name}`, "required field is missing");
  }
  for (const [name, item] of Object.entries(value)) {
    const property = properties[name];
    if (property !== undefined) {
      validate(item, property as Schema, `${path}.${name}`);
    } else if (schema.additionalProperties === false) {
      invalid(`${path}.${name}`, "unknown field");
    } else if (isObject(schema.additionalProperties)) {
      validate(item, schema.additionalProperties, `${path}.${name}`);
    }
  }
}

function resolveReference(reference: string): Schema {
  const prefix = "#/$defs/";
  if (!reference.startsWith(prefix))
    throw new Error(`Unsupported API schema ref: ${reference}`);
  const definitions = objectValue(contract.$defs, "contract.$defs");
  const schema = definitions[reference.slice(prefix.length)];
  if (schema === undefined)
    throw new Error(`Unknown API schema ref: ${reference}`);
  return schema as Schema;
}

function objectValue(value: unknown, path: string): Record<string, unknown> {
  if (!isObject(value))
    throw new Error(`Invalid generated API schema at ${path}`);
  return value;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function deepEqual(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function invalid(path: string, message: string): never {
  throw new Error(`Invalid control-plane response at ${path}: ${message}`);
}
