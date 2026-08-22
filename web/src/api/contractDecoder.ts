import Ajv2020, { type ErrorObject, type ValidateFunction } from "ajv/dist/2020";
import addFormats from "ajv-formats";

import rawContract from "../../../crates/transferia-server-contracts/contracts/server-api.schema.json";
import type { ApiContract, ApiContractName } from "../generated/apiContract";

type SchemaObject = Record<string, unknown>;

const contract = withoutOmittedNulls(rawContract) as SchemaObject;
const ajv = new Ajv2020({ allErrors: true, strict: true });
addFormats(ajv);
ajv.addKeyword({ keyword: "x-omit-none", schemaType: "boolean" });
ajv.addKeyword({ keyword: "x-typescript-type", schemaType: "string" });
for (const format of ["int64", "uint", "uint32", "uint64"])
  ajv.addFormat(format, true);

const validators = new Map<string, ValidateFunction>();
const properties = objectValue(contract.properties, "contract.properties");
for (const [name, schema] of Object.entries(properties)) {
  validators.set(
    name,
    ajv.compile({
      ...(typeof contract.$schema === "string"
        ? { $schema: contract.$schema }
        : {}),
      $defs: contract.$defs,
      ...(schema as SchemaObject),
    }),
  );
}

export function decodeApi<Name extends ApiContractName>(
  name: Name,
  value: unknown,
  path: string,
): ApiContract[Name] {
  const validate = validators.get(name);
  if (validate === undefined)
    throw new Error(`Unknown API contract root: ${name}`);
  if (!validate(value)) throw contractError(path, validate.errors ?? []);
  return value as ApiContract[Name];
}

function contractError(path: string, errors: ErrorObject[]): Error {
  const first = errors[0];
  if (first === undefined)
    return new Error(`Invalid control-plane response at ${path}`);
  const location = first.instancePath
    .split("/")
    .filter(Boolean)
    .map((part) => part.replaceAll("~1", "/").replaceAll("~0", "~"))
    .join(".");
  const suffix = location.length === 0 ? "" : `.${location}`;
  return new Error(
    `Invalid control-plane response at ${path}${suffix}: ${first.message ?? first.keyword}`,
  );
}

function withoutOmittedNulls(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(withoutOmittedNulls);
  if (!isObject(value)) return value;
  const result = Object.fromEntries(
    Object.entries(value).map(([key, child]) => [key, withoutOmittedNulls(child)]),
  );
  if (result["x-omit-none"] === true) {
    if (Array.isArray(result.type))
      result.type = result.type.filter((type) => type !== "null");
    for (const key of ["oneOf", "anyOf"] as const) {
      const choices = result[key];
      if (Array.isArray(choices))
        result[key] = choices.filter(
          (choice) => !isObject(choice) || choice.type !== "null",
        );
    }
  }
  return result;
}

function objectValue(value: unknown, path: string): SchemaObject {
  if (!isObject(value)) throw new Error(`Invalid generated API schema at ${path}`);
  return value;
}

function isObject(value: unknown): value is SchemaObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
