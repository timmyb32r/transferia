import type { JsonObject, JsonSchema, JsonValue } from "../types";

interface NodeBase {
  title?: string;
  description?: string;
  defaultValue?: JsonValue;
  xUi: Record<string, JsonValue>;
}

export type CompiledNode =
  | (NodeBase & { kind: "string"; enumValues?: JsonValue[] })
  | (NodeBase & {
      kind: "number";
      integer: boolean;
      minimum?: number;
      maximum?: number;
    })
  | (NodeBase & { kind: "boolean" })
  | (NodeBase & {
      kind: "object";
      properties: Record<string, CompiledNode>;
      required: Set<string>;
    })
  | (NodeBase & { kind: "array"; item: CompiledNode })
  | (NodeBase & { kind: "union"; branches: UnionBranch[] })
  | (NodeBase & { kind: "nullable"; inner: CompiledNode });

export interface UnionBranch {
  label: string;
  node: CompiledNode;
  constant?: JsonValue;
  objectKey?: string;
  discriminator?: { key: string; value: JsonValue };
  requiredKeys?: string[];
}

export class SchemaContractError extends Error {}

export function compileSchema(root: JsonSchema): CompiledNode {
  const seen = new Set<string>();
  const compile = (input: JsonSchema, path: string): CompiledNode => {
    const schema = resolveReference(root, input, seen);
    validateKeywords(schema, path);
    const base = baseNode(schema);
    const choices = schema.oneOf ?? schema.anyOf;
    if (choices !== undefined) {
      const nonNull = choices.filter(
        (choice) => !isNullSchema(resolveReference(root, choice, seen)),
      );
      if (nonNull.length === 1 && nonNull.length !== choices.length) {
        return {
          ...base,
          kind: "nullable",
          inner: compile(nonNull[0]!, `${path}/nullable`),
        };
      }
      return {
        ...base,
        kind: "union",
        branches: choices.map((choice, index) =>
          compileBranch(choice, index, path, compile, root, seen),
        ),
      };
    }
    if (schema.enum !== undefined || schema.const !== undefined) {
      return {
        ...base,
        kind: "string",
        enumValues: schema.enum ?? [schema.const as JsonValue],
      };
    }
    const type = normalizedType(schema.type);
    if (type === "object" || schema.properties !== undefined) {
      const properties = Object.fromEntries(
        Object.entries(schema.properties ?? {}).map(([name, child]) => [
          name,
          compile(child, `${path}/${name}`),
        ]),
      );
      return {
        ...base,
        kind: "object",
        properties,
        required: new Set(schema.required ?? []),
      };
    }
    if (type === "array") {
      if (schema.items === undefined)
        throw new SchemaContractError(`${path}: array schema has no items`);
      return {
        ...base,
        kind: "array",
        item: compile(schema.items, `${path}/items`),
      };
    }
    if (type === "boolean") return { ...base, kind: "boolean" };
    if (type === "number" || type === "integer") {
      return {
        ...base,
        kind: "number",
        integer: type === "integer",
        ...(schema.minimum === undefined ? {} : { minimum: schema.minimum }),
        ...(schema.maximum === undefined ? {} : { maximum: schema.maximum }),
      };
    }
    if (type === "string" || type === undefined) {
      const options = base.xUi.options;
      return {
        ...base,
        kind: "string",
        ...(Array.isArray(options) ? { enumValues: options } : {}),
      };
    }
    throw new SchemaContractError(
      `${path}: unsupported schema type ${JSON.stringify(schema.type)}`,
    );
  };
  return compile(root, "#");
}

const SUPPORTED_KEYWORDS = new Set([
  "$schema",
  "$defs",
  "$ref",
  "type",
  "title",
  "description",
  "default",
  "const",
  "enum",
  "oneOf",
  "anyOf",
  "properties",
  "required",
  "items",
  "minimum",
  "maximum",
  "format",
  "additionalProperties",
  "x-ui",
]);

function validateKeywords(schema: JsonSchema, path: string): void {
  const unsupported = Object.keys(schema).filter(
    (key) => !SUPPORTED_KEYWORDS.has(key),
  );
  if (unsupported.length > 0) {
    throw new SchemaContractError(
      `${path}: unsupported JSON Schema keywords: ${unsupported.join(", ")}`,
    );
  }
}

export function createValue(node: CompiledNode): JsonValue {
  if (node.defaultValue !== undefined)
    return structuredClone(node.defaultValue);
  switch (node.kind) {
    case "nullable":
      return null;
    case "union":
      return {};
    case "object": {
      const value: JsonObject = {};
      for (const [name, child] of Object.entries(node.properties)) {
        if (node.required.has(name) || child.defaultValue !== undefined)
          value[name] = createValue(child);
      }
      return value;
    }
    case "array":
      return Array.from(
        { length: numberUiValue(node.xUi.initial_items) ?? 0 },
        () => createValue(node.item),
      );
    case "boolean":
      return false;
    case "number":
      return node.minimum ?? 0;
    case "string":
      return node.enumValues?.[0] ?? "";
  }
}

function numberUiValue(value: JsonValue | undefined): number | undefined {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0
    ? value
    : undefined;
}

export function isComplete(
  node: CompiledNode,
  value: JsonValue | undefined,
): boolean {
  if (value === undefined) return false;
  switch (node.kind) {
    case "nullable":
      return value === null || isComplete(node.inner, value);
    case "union": {
      const branch = node.branches.find((candidate) =>
        branchMatches(candidate, value),
      );
      return branch !== undefined && isComplete(branch.node, value);
    }
    case "object": {
      if (!isObject(value)) return false;
      for (const required of node.required) {
        if (
          !(required in value) ||
          !isComplete(node.properties[required]!, value[required])
        )
          return false;
      }
      return true;
    }
    case "array":
      return (
        Array.isArray(value) &&
        value.every((item) => isComplete(node.item, item))
      );
    case "boolean":
      return typeof value === "boolean";
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "string":
      return node.enumValues === undefined
        ? typeof value === "string" && value.length > 0
        : node.enumValues.some((candidate) => Object.is(candidate, value));
  }
}

export function branchMatches(branch: UnionBranch, value: JsonValue): boolean {
  if (branch.constant !== undefined) return Object.is(branch.constant, value);
  if (branch.discriminator !== undefined) {
    return (
      isObject(value) &&
      Object.is(value[branch.discriminator.key], branch.discriminator.value)
    );
  }
  if (branch.requiredKeys !== undefined && branch.requiredKeys.length > 0) {
    return isObject(value) && branch.requiredKeys.every((key) => key in value);
  }
  return (
    branch.objectKey !== undefined &&
    isObject(value) &&
    branch.objectKey in value
  );
}

function compileBranch(
  choice: JsonSchema,
  index: number,
  path: string,
  compile: (schema: JsonSchema, path: string) => CompiledNode,
  root: JsonSchema,
  seen: Set<string>,
): UnionBranch {
  const resolved = resolveReference(root, choice, seen);
  const node = compile(resolved, `${path}/branch/${index}`);
  const objectKey =
    resolved.properties !== undefined &&
    Object.keys(resolved.properties).length === 1
      ? Object.keys(resolved.properties)[0]
      : undefined;
  const constant =
    resolved.const ??
    (resolved.enum?.length === 1 ? resolved.enum[0] : undefined);
  const discriminatorEntry = Object.entries(resolved.properties ?? {}).find(
    ([, property]) => property.const !== undefined,
  );
  const discriminator =
    discriminatorEntry === undefined
      ? undefined
      : {
          key: discriminatorEntry[0],
          value: discriminatorEntry[1].const as JsonValue,
        };
  const fallback = constant === undefined ? objectKey : String(constant);
  const requiredKeys =
    resolved.type === "object" || resolved.properties !== undefined
      ? (resolved.required ?? [])
      : undefined;
  return {
    label: resolved.title ?? humanize(fallback ?? `Option ${index + 1}`),
    node,
    ...(constant === undefined ? {} : { constant }),
    ...(objectKey === undefined ? {} : { objectKey }),
    ...(discriminator === undefined ? {} : { discriminator }),
    ...(requiredKeys === undefined ? {} : { requiredKeys }),
  };
}

function resolveReference(
  root: JsonSchema,
  input: JsonSchema,
  seen: Set<string>,
): JsonSchema {
  if (input.$ref === undefined) return input;
  if (!input.$ref.startsWith("#/"))
    throw new SchemaContractError(
      `external schema reference is not supported: ${input.$ref}`,
    );
  if (seen.has(input.$ref))
    throw new SchemaContractError(`cyclic schema reference: ${input.$ref}`);
  const nextSeen = new Set(seen).add(input.$ref);
  const target = input.$ref
    .slice(2)
    .split("/")
    .reduce<unknown>((value, segment) => {
      if (typeof value !== "object" || value === null) return undefined;
      return (value as Record<string, unknown>)[
        segment.replaceAll("~1", "/").replaceAll("~0", "~")
      ];
    }, root);
  if (typeof target !== "object" || target === null)
    throw new SchemaContractError(`unresolved schema reference: ${input.$ref}`);
  const merged = { ...(target as JsonSchema), ...input };
  delete merged.$ref;
  return resolveReference(root, merged, nextSeen);
}

function baseNode(schema: JsonSchema): NodeBase {
  return {
    ...(schema.title === undefined ? {} : { title: schema.title }),
    ...(schema.description === undefined
      ? {}
      : { description: schema.description }),
    ...(schema.default === undefined ? {} : { defaultValue: schema.default }),
    xUi: schema["x-ui"] ?? {},
  };
}

function normalizedType(type: JsonSchema["type"]): string | undefined {
  if (!Array.isArray(type)) return type;
  const nonNull = type.filter((value) => value !== "null");
  if (nonNull.length === 1) return nonNull[0];
  throw new SchemaContractError(
    `ambiguous type union: ${JSON.stringify(type)}`,
  );
}

function isNullSchema(schema: JsonSchema): boolean {
  return schema.type === "null" || schema.const === null;
}

function isObject(value: JsonValue): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function humanize(value: string): string {
  return value
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase())
    .replace(/Pqv1/gi, "PQv1")
    .replace(/Ydb/gi, "YDB")
    .replace(/Json/gi, "JSON")
    .replace(/Ttl/gi, "TTL")
    .replace(/S3/gi, "S3");
}
