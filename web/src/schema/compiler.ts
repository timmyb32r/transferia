import type { JsonObject, JsonSchema, JsonValue } from "../types";

export const SCHEMA_DIALECT_VERSION =
  "transferia-schema-dialect-path-options-v2";
import {
  NO_WIDGETS,
  type WidgetContracts,
  widgetSupportsKind,
} from "./widgetDefinitions";
import { decodeUiHints, type UiHints } from "./uiHints";

interface NodeBase {
  title?: string;
  description?: string;
  defaultValue?: JsonValue;
  hidden?: boolean;
  xUi: UiHints;
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
      additionalProperties?: boolean;
    })
  | (NodeBase & { kind: "array"; item: CompiledNode; minItems?: number })
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

export function compileSchema(
  root: JsonSchema,
  widgets: WidgetContracts = NO_WIDGETS,
): CompiledNode {
  const compile = (
    input: JsonSchema,
    path: string,
    activeReferences = new Set<string>(),
  ): CompiledNode => {
    if (input.$ref !== undefined) {
      const reference = input.$ref;
      if (activeReferences.has(reference))
        throw new SchemaContractError(
          `${path}: cyclic schema reference: ${reference}`,
        );
      const merged = { ...referenceTarget(root, reference), ...input };
      delete merged.$ref;
      return compile(merged, path, new Set(activeReferences).add(reference));
    }
    const schema = input;
    validateKeywords(schema, path);
    const base = baseNode(schema, path, widgets);
    if (Array.isArray(schema.type)) {
      const nonNull = schema.type.filter((type) => type !== "null");
      if (nonNull.length === 1 && nonNull.length !== schema.type.length) {
        const nonNullType = nonNull[0]!;
        return {
          ...base,
          kind: "nullable",
          inner: compile(
            { ...schema, type: nonNullType },
            `${path}/nullable`,
            activeReferences,
          ),
        };
      }
      throw new SchemaContractError(
        `${path}: ambiguous type union: ${JSON.stringify(schema.type)}`,
      );
    }
    const choices = schema.oneOf ?? schema.anyOf;
    if (choices !== undefined) {
      const nonNull = choices.filter(
        (choice) => !isNullSchema(resolveShallowReference(root, choice)),
      );
      if (nonNull.length === 1 && nonNull.length !== choices.length) {
        return {
          ...base,
          kind: "nullable",
          inner: compile(nonNull[0]!, `${path}/nullable`, activeReferences),
        };
      }
      return {
        ...base,
        kind: "union",
        branches: choices
          .map((choice, index) => ({
            branch: compileBranch(
              choice,
              index,
              path,
              compile,
              root,
              activeReferences,
            ),
            index,
          }))
          .sort(
            (left, right) =>
              (left.branch.node.xUi.order ?? 0) -
                (right.branch.node.xUi.order ?? 0) || left.index - right.index,
          )
          .map(({ branch }) => branch),
      };
    }
    if (schema.enum !== undefined || schema.const !== undefined) {
      const enumValues = schema.enum ?? [schema.const as JsonValue];
      if (!enumValues.every((value) => typeof value === "string")) {
        throw new SchemaContractError(
          `${path}: only string enum and const values are supported`,
        );
      }
      return {
        ...base,
        kind: "string",
        enumValues,
        ...(enumValues.length === 1 ? { hidden: true } : {}),
      };
    }
    const type = schema.type;
    if (type === "object" || schema.properties !== undefined) {
      const properties = Object.fromEntries(
        Object.entries(schema.properties ?? {}).map(([name, child]) => [
          name,
          compile(child, `${path}/${name}`, activeReferences),
        ]),
      );
      return {
        ...base,
        kind: "object",
        properties,
        required: new Set(schema.required ?? []),
        additionalProperties: schema.additionalProperties !== false,
      };
    }
    if (type === "array") {
      if (schema.items === undefined)
        throw new SchemaContractError(`${path}: array schema has no items`);
      return {
        ...base,
        kind: "array",
        item: compile(schema.items, `${path}/items`, activeReferences),
        ...(schema.minItems === undefined ? {} : { minItems: schema.minItems }),
      };
    }
    if (type === "boolean") return { ...base, kind: "boolean" };
    if (type === "number" || type === "integer") {
      const minimum = schema.format?.startsWith("uint")
        ? Math.max(0, schema.minimum ?? 0)
        : schema.minimum;
      return {
        ...base,
        kind: "number",
        integer: type === "integer",
        ...(minimum === undefined ? {} : { minimum }),
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
  const compiled = compile(root, "#");
  validateWidgetTree(compiled, "#", widgets);
  return compiled;
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
  "minItems",
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
  if (schema.format !== undefined && !NUMERIC_FORMATS.has(schema.format)) {
    throw new SchemaContractError(
      `${path}: unsupported JSON Schema format: ${schema.format}`,
    );
  }
  if (schema.format !== undefined && !numericSchemaType(schema.type)) {
    throw new SchemaContractError(
      `${path}: numeric JSON Schema format ${schema.format} requires a numeric type`,
    );
  }
  if (
    schema.minItems !== undefined &&
    (!Number.isSafeInteger(schema.minItems) || schema.minItems < 0)
  ) {
    throw new SchemaContractError(
      `${path}: minItems must be a non-negative integer`,
    );
  }
  if (
    schema.additionalProperties !== undefined &&
    typeof schema.additionalProperties !== "boolean"
  ) {
    throw new SchemaContractError(
      `${path}: schema-valued additionalProperties is not supported`,
    );
  }
  if (
    schema.minimum !== undefined &&
    (!Number.isFinite(schema.minimum) || typeof schema.minimum !== "number")
  ) {
    throw new SchemaContractError(`${path}: minimum must be a finite number`);
  }
  if (
    schema.maximum !== undefined &&
    (!Number.isFinite(schema.maximum) || typeof schema.maximum !== "number")
  ) {
    throw new SchemaContractError(`${path}: maximum must be a finite number`);
  }
  if (
    schema.minimum !== undefined &&
    schema.maximum !== undefined &&
    schema.minimum > schema.maximum
  ) {
    throw new SchemaContractError(`${path}: minimum exceeds maximum`);
  }
  if (schema.required !== undefined) {
    const unique = new Set(schema.required);
    if (unique.size !== schema.required.length)
      throw new SchemaContractError(`${path}: required contains duplicates`);
    for (const required of schema.required) {
      if (schema.properties?.[required] === undefined)
        throw new SchemaContractError(
          `${path}: required property ${JSON.stringify(required)} is missing from properties`,
        );
    }
  }
}

function validateWidgetTree(
  node: CompiledNode,
  path: string,
  widgets: WidgetContracts,
): void {
  const widget = node.xUi.widget;
  const supported =
    widget === undefined ||
    widgetSupportsKind(widgets, widget, node.kind) ||
    (node.kind === "nullable" &&
      widgetSupportsKind(widgets, widget, node.inner.kind));
  if (!supported) {
    throw new SchemaContractError(
      `${path}: x-ui widget ${JSON.stringify(widget)} does not support ${node.kind}`,
    );
  }
  if (node.kind === "object") {
    for (const [name, child] of Object.entries(node.properties))
      validateWidgetTree(child, `${path}/${name}`, widgets);
  } else if (node.kind === "array") {
    validateWidgetTree(node.item, `${path}/items`, widgets);
  } else if (node.kind === "nullable") {
    validateWidgetTree(node.inner, `${path}/nullable`, widgets);
  } else if (node.kind === "union") {
    node.branches.forEach((branch, index) =>
      validateWidgetTree(branch.node, `${path}/branch-${index}`, widgets),
    );
  }
}

export function createValue(node: CompiledNode): JsonValue {
  const deterministic = deterministicValue(node);
  if (deterministic !== undefined) return deterministic;
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
      return null;
    case "string":
      return "";
  }
}

/** A schema-authored or logically unique value; never an invented UI fallback. */
export function deterministicValue(
  node: CompiledNode,
): JsonValue | undefined {
  if (node.defaultValue !== undefined)
    return structuredClone(node.defaultValue);
  if (node.kind === "string" && node.enumValues?.length === 1)
    return structuredClone(node.enumValues[0]!);
  return undefined;
}

/** Creates exactly the value produced when the user selects this union branch. */
export function materializeBranch(branch: UnionBranch): JsonValue {
  const created = branch.constant ?? createValue(branch.node);
  return branch.discriminator !== undefined && isObject(created)
    ? {
        ...created,
        [branch.discriminator.key]: structuredClone(
          branch.discriminator.value,
        ),
      }
    : created;
}

export function acceptsDraftSeed(
  node: CompiledNode,
  value: JsonValue,
): boolean {
  return draftSeedError(node, value) === undefined;
}

export function draftSeedError(
  node: CompiledNode,
  value: JsonValue,
  path = "#",
): string | undefined {
  if (value === null) return undefined;
  switch (node.kind) {
    case "nullable":
      return draftSeedError(node.inner, value, path);
    case "union":
      return node.branches.some(
        (branch) => draftSeedError(branch.node, value, path) === undefined,
      )
        ? undefined
        : `${path}: value does not match any union branch`;
    case "object": {
      if (!isObject(value)) return `${path}: expected an object`;
      if (
        !node.additionalProperties &&
        Object.keys(value).some((name) => node.properties[name] === undefined)
      )
        return `${path}: contains an unknown property`;
      for (const [name, childValue] of Object.entries(value)) {
        const child = node.properties[name];
        if (child === undefined) continue;
        const error = draftSeedError(child, childValue, `${path}/${name}`);
        if (error !== undefined) return error;
      }
      return undefined;
    }
    case "array":
      if (!Array.isArray(value)) return `${path}: expected an array`;
      for (const [index, item] of value.entries()) {
        const error = draftSeedError(node.item, item, `${path}/${index}`);
        if (error !== undefined) return error;
      }
      return undefined;
    case "boolean":
      return typeof value === "boolean"
        ? undefined
        : `${path}: expected a boolean`;
    case "number":
      return typeof value === "number" &&
        Number.isFinite(value) &&
        (!node.integer || Number.isInteger(value)) &&
        (node.minimum === undefined || value >= node.minimum) &&
        (node.maximum === undefined || value <= node.maximum)
        ? undefined
        : `${path}: invalid numeric value`;
    case "string":
      return value === "" ||
        (typeof value === "string" &&
          (node.enumValues === undefined || node.enumValues.includes(value)))
        ? undefined
        : `${path}: invalid string value`;
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
  return firstCompletionIssue(node, value) === undefined;
}

export function isFieldComplete(
  node: CompiledNode,
  value: JsonValue | undefined,
  required: boolean,
): boolean {
  return firstCompletionIssue(node, value, required) === undefined;
}

export type CompletionIssueCode =
  | "missing"
  | "invalid_type"
  | "invalid_value"
  | "min_items"
  | "unknown_property"
  | "variant";

export interface CompletionIssue {
  path: string;
  code: CompletionIssueCode;
  /** No generated UI control can repair this issue. */
  hidden: boolean;
}

/** Returns the same first blocker used by action readiness and field guidance. */
export function firstCompletionIssue(
  node: CompiledNode,
  value: JsonValue | undefined,
  required = true,
  path = "#",
): CompletionIssue | undefined {
  return completionIssue(node, value, required, path, false);
}

function completionIssue(
  node: CompiledNode,
  value: JsonValue | undefined,
  required: boolean,
  path: string,
  hiddenAncestor: boolean,
): CompletionIssue | undefined {
  const hidden = hiddenAncestor || node.hidden === true;
  if (value === undefined) {
    if (!required) return undefined;
    if (node.kind === "object" && !hidden) {
      for (const name of node.required) {
        const issue = completionIssue(
          node.properties[name]!,
          undefined,
          true,
          `${path}/${escapePointerSegment(name)}`,
          hidden,
        );
        if (issue !== undefined) return issue;
      }
    }
    return { path, code: "missing", hidden };
  }
  switch (node.kind) {
    case "nullable":
      return value === null
        ? undefined
        : completionIssue(node.inner, value, required, path, hidden);
    case "union": {
      const branch = node.branches.find((candidate) =>
        branchMatches(candidate, value),
      );
      return branch === undefined
        ? { path, code: "variant", hidden }
        : completionIssue(branch.node, value, required, path, hidden);
    }
    case "object": {
      if (!isObject(value)) return { path, code: "invalid_type", hidden };
      if (node.additionalProperties === false) {
        const unknown = Object.keys(value).find(
          (key) => node.properties[key] === undefined,
        );
        if (unknown !== undefined)
          return {
            path: `${path}/${escapePointerSegment(unknown)}`,
            code: "unknown_property",
            hidden: true,
          };
      }
      for (const name of node.required) {
        const issue = completionIssue(
          node.properties[name]!,
          value[name],
          true,
          `${path}/${escapePointerSegment(name)}`,
          hidden,
        );
        if (issue !== undefined) return issue;
      }
      for (const [name, childValue] of Object.entries(value)) {
        if (node.required.has(name)) continue;
        const child = node.properties[name];
        if (child === undefined) continue;
        const issue = completionIssue(
          child,
          childValue,
          false,
          `${path}/${escapePointerSegment(name)}`,
          hidden,
        );
        if (issue !== undefined) return issue;
      }
      return undefined;
    }
    case "array": {
      if (!Array.isArray(value)) return { path, code: "invalid_type", hidden };
      if (node.minItems !== undefined && value.length < node.minItems)
        return { path, code: "min_items", hidden };
      for (const [index, item] of value.entries()) {
        const issue = completionIssue(
          node.item,
          item,
          true,
          `${path}/${index}`,
          hidden,
        );
        if (issue !== undefined) return issue;
      }
      return undefined;
    }
    case "boolean":
      return typeof value === "boolean"
        ? undefined
        : { path, code: "invalid_type", hidden };
    case "number": {
      if (typeof value !== "number" || !Number.isFinite(value))
        return { path, code: "invalid_type", hidden };
      return (!node.integer || Number.isSafeInteger(value)) &&
        (node.minimum === undefined || value >= node.minimum) &&
        (node.maximum === undefined || value <= node.maximum)
        ? undefined
        : { path, code: "invalid_value", hidden };
    }
    case "string": {
      if (typeof value !== "string")
        return { path, code: "invalid_type", hidden };
      if (required && value.length === 0)
        return { path, code: "missing", hidden };
      return node.enumValues === undefined ||
        node.enumValues.some((candidate) => Object.is(candidate, value))
        ? undefined
        : { path, code: "invalid_value", hidden };
    }
  }
}

function escapePointerSegment(value: string): string {
  return value.replaceAll("~", "~0").replaceAll("/", "~1");
}

const NUMERIC_FORMATS = new Set([
  "uint",
  "uint8",
  "uint16",
  "uint32",
  "uint64",
  "int8",
  "int16",
  "int32",
  "int64",
  "float",
  "double",
]);

function numericSchemaType(type: JsonSchema["type"]): boolean {
  const types = Array.isArray(type) ? type : [type];
  return types.some(
    (candidate) => candidate === "number" || candidate === "integer",
  );
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
  compile: (
    schema: JsonSchema,
    path: string,
    activeReferences?: Set<string>,
  ) => CompiledNode,
  root: JsonSchema,
  activeReferences: Set<string>,
): UnionBranch {
  const resolved = resolveShallowReference(root, choice);
  const node = compile(choice, `${path}/branch/${index}`, activeReferences);
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

function resolveShallowReference(
  root: JsonSchema,
  input: JsonSchema,
  seen = new Set<string>(),
): JsonSchema {
  if (input.$ref === undefined) return input;
  const reference = input.$ref;
  if (seen.has(reference))
    throw new SchemaContractError(`cyclic schema reference: ${reference}`);
  const merged = { ...referenceTarget(root, reference), ...input };
  delete merged.$ref;
  return resolveShallowReference(root, merged, new Set(seen).add(reference));
}

function referenceTarget(root: JsonSchema, reference: string): JsonSchema {
  if (!reference.startsWith("#/"))
    throw new SchemaContractError(
      `external schema reference is not supported: ${reference}`,
    );
  const target = reference
    .slice(2)
    .split("/")
    .reduce<unknown>((value, segment) => {
      if (typeof value !== "object" || value === null) return undefined;
      return (value as Record<string, unknown>)[
        segment.replaceAll("~1", "/").replaceAll("~0", "~")
      ];
    }, root);
  if (typeof target !== "object" || target === null)
    throw new SchemaContractError(`unresolved schema reference: ${reference}`);
  return target as JsonSchema;
}

function baseNode(
  schema: JsonSchema,
  path: string,
  widgets: WidgetContracts,
): NodeBase {
  const xUi = decodeUiHints(schema["x-ui"], path, failSchemaContract, widgets);
  const widget =
    xUi.widget === undefined ? undefined : widgets.definition(xUi.widget);
  return {
    ...(schema.title === undefined ? {} : { title: schema.title }),
    ...(schema.description === undefined
      ? {}
      : { description: schema.description }),
    ...(schema.default !== undefined
      ? { defaultValue: schema.default }
      : schema.const !== undefined
        ? { defaultValue: schema.const }
        : {}),
    ...(widget?.hidden === true ? { hidden: true } : {}),
    xUi,
  };
}

function failSchemaContract(message: string): never {
  throw new SchemaContractError(message);
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
