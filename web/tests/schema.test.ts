import { describe, expect, it } from "vitest";

import {
  acceptsDraftSeed,
  compileSchema,
  createValue,
  isComplete,
  SchemaContractError,
} from "../src/schema/compiler";

describe("schema compiler", () => {
  it("requires an explicit one-of selection", () => {
    const node = compileSchema({
      oneOf: [
        {
          title: "Token",
          type: "object",
          properties: { token: { type: "string" } },
          required: ["token"],
        },
        {
          title: "Token file",
          type: "object",
          properties: { token_file: { type: "string" } },
          required: ["token_file"],
        },
      ],
    });
    expect(isComplete(node, {})).toBe(false);
    expect(isComplete(node, { token: "secret" })).toBe(true);
  });

  it("selects tagged object variants from their discriminator", () => {
    const node = compileSchema({
      oneOf: [
        {
          title: "Token",
          type: "object",
          properties: { type: { const: "token" }, token: { type: "string" } },
          required: ["type", "token"],
        },
        {
          title: "Token file",
          type: "object",
          properties: {
            type: { const: "token_file" },
            token_file: { type: "string" },
          },
          required: ["type", "token_file"],
        },
      ],
    });
    expect(isComplete(node, { type: "token", token: "secret" })).toBe(true);
  });

  it("recognizes object variants with multiple required keys", () => {
    const node = compileSchema({
      anyOf: [
        {
          title: "JSON",
          type: "object",
          properties: {
            common: { type: "object" },
            json_parser: { type: "object" },
          },
          required: ["common", "json_parser"],
        },
        {
          title: "Discard",
          type: "object",
          properties: { benchmark_discard: { type: "object" } },
          required: ["benchmark_discard"],
        },
      ],
    });
    expect(isComplete(node, { common: {}, json_parser: {} })).toBe(true);
  });

  it("fails fast on unsupported schema types", () => {
    expect(() => compileSchema({ type: "null" })).toThrow(SchemaContractError);
  });

  it("fails fast on unsupported schema keywords", () => {
    expect(() => compileSchema({ type: "string", pattern: "^x$" })).toThrow(
      /unsupported JSON Schema keywords/,
    );
  });

  it("materializes schema-defined initial array rows", () => {
    const node = compileSchema({
      type: "array",
      items: { type: "string", default: "first" },
      "x-ui": { initial_items: 1 },
    });
    expect(createValue(node)).toEqual(["first"]);
  });

  it("preserves nullable type arrays", () => {
    const node = compileSchema({ type: ["string", "null"] });
    expect(isComplete(node, null)).toBe(true);
    expect(isComplete(node, "value")).toBe(true);
    expect(isComplete(node, 1)).toBe(false);
  });

  it("validates integer ranges", () => {
    const node = compileSchema({
      type: "integer",
      minimum: 1,
      maximum: 3,
    });
    expect(isComplete(node, 1)).toBe(true);
    expect(isComplete(node, 1.5)).toBe(false);
    expect(isComplete(node, 4)).toBe(false);
  });

  it("enforces additionalProperties false", () => {
    const node = compileSchema({
      type: "object",
      properties: { known: { type: "string" } },
      additionalProperties: false,
    });
    expect(isComplete(node, { known: "value" })).toBe(true);
    expect(isComplete(node, { known: "value", typo: true })).toBe(false);
  });

  it("rejects schema features the form cannot honor", () => {
    expect(() => compileSchema({ type: "string", format: "email" })).toThrow(
      /unsupported JSON Schema format/,
    );
    expect(() =>
      compileSchema({
        type: "object",
        additionalProperties: { type: "string" },
      }),
    ).toThrow(/schema-valued additionalProperties/);
    expect(() => compileSchema({ type: "number", enum: [1, 2] })).toThrow(
      /only string enum and const values/,
    );
  });

  it("detects structural reference cycles", () => {
    expect(() =>
      compileSchema({
        $defs: {
          node: {
            type: "object",
            properties: { child: { $ref: "#/$defs/node" } },
          },
        },
        $ref: "#/$defs/node",
      }),
    ).toThrow(/cyclic schema reference/);
  });

  it("rejects inconsistent structural contracts", () => {
    expect(() =>
      compileSchema({
        type: "object",
        properties: {},
        required: ["missing"],
      }),
    ).toThrow(/missing from properties/);
    expect(() =>
      compileSchema({ type: "number", minimum: 2, maximum: 1 }),
    ).toThrow(/minimum exceeds maximum/);
  });

  it("rejects unknown or malformed x-ui contracts", () => {
    expect(() =>
      compileSchema({ type: "string", "x-ui": { widget: "magic" } }),
    ).toThrow(/unsupported x-ui widget/);
    expect(() =>
      compileSchema({ type: "array", items: { type: "string" }, "x-ui": { initial_items: -1 } }),
    ).toThrow(/initial_items/);
    expect(() =>
      compileSchema({ type: "string", "x-ui": { surprise: true } }),
    ).toThrow(/unsupported x-ui hints/);
    expect(() =>
      compileSchema({ type: "string", "x-ui": { widget: "compact_array" } }),
    ).toThrow(/does not support string/);
  });

  it("validates draft seeds without requiring deliberately unselected fields", () => {
    const node = compileSchema({
      type: "object",
      properties: {
        required_choice: { type: "string", enum: ["one", "two"] },
        count: { type: "integer", minimum: 1 },
      },
      required: ["required_choice", "count"],
      additionalProperties: false,
    });
    expect(acceptsDraftSeed(node, {})).toBe(true);
    expect(acceptsDraftSeed(node, { count: 2 })).toBe(true);
    expect(acceptsDraftSeed(node, { count: 0 })).toBe(false);
    expect(acceptsDraftSeed(node, { typo: true })).toBe(false);
  });
});
