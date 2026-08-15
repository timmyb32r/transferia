import { describe, expect, it } from "vitest";

import {
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
});
