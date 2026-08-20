import { describe, expect, it } from "vitest";

import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";

import {
  acceptsDraftSeed,
  compileSchema,
  createValue,
  isComplete,
  SchemaContractError,
} from "../src/schema/compiler";
import { draftValue } from "../src/schema/draft";
import { validateCatalogSchemas } from "../src/delivery/editorConfig";

describe("schema compiler", () => {
  it("validates every provider schema before the catalog becomes interactive", () => {
    expect(() =>
      validateCatalogSchemas(
        {
          common_schema: { type: "object" },
          initial: {},
          providers: [
            {
              key: "broken",
              title: "Broken",
              source: {
                schema: {
                  type: "string",
                  "x-ui": { widget: "compact_array" },
                },
                initial: {},
                delivery_modes: [],
                partitioned: false,
                connection_check: false,
              },
            },
          ],
        },
        productionWidgetRegistry,
      ),
    ).toThrow(/compact_array.*does not support string/);
  });
  it("accepts safe external-console links and rejects unsafe templates", () => {
    const node = compileSchema({
      type: "string",
      "x-ui": {
        external_link_template: "https://console.example/items/{value}",
      },
    });
    expect(node.xUi.external_link_template).toBe(
      "https://console.example/items/{value}",
    );
    expect(() =>
      compileSchema({
        type: "string",
        "x-ui": { external_link_template: "javascript:{value}" },
      }),
    ).toThrow(/must be an HTTPS URL/);
  });

  it("accepts dependency-aware dynamic options emitted for managed MDB fields", () => {
    const node = compileSchema({
      type: "string",
      "x-ui": {
        dynamic_options: "yandex.mdb.clickhouse.databases",
        dynamic_options_dependencies: {
          cluster_id: "/installation/cluster_id",
        },
      },
    });

    expect(node.xUi).toEqual({
      dynamic_options: "yandex.mdb.clickhouse.databases",
      dynamic_options_dependencies: {
        cluster_id: "/installation/cluster_id",
      },
    });
  });

  it("compiles hierarchical dynamic options as an explicit path control", () => {
    const node = compileSchema({
      type: "string",
      "x-ui": {
        dynamic_options: "endpoint.paths",
        dynamic_options_control: "path",
      },
    });
    expect(node.xUi.dynamic_options_control).toBe("path");
    expect(() =>
      compileSchema({
        type: "string",
        "x-ui": {
          dynamic_options: "broken",
          dynamic_options_control: "tree",
        },
      }),
    ).toThrow(/dynamic_options_control must be path/);
  });

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

  it("does not treat an empty enum placeholder as a completed required value", () => {
    const node = compileSchema({
      type: "object",
      properties: {
        cluster: {
          type: "string",
          enum: ["", "logbroker", "logbroker-prestable"],
        },
      },
      required: ["cluster"],
    });

    expect(isComplete(node, { cluster: "" })).toBe(false);
    expect(isComplete(node, { cluster: "logbroker" })).toBe(true);
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

  it("rejects invalid optional properties when they are present", () => {
    const node = compileSchema({
      type: "object",
      properties: {
        name: { type: "string" },
        optional_port: { type: "integer", minimum: 1 },
      },
      required: ["name"],
    });

    expect(isComplete(node, { name: "delivery" })).toBe(true);
    expect(isComplete(node, { name: "delivery", optional_port: 9440 })).toBe(
      true,
    );
    expect(isComplete(node, { name: "delivery", optional_port: 0 })).toBe(
      false,
    );
    expect(isComplete(node, { name: "delivery", optional_port: "9440" })).toBe(
      false,
    );
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
      compileSchema({
        type: "array",
        items: { type: "string" },
        "x-ui": { initial_items: -1 },
      }),
    ).toThrow(/initial_items/);
    expect(() =>
      compileSchema({ type: "string", "x-ui": { surprise: true } }),
    ).toThrow(/unsupported x-ui hints/);
    expect(() =>
      compileSchema(
        { type: "string", "x-ui": { widget: "compact_array" } },
        productionWidgetRegistry,
      ),
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

  it("creates editor defaults only for absent values", () => {
    const node = compileSchema({ type: "integer", minimum: 1 });
    expect(draftValue(node, undefined)).toBeNull();
    expect(draftValue(node, null)).toBeNull();
    expect(draftValue(node, 0)).toBe(0);
  });
});
