import { describe, expect, it } from "vitest";

import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";

import {
  acceptsDraftSeed,
  compileSchema,
  createValue,
  firstCompletionIssue,
  isComplete,
  SchemaContractError,
} from "../src/schema/compiler";
import { draftValue } from "../src/schema/draft";
import { validateCatalogSchemas } from "../src/delivery/editorConfig";
import type { JsonSchema, JsonValue } from "../src/types";

describe("schema compiler", () => {
  it("validates every connector schema before the catalog becomes interactive", () => {
    expect(() =>
      validateCatalogSchemas(
        {
          common_schema: { type: "object" },
          initial: {},
          connectors: [
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
                message_preview: false,
              },
            },
          ],
        },
        productionWidgetRegistry,
      ),
    ).toThrow(/compact_array.*does not support string/);
  });

  it("rejects an invalid connector initial value before the catalog becomes interactive", () => {
    expect(() =>
      validateCatalogSchemas(
        {
          common_schema: { type: "object" },
          initial: {},
          connectors: [
            {
              key: "broken",
              title: "Broken",
              source: {
                schema: {
                  type: "object",
                  properties: {
                    name: { type: "string" },
                    optional_port: { type: "integer", minimum: 1 },
                  },
                  required: ["name"],
                },
                initial: { name: "", optional_port: 0 },
                delivery_modes: ["batch"],
                partitioned: false,
                connection_check: false,
                message_preview: false,
              },
            },
          ],
        },
        productionWidgetRegistry,
      ),
    ).toThrow(/broken source initial.*optional_port/);
  });

  it("requires every hidden required scalar to materialize a valid value", () => {
    expect(() =>
      validateCatalogSchemas(
        {
          common_schema: { type: "object" },
          initial: {},
          connectors: [
            {
              key: "broken",
              title: "Broken",
              source: {
                schema: {
                  type: "object",
                  properties: {
                    hidden_region: {
                      type: "string",
                      "x-ui": { widget: "hidden" },
                    },
                  },
                  required: ["hidden_region"],
                },
                initial: { hidden_region: "us-east-1" },
                delivery_modes: ["batch"],
                partitioned: false,
                connection_check: false,
                message_preview: false,
              },
            },
          ],
        },
        productionWidgetRegistry,
      ),
    ).toThrow(
      /broken source.*hidden required field.*hidden_region.*deterministically/,
    );
  });

  it("rejects an incomplete hidden value supplied by an endpoint initial", () => {
    expect(() =>
      validateCatalogSchemas(
        {
          common_schema: { type: "object" },
          initial: {},
          connectors: [
            {
              key: "broken",
              title: "Broken",
              source: {
                schema: {
                  type: "object",
                  properties: {
                    hidden_region: {
                      type: "string",
                      default: "us-east-1",
                      "x-ui": { widget: "hidden" },
                    },
                  },
                  required: ["hidden_region"],
                },
                initial: { hidden_region: "" },
                delivery_modes: ["batch"],
                partitioned: false,
                connection_check: false,
                message_preview: false,
              },
            },
          ],
        },
        productionWidgetRegistry,
      ),
    ).toThrow(/broken source initial hidden field.*hidden_region.*incomplete/);
  });

  it("applies hidden-field materialization rules to the common schema", () => {
    expect(() =>
      validateCatalogSchemas(
        {
          common_schema: {
            type: "object",
            properties: {
              hidden_mode: {
                type: "string",
                "x-ui": { widget: "hidden" },
              },
            },
            required: ["hidden_mode"],
          },
          initial: { hidden_mode: "configured" },
          connectors: [],
        },
        productionWidgetRegistry,
      ),
    ).toThrow(
      /common.*hidden required field.*hidden_mode.*deterministically/,
    );
  });

  it("rejects every non-deterministic hidden composite and accepts a complete marker", () => {
    const catalogWith = (hidden: JsonSchema, initial: JsonValue) => ({
      common_schema: { type: "object" as const },
      initial: {},
      connectors: [
        {
          key: "composite",
          title: "Composite",
          source: {
            schema: {
              type: "object" as const,
              properties: {
                hidden: {
                  ...hidden,
                  "x-ui": { widget: "hidden" },
                },
              },
              required: ["hidden"],
            },
            initial: { hidden: initial },
            delivery_modes: ["batch" as const],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    });

    const nestedRequired: JsonSchema = {
      type: "object",
      properties: { value: { type: "string" } },
      required: ["value"],
    };
    const requiredArray: JsonSchema = {
      type: "array",
      minItems: 1,
      items: { type: "string", enum: ["fixed"] },
    };
    const ambiguousUnion: JsonSchema = {
      oneOf: [
        { type: "string", const: "left" },
        { type: "string", const: "right" },
      ],
    };
    const singleBranchWithGuessedBoolean: JsonSchema = {
      oneOf: [
        {
          type: "object",
          properties: { enabled: { type: "boolean" } },
          required: ["enabled"],
        },
      ],
    };
    const nullable: JsonSchema = {
      oneOf: [{ type: "null" }, { type: "string", const: "configured" }],
    };

    const invalidCases: Array<[JsonSchema, JsonValue]> = [
      [nestedRequired, { value: "configured" }],
      [requiredArray, ["fixed"]],
      [ambiguousUnion, "left"],
      [singleBranchWithGuessedBoolean, { enabled: true }],
      [nullable, null],
    ];
    for (const [schema, initial] of invalidCases)
      expect(() =>
        validateCatalogSchemas(
          catalogWith(schema, initial),
          productionWidgetRegistry,
        ),
      ).toThrow(/hidden required field.*cannot be materialized deterministically/);

    expect(() =>
      validateCatalogSchemas(
        catalogWith({ type: "object", additionalProperties: false }, {}),
        productionWidgetRegistry,
      ),
    ).not.toThrow();
  });

  it("materializes a singleton enum used as a hidden fixed field", () => {
    const schema: JsonSchema = {
      type: "object",
      properties: {
        host_selection: {
          type: "string",
          enum: ["first_alive_replica"],
        },
      },
      required: ["host_selection"],
      additionalProperties: false,
    };
    const node = compileSchema(schema);

    expect(createValue(node)).toEqual({
      host_selection: "first_alive_replica",
    });
    expect(
      createValue(
        compileSchema({
          type: "object",
          properties: {
            optional_mode: { type: "string", enum: ["fixed"] },
          },
        }),
      ),
    ).toEqual({});
    expect(() =>
      validateCatalogSchemas(
        {
          common_schema: { type: "object" },
          initial: {},
          connectors: [
            {
              key: "postgres",
              title: "PostgreSQL",
              source: {
                schema,
                initial: { host_selection: "first_alive_replica" },
                delivery_modes: ["batch"],
                partitioned: false,
                connection_check: false,
                message_preview: false,
              },
            },
          ],
        },
        productionWidgetRegistry,
      ),
    ).not.toThrow();
  });

  it("validates hidden fixed fields in every unselected union branch", () => {
    const deterministicBranch: JsonSchema = {
      type: "object",
      properties: {
        type: { type: "string", const: "managed" },
        host_selection: {
          type: "string",
          enum: ["first_alive_replica"],
        },
      },
      required: ["type", "host_selection"],
      additionalProperties: false,
    };
    const brokenBranch: JsonSchema = {
      type: "object",
      properties: {
        type: { type: "string", const: "on_premise" },
        hidden_region: {
          type: "string",
          "x-ui": { widget: "hidden" },
        },
      },
      required: ["type", "hidden_region"],
      additionalProperties: false,
    };
    const catalogWith = (alternative: JsonSchema) => {
      const endpoint = {
        schema: { oneOf: [deterministicBranch, alternative] },
        initial: {
          type: "managed",
          host_selection: "first_alive_replica",
        },
        delivery_modes: ["batch" as const],
        partitioned: false,
        connection_check: false,
        message_preview: false,
      };
      return {
        common_schema: {
          type: "object" as const,
          properties: {
            common_mode: {
              type: "string" as const,
              enum: ["local"],
            },
          },
          required: ["common_mode"],
        },
        initial: { common_mode: "local" },
        connectors: [
          {
            key: "postgres",
            title: "PostgreSQL",
            source: endpoint,
            sink: endpoint,
          },
        ],
      };
    };

    expect(() =>
      validateCatalogSchemas(
        catalogWith(brokenBranch),
        productionWidgetRegistry,
      ),
    ).toThrow(
      /postgres source.*branch-1.*hidden_region.*deterministically/,
    );

    const repairedBranch = structuredClone(brokenBranch);
    repairedBranch.properties!.hidden_region!.default = "us-east-1";
    expect(() =>
      validateCatalogSchemas(
        catalogWith(repairedBranch),
        productionWidgetRegistry,
      ),
    ).not.toThrow();
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

    const dependent = compileSchema({
      type: "string",
      "x-ui": {
        external_link_template:
          "https://console.example/{cluster}/navigation?path=//{value}",
        external_link_dependencies: {
          cluster: "/installation/cluster",
        },
      },
    });
    expect(dependent.xUi.external_link_dependencies).toEqual({
      cluster: "/installation/cluster",
    });
    expect(() =>
      compileSchema({
        type: "string",
        "x-ui": {
          external_link_template:
            "https://console.example/{undeclared}/items/{value}",
          external_link_dependencies: {
            cluster: "/installation/cluster",
          },
        },
      }),
    ).toThrow(/declared placeholder/);
  });

  it("accepts dependency-aware dynamic options emitted by extensions", () => {
    const node = compileSchema({
      type: "string",
      "x-ui": {
        dynamic_options: "vendor.database.names",
        dynamic_options_dependencies: {
          cluster_id: "/installation/cluster_id",
        },
      },
    });

    expect(node.xUi).toEqual({
      dynamic_options: "vendor.database.names",
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
        dynamic_options_path_syntax: "double_slash_absolute",
        dynamic_options_entity: "table",
      },
    });
    expect(node.xUi.dynamic_options_control).toBe("path");
    expect(node.xUi.dynamic_options_path_syntax).toBe("double_slash_absolute");
    expect(node.xUi.dynamic_options_entity).toBe("table");
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

  it("reports the exact hidden path for an uneditable nested blocker", () => {
    const node = compileSchema(
      {
        type: "object",
        properties: {
          connection: { type: "string" },
          projection: {
            type: "object",
            "x-ui": { widget: "hidden" },
            properties: {
              columns: {
                type: "array",
                minItems: 1,
                items: { type: "string" },
              },
            },
            required: ["columns"],
          },
        },
        required: ["connection", "projection"],
      },
      productionWidgetRegistry,
    );

    expect(
      firstCompletionIssue(node, {
        connection: "https://registry.example",
        projection: { columns: [] },
      }),
    ).toEqual({
      path: "#/projection/columns",
      code: "min_items",
      hidden: true,
    });
    expect(isComplete(node, {
      connection: "https://registry.example",
      projection: { columns: [] },
    })).toBe(false);
  });

  it("enforces minItems when deciding whether an array is complete", () => {
    const node = compileSchema({
      type: "array",
      minItems: 1,
      items: { type: "string" },
    });

    expect(isComplete(node, [])).toBe(false);
    expect(isComplete(node, [""])).toBe(false);
    expect(isComplete(node, ["column"])).toBe(true);
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

  it("does not treat an empty optional string as a missing required field", () => {
    const node = compileSchema({
      type: "object",
      properties: {
        database: { type: "string" },
        shard_group: { type: "string" },
      },
      required: ["database"],
    });

    expect(isComplete(node, { database: "db1", shard_group: "" })).toBe(true);
    expect(isComplete(node, { database: "", shard_group: "analytics" })).toBe(
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
