// @vitest-environment jsdom

import { cleanup, fireEvent } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";

import catalogFixture from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { decodeApi } from "../src/api/contractDecoder";
import {
  completionIssueLabel,
  configurationReadiness,
  selectedEndpoints,
  validateCatalogSchemas,
} from "../src/delivery/editorConfig";
import { orderedEndpointConnectors } from "../src/connectorCatalog";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import {
  branchMatches,
  compileSchema,
  createValue,
  firstCompletionIssue,
  isComplete,
  isFieldComplete,
  type CompiledNode,
} from "../src/schema/compiler";
import { SchemaForm } from "../src/schema/SchemaForm";
import { configuredEndpointCapabilities, sourceRecordSemantics, DELIVERY_TYPES } from "../src/recordSemantics";
import { compatibilityRoutes } from "../src/ui/CompatibilityMatrixDialog";
import type { JsonObject, JsonValue, UiCatalog } from "../src/types";
import {
  nextRequiredTarget,
  REQUIRED_CONTROL_SELECTOR,
} from "../src/ui/requiredGuidance";
import { render } from "./support/render";

afterEach(cleanup);

describe("connector catalog readiness", () => {
  const matrixCatalog = decodeApi("catalog_response", catalogFixture, "catalog");
  it.each(compatibilityRoutes(matrixCatalog).flatMap((route) => DELIVERY_TYPES.map((mode) => ({
    name: `${route.source.key} → ${route.sink.key} / ${mode}`, route, mode,
  }))))("agrees with the matrix for $name even before configuration is complete", ({ route, mode }) => {
    for (const initial of [true, false]) {
      const config = { delivery_type: mode,
        source: { [route.source.key]: initial ? route.source.source!.initial : {} },
        sink: { [route.sink.key]: initial ? route.sink.sink!.initial : {} },
      };
      const selection = selectedEndpoints(matrixCatalog, config, productionWidgetRegistry);
      expect(selection.routeError === undefined).toBe(route.supported.includes(mode));
    }
  });
  it("validates the selected delivery type as an exact source capability", () => {
    const catalog: UiCatalog = {
      common_schema: { type: "object" },
      initial: {},
      connectors: [
        {
          key: "legacy",
          title: "Legacy separate modes",
          source: {
            schema: {},
            initial: {},
            delivery_modes: ["batch", "stream"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
        {
          key: "combined",
          title: "Combined",
          source: {
            schema: {},
            initial: {},
            delivery_modes: ["batch_and_stream"],
            record_semantics: ["append_only", "changelog"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    };

    expect(
      selectedEndpoints(
        catalog,
        {
          delivery_type: "batch_and_stream",
          source: { legacy: {} },
        },
        productionWidgetRegistry,
      ).error,
    ).toBe(
      "Legacy separate modes does not support batch and stream delivery.",
    );
    expect(
      selectedEndpoints(
        catalog,
        {
          delivery_type: "batch_and_stream",
          source: { combined: {} },
        },
        productionWidgetRegistry,
      ).error,
    ).toBeUndefined();
  });

  it("uses active endpoint branches for readiness while retaining aggregate modes", () => {
    const catalog: UiCatalog = {
      common_schema: { type: "object" },
      initial: {},
      connectors: [
        {
          key: "database",
          title: "Database",
          source: {
            schema: conditionalEndpointSchema("source"),
            initial: { replication: null },
            delivery_modes: ["batch", "stream", "batch_and_stream"],
            record_semantics: ["append_only", "changelog"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
        {
          key: "destination",
          title: "Destination",
          sink: {
            schema: conditionalEndpointSchema("destination"),
            initial: { replication: null },
            delivery_modes: [],
            record_semantics: ["append_only", "changelog"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    };

    expect(() =>
      validateCatalogSchemas(catalog, productionWidgetRegistry),
    ).not.toThrow();

    expect(
      selectedEndpoints(
        catalog,
        {
          delivery_type: "batch",
          source: { database: { replication: null } },
          sink: { destination: { replication: null } },
        },
        productionWidgetRegistry,
      ).error,
    ).toBeUndefined();
    expect(
      selectedEndpoints(
        catalog,
        {
          delivery_type: "stream",
          source: { database: { replication: null } },
          sink: { destination: { replication: null } },
        },
        productionWidgetRegistry,
      ).error,
    ).toBe("Configure Database for stream delivery; the current source settings do not enable this mode.");
    expect(
      selectedEndpoints(
        catalog,
        {
          delivery_type: "stream",
          source: { database: { replication: {} } },
          sink: { destination: { replication: null } },
        },
        productionWidgetRegistry,
      ).error,
    ).toBe(
      "Destination cannot accept the records produced by Database for stream delivery.",
    );
    expect(
      selectedEndpoints(
        catalog,
        {
          delivery_type: "stream",
          source: { database: { replication: {} } },
          sink: { destination: { replication: {} } },
        },
        productionWidgetRegistry,
      ).error,
    ).toBeUndefined();
  });

  it("uses delivery type for PostgreSQL without a replication toggle or nested advanced options", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    const postgres = catalog.connectors.find((connector) => connector.key === "postgres")!.source!;
    const schema = compileSchema(postgres.schema, productionWidgetRegistry);
    expect(postgres.delivery_modes).toEqual(["batch", "stream", "batch_and_stream"]);
    for (const deliveryType of DELIVERY_TYPES) {
      const selection = selectedEndpoints(catalog, {
        delivery_type: deliveryType,
        source: { postgres: postgres.initial },
      }, productionWidgetRegistry);
      expect(selection.error).toBeUndefined();
      expect(sourceRecordSemantics(postgres, schema, postgres.initial, deliveryType))
        .toEqual(deliveryType === "batch" ? ["append_only"]
          : deliveryType === "stream" ? ["changelog"] : ["append_only", "changelog"]);
    }
    const view = render(<SchemaForm node={schema} value={postgres.initial} onChange={() => undefined} />);
    for (const toggle of view.queryAllByText("Advanced settings")) fireEvent.click(toggle);
    expect(view.queryByText("Replication")).toBeNull();
    expect(view.queryByText("Plugin")).toBeNull();
    expect(view.queryByText("Replication bootstrap timeout")).toBeNull();
    expect(view.queryByText("COPY TO format")).not.toBeNull();
  });

  it("derives MySQL delivery modes and semantics from replication configuration", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    const mysql = catalog.connectors.find(
      (connector) => connector.key === "mysql",
    )!.source!;
    const schema = compileSchema(mysql.schema, productionWidgetRegistry);
    const replication = {
      ...mysql.initial,
      replication: { server_id: 42 },
    };

    expect(mysql.delivery_modes).toEqual([
      "batch",
      "stream",
      "batch_and_stream",
    ]);
    expect(mysql.record_semantics).toEqual(["append_only", "changelog"]);
    expect(
      configuredEndpointCapabilities(
        mysql,
        schema,
        mysql.initial,
        "source",
      ),
    ).toEqual({
      delivery_modes: ["batch"],
      record_semantics: ["append_only"],
    });
    expect(
      configuredEndpointCapabilities(mysql, schema, replication, "source"),
    ).toEqual({
      delivery_modes: ["stream", "batch_and_stream"],
      record_semantics: ["changelog"],
    });
    expect(
      selectedEndpoints(
        catalog,
        {
          delivery_type: "stream",
          source: { mysql: mysql.initial },
        },
        productionWidgetRegistry,
      ).error,
    ).toBe("Configure MySQL for stream delivery; the current source settings do not enable this mode.");
    expect(
      selectedEndpoints(
        catalog,
        {
          delivery_type: "stream",
          source: { mysql: replication },
        },
        productionWidgetRegistry,
      ).error,
    ).toBeUndefined();
    expect(
      selectedEndpoints(
        catalog,
        {
          delivery_type: "batch",
          source: { mysql: replication },
        },
        productionWidgetRegistry,
      ).error,
    ).toBe("Configure MySQL for batch delivery; the current source settings do not enable this mode.");
  });

  it("allows parser-defined schema preview before source connectivity is configured", () => {
    const catalog: UiCatalog = {
      common_schema: { type: "object" },
      initial: {},
      connectors: [
        {
          key: "queue",
          title: "Queue",
          source: {
            schema: {
              type: "object",
              properties: {
                brokers: {
                  type: "array",
                  items: { type: "string" },
                  minItems: 1,
                },
                parser: {
                  type: "object",
                  properties: { table_name: { type: "string" } },
                  required: ["table_name"],
                },
              },
              required: ["brokers", "parser"],
            },
            initial: { brokers: [], parser: { table_name: "" } },
            delivery_modes: ["stream"],
            record_semantics: ["append_only"],
            partitioned: true,
            connection_check: true,
            message_preview: true,
          },
        },
      ],
    };

    const incomplete = configurationReadiness(
      catalog,
      {
        delivery_type: "stream",
        source: { queue: { brokers: [], parser: { table_name: "" } } },
      },
      productionWidgetRegistry,
    );
    expect(incomplete.sourceComplete).toBe(false);
    expect(incomplete.sourceSchemaReady).toBe(false);
    expect(incomplete.sourceSchemaIssue?.path).toBe(
      "#/source/queue/parser/table_name",
    );
    const sourceNode = compileSchema(
      catalog.connectors[0]!.source!.schema,
      productionWidgetRegistry,
    );
    expect(
      completionIssueLabel(
        sourceNode,
        { brokers: [], parser: { table_name: "" } },
        incomplete.sourceSchemaIssue!,
        "#/source/queue",
      ),
    ).toBe("Table name");

    const previewable = configurationReadiness(
      catalog,
      {
        delivery_type: "stream",
        source: {
          queue: { brokers: [], parser: { table_name: "events" } },
        },
      },
      productionWidgetRegistry,
    );
    expect(previewable.sourceComplete).toBe(false);
    expect(previewable.sourceSchemaReady).toBe(true);
  });

  it("orders regular endpoints alphabetically and keeps benchmark endpoints last", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");

    expect(
      orderedEndpointConnectors(catalog, "source").map(
        (connector) => connector.title,
      ),
    ).toEqual([
      "Apache Iceberg",
      "ClickHouse",
      "Kafka",
      "Logbroker",
      "MySQL",
      "OpenSearch",
      "PostgreSQL",
      "S3",
      "YDB",
      "YTsaurus",
      "Data generator (for benchmarks)",
    ]);
    expect(
      orderedEndpointConnectors(catalog, "sink").map(
        (connector) => connector.title,
      ),
    ).toEqual([
      "Apache Iceberg",
      "ClickHouse",
      "Kafka",
      "Logbroker",
      "MySQL",
      "OpenSearch",
      "PostgreSQL",
      "S3",
      "YDB",
      "YTsaurus",
      "Discard (for benchmarks)",
    ]);
  });

  it("accepts every current catalog schema and initial value at startup", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");

    expect(() =>
      validateCatalogSchemas(catalog, productionWidgetRegistry),
    ).not.toThrow();
  });

  it("rejects a catalog whose initial value selects a non-first union branch", () => {
    const catalog: UiCatalog = {
      common_schema: { type: "object" },
      initial: {},
      connectors: [
        {
          key: "source",
          title: "Source",
          source: {
            schema: {
              oneOf: [
                { type: "object", properties: { type: { const: "other" } } },
                {
                  type: "object",
                  properties: { type: { const: "default" } },
                },
              ],
            },
            initial: { type: "default" },
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    };

    expect(() =>
      validateCatalogSchemas(catalog, productionWidgetRegistry),
    ).toThrow("the default branch must be first");
  });

  it("gives every incomplete source and sink initial state an actionable target", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    let sourceCount = 0;
    let sinkCount = 0;

    for (const connector of catalog.connectors) {
      for (const [role, endpoint] of [
        ["source", connector.source],
        ["sink", connector.sink],
      ] as const) {
        if (endpoint === undefined) continue;
        if (role === "source") sourceCount += 1;
        else sinkCount += 1;
        const node = compileSchema(endpoint.schema, productionWidgetRegistry);
        const view = render(
          <SchemaForm
            node={node}
            value={endpoint.initial}
            showRequiredErrors
            onChange={() => undefined}
          />,
        );

        const issue = firstCompletionIssue(node, endpoint.initial);
        if (issue !== undefined && !issue.hidden) {
          const target = nextRequiredTarget(view.container as HTMLElement);
          expect(
            target,
            `${connector.key} ${role} is incomplete but has no feedback target`,
          ).toBeDefined();
          expect(
            target?.matches(REQUIRED_CONTROL_SELECTOR) ||
              target?.querySelector(REQUIRED_CONTROL_SELECTOR) !== null,
            `${connector.key} ${role} feedback target is not actionable`,
          ).toBe(true);
        }
        cleanup();
      }
    }

    expect(sourceCount).toBeGreaterThan(0);
    expect(sinkCount).toBeGreaterThan(0);
  });

  it("leaves no non-editable blocker undiscovered in any selectable endpoint variant", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    let variantCount = 0;

    for (const connector of catalog.connectors) {
      for (const [role, endpoint] of [
        ["source", connector.source],
        ["sink", connector.sink],
      ] as const) {
        if (endpoint === undefined) continue;
        const node = compileSchema(endpoint.schema, productionWidgetRegistry);
        for (const scenario of unionScenarios(node)) {
          const value = visibleWitness(
            node,
            endpoint.initial,
            true,
            scenario.forces,
          );
          const issue = firstCompletionIssue(node, value);
          expect(
            issue,
            `${connector.key} ${role} ${scenario.label} retains a non-editable blocker at ${issue?.path}`,
          ).toBeUndefined();
          variantCount += 1;
        }
      }
    }

    expect(variantCount).toBeGreaterThan(0);
  });

  it("accepts a complete structural witness for every source and destination pair", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    const common = compileSchema(
      catalog.common_schema,
      productionWidgetRegistry,
    );
    const commonValue = completeWitness(common, catalog.initial);
    if (!isObject(commonValue))
      throw new Error("common witness is not an object");
    const sources = catalog.connectors.flatMap((connector) =>
      connector.source === undefined
        ? []
        : [{ key: connector.key, endpoint: connector.source }],
    );
    const sinks = catalog.connectors.flatMap((connector) =>
      connector.sink === undefined
        ? []
        : [{ key: connector.key, endpoint: connector.sink }],
    );
    let pairCount = 0;

    for (const source of sources) {
      const sourceNode = compileSchema(
        source.endpoint.schema,
        productionWidgetRegistry,
      );
      const sourceValue = completeWitness(sourceNode, source.endpoint.initial);
      for (const sink of sinks) {
        const sinkNode = compileSchema(
          sink.endpoint.schema,
          productionWidgetRegistry,
        );
        const sinkValue = completeWitness(sinkNode, sink.endpoint.initial);
        const config: JsonObject = {
          ...structuredClone(commonValue),
          delivery_type: source.endpoint.delivery_modes[0] ?? null,
          source: { [source.key]: sourceValue },
          sink: { [sink.key]: sinkValue },
        };
        const readiness = configurationReadiness(
          catalog,
          config,
          productionWidgetRegistry,
        );

        expect(
          readiness.complete,
          `${source.key} -> ${sink.key} has no structurally complete configuration`,
        ).toBe(true);
        pairCount += 1;
      }
    }

    expect(pairCount).toBe(sources.length * sinks.length);
    expect(pairCount).toBeGreaterThan(0);
  });
});

function completeWitness(
  node: CompiledNode,
  seed: JsonValue | undefined,
  required = true,
): JsonValue {
  if (seed !== undefined && isFieldComplete(node, seed, required))
    return structuredClone(seed);
  switch (node.kind) {
    case "nullable":
      return seed === null || seed === undefined
        ? null
        : completeWitness(node.inner, seed, required);
    case "union": {
      const branch =
        node.branches.find((candidate) =>
          seed === undefined ? false : branchMatches(candidate, seed),
        ) ?? node.branches[0];
      if (branch === undefined) throw new Error("union has no branch");
      if (branch.constant !== undefined)
        return structuredClone(branch.constant);
      const value = completeWitness(branch.node, seed, required);
      return branch.discriminator !== undefined && isObject(value)
        ? {
            ...value,
            [branch.discriminator.key]: structuredClone(
              branch.discriminator.value,
            ),
          }
        : value;
    }
    case "object": {
      const source = isObject(seed) ? seed : {};
      const value: JsonObject = {};
      for (const [name, child] of Object.entries(node.properties)) {
        const childRequired = node.required.has(name);
        if (childRequired || source[name] !== undefined)
          value[name] = completeWitness(child, source[name], childRequired);
      }
      return value;
    }
    case "array": {
      const source = Array.isArray(seed) ? seed : [];
      const length = Math.max(source.length, node.minItems ?? 0);
      return Array.from({ length }, (_, index) =>
        completeWitness(node.item, source[index], true),
      );
    }
    case "boolean":
      return typeof seed === "boolean" ? seed : false;
    case "number":
      return typeof seed === "number" && isFieldComplete(node, seed, required)
        ? seed
        : (node.minimum ?? 0);
    case "string": {
      const option = node.enumValues?.find(
        (candidate) => typeof candidate === "string" && candidate !== "",
      );
      return option ?? "configured";
    }
  }
}

function unionScenarios(
  node: CompiledNode,
  path = "#",
  ancestors = new Map<CompiledNode, number>(),
): Array<{ label: string; forces: Map<CompiledNode, number> }> {
  const scenarios: Array<{
    label: string;
    forces: Map<CompiledNode, number>;
  }> = [];
  if (path === "#")
    scenarios.push({ label: "initial branches", forces: ancestors });
  if (node.hidden === true) return scenarios;
  switch (node.kind) {
    case "union":
      node.branches.forEach((branch, index) => {
        const forces = new Map(ancestors).set(node, index);
        scenarios.push({ label: `${path} branch ${index}`, forces });
        scenarios.push(
          ...unionScenarios(branch.node, path, forces).filter(
            (scenario) => scenario.label !== "initial branches",
          ),
        );
      });
      break;
    case "object":
      for (const [name, child] of Object.entries(node.properties))
        scenarios.push(
          ...unionScenarios(child, `${path}/${name}`, ancestors).filter(
            (scenario) => scenario.label !== "initial branches",
          ),
        );
      break;
    case "array":
      scenarios.push(
        ...unionScenarios(node.item, `${path}/items`, ancestors).filter(
          (scenario) => scenario.label !== "initial branches",
        ),
      );
      break;
    case "nullable":
      scenarios.push(
        ...unionScenarios(node.inner, path, ancestors).filter(
          (scenario) => scenario.label !== "initial branches",
        ),
      );
      break;
    default:
      break;
  }
  return scenarios;
}

function visibleWitness(
  node: CompiledNode,
  seed: JsonValue | undefined,
  required: boolean,
  forces: ReadonlyMap<CompiledNode, number>,
): JsonValue {
  if (node.hidden === true)
    return seed === undefined ? createValue(node) : structuredClone(seed);
  const forcedInSubtree = containsForcedUnion(node, forces);
  if (
    !forcedInSubtree &&
    seed !== undefined &&
    isFieldComplete(node, seed, required)
  )
    return structuredClone(seed);
  switch (node.kind) {
    case "nullable":
      return !containsForcedUnion(node.inner, forces) &&
        (seed === undefined || seed === null)
        ? null
        : visibleWitness(node.inner, seed, required, forces);
    case "union": {
      const forced = forces.get(node);
      const index =
        forced ??
        node.branches.findIndex((branch) =>
          seed === undefined ? false : branchMatches(branch, seed),
        );
      const branch = node.branches[index < 0 ? 0 : index];
      if (branch === undefined) throw new Error("union has no branch");
      if (branch.constant !== undefined)
        return structuredClone(branch.constant);
      const value = visibleWitness(branch.node, seed, required, forces);
      return branch.discriminator !== undefined && isObject(value)
        ? {
            ...value,
            [branch.discriminator.key]: structuredClone(
              branch.discriminator.value,
            ),
          }
        : value;
    }
    case "object": {
      const source = isObject(seed) ? seed : {};
      const value: JsonObject = {};
      for (const [name, child] of Object.entries(node.properties)) {
        const childRequired = node.required.has(name);
        if (
          childRequired ||
          source[name] !== undefined ||
          child.defaultValue !== undefined ||
          containsForcedUnion(child, forces)
        )
          value[name] = visibleWitness(
            child,
            source[name],
            childRequired,
            forces,
          );
      }
      return value;
    }
    case "array": {
      const source = Array.isArray(seed) ? seed : [];
      const length = Math.max(
        source.length,
        node.minItems ?? 0,
        containsForcedUnion(node.item, forces) ? 1 : 0,
      );
      return Array.from({ length }, (_, index) =>
        visibleWitness(node.item, source[index], true, forces),
      );
    }
    case "boolean":
      return typeof seed === "boolean" ? seed : false;
    case "number":
      return typeof seed === "number" && isFieldComplete(node, seed, required)
        ? seed
        : (node.minimum ?? 0);
    case "string": {
      const option = node.enumValues?.find(
        (candidate) => typeof candidate === "string" && candidate !== "",
      );
      return option ?? "configured";
    }
  }
}

function containsForcedUnion(
  node: CompiledNode,
  forces: ReadonlyMap<CompiledNode, number>,
): boolean {
  if (node.kind === "union")
    return (
      forces.has(node) ||
      node.branches.some((branch) => containsForcedUnion(branch.node, forces))
    );
  if (node.kind === "object")
    return Object.values(node.properties).some((child) =>
      containsForcedUnion(child, forces),
    );
  if (node.kind === "array") return containsForcedUnion(node.item, forces);
  if (node.kind === "nullable") return containsForcedUnion(node.inner, forces);
  return false;
}

function conditionalEndpointSchema(component: "source" | "destination") {
  const capability = (
    key: string,
    recordSemantics: ("append_only" | "changelog")[],
    deliveryModes: ("batch" | "stream" | "batch_and_stream")[],
  ) => ({
    component,
    key,
    ...(component === "source" ? { delivery_modes: deliveryModes } : {}),
    record_semantics: recordSemantics,
  });
  return {
    type: "object" as const,
    properties: {
      replication: {
        anyOf: [
          {
            type: "object" as const,
            properties: {},
            "x-ui": {
              capabilities: capability(
                "replication",
                ["changelog"],
                ["stream", "batch_and_stream"],
              ),
            },
          },
          { type: "null" as const },
        ],
      },
    },
    "x-ui": {
      capabilities: capability("snapshot", ["append_only"], ["batch"]),
    },
  };
}

function isObject(value: JsonValue | undefined): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
