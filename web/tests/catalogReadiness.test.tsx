// @vitest-environment jsdom

import { cleanup, fireEvent, within } from "@testing-library/preact";
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
import { compatibilityRoutes, catalogBatchStreamHandoffs, catalogParserSupport, CompatibilityMatrixDialog } from "../src/ui/CompatibilityMatrixDialog";
import type { JsonObject, JsonValue, UiCatalog } from "../src/types";
import {
  nextRequiredTarget,
  REQUIRED_CONTROL_SELECTOR,
} from "../src/ui/requiredGuidance";
import { render } from "./support/render";

afterEach(cleanup);

describe("connector catalog readiness", () => {
  it("shows source-derived S3 and MQ parser support in Entities and Properties", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    const support = catalogParserSupport(catalog);
    expect(support.get("Parquet parser")).toEqual({ s3: true, mq: [] });
    expect(support.get("JSON parser")?.s3).toBe(true);
    expect(support.get("JSON parser")?.mq).toContain("Kafka");
    expect(support.get("Schema Registry parser")?.s3).toBe(false);
    expect(support.get("Schema Registry parser")?.mq).toContain("Kafka");
    const view = render(<CompatibilityMatrixDialog catalog={catalog} onClose={() => {}} />);
    fireEvent.click(view.getByRole("tab", { name: "Entities" }));
    const check = () => {
      const table = within(view.getByRole("table", { name: "Parser source support" }));
      expect(table.getAllByRole("columnheader").map((cell) => cell.textContent)).toEqual(["Parser", "S3", "MQ"]);
      const parquet = within(table.getByRole("rowheader", { name: "Parquet parser" }).closest("tr")!);
      expect(parquet.getByRole("cell", { name: "S3: supported" }).textContent).toBe("✓");
      expect(parquet.getByRole("cell", { name: "MQ: not supported" }).textContent).toBe("×");
      const json = within(table.getByRole("rowheader", { name: "JSON parser", exact: true }).closest("tr")!);
      expect(json.getByRole("cell", { name: "S3: supported" }).textContent).toBe("✓");
      expect(json.getByRole("cell", { name: "MQ: supported" }).textContent).toBe("✓");
      const registry = within(table.getByRole("rowheader", { name: "Schema Registry parser" }).closest("tr")!);
      expect(registry.getByRole("cell", { name: "S3: not supported" }).textContent).toBe("×");
      expect(registry.getByRole("cell", { name: "MQ: supported" }).textContent).toBe("✓");
    };
    check();
    fireEvent.click(view.getByRole("tab", { name: "Properties" }));
    fireEvent.click(view.getByRole("button", { name: "All parsers" }));
    check();
    const withoutQueues = { ...catalog, connectors: catalog.connectors.filter((connector) => !["kafka", "logbroker"].includes(connector.key)) };
    expect(catalogParserSupport(withoutQueues).get("Schema Registry parser")).toBeUndefined();
  });
  it("shows source-owned snapshot handoffs in the existing combined-delivery property", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    const handoffs = catalogBatchStreamHandoffs(catalog);
    expect(handoffs.get("PostgreSQL")).toBe("Exactly-once switchover");
    expect(handoffs.get("MySQL")).toBe("Exactly-once switchover");
    expect(handoffs.get("YDB")).toBe("Overlapping");
    const view = render(<CompatibilityMatrixDialog catalog={catalog} onClose={() => {}} />);
    fireEvent.click(view.getByRole("tab", { name: "Properties" }));
    const tabs = view.getAllByRole("tab");
    const propertyButtons = within(view.getByRole("navigation", { name: "Properties" })).getAllByRole("button");
    const region = view.getByRole("region", { name: "Property membership" });
    fireEvent.click(view.getByRole("button", { name: "Batch + stream delivery" }));
    const sources = within(region).getByRole("list", { name: "Sources with property" });
    expect(within(sources).getByText("PostgreSQL").closest("li")?.textContent).toContain("Exactly-once switchover");
    expect(within(sources).getByText("MySQL").closest("li")?.textContent).toContain("Exactly-once switchover");
    expect(within(sources).getByText("YDB").closest("li")?.textContent).toContain("Overlapping");
    expect(within(region).getByText(/not end-to-end/)).toBeTruthy();
    fireEvent.click(view.getByRole("button", { name: "Batch delivery" }));
    expect(view.getByRole("region", { name: "Property membership" })).toBe(region);
    expect(view.getAllByRole("tab")).toEqual(tabs);
    expect(within(view.getByRole("navigation", { name: "Properties" })).getAllByRole("button")).toEqual(propertyButtons);
    expect(region.classList.contains("sources-only")).toBe(true);
    expect(within(region).queryByText("Overlapping")).toBeNull();
    const renamed = { ...catalog, connectors: catalog.connectors.map((connector) => ({ ...connector, key: `custom-${connector.key}`, title: `Custom ${connector.title}` })) };
    expect(catalogBatchStreamHandoffs(renamed).get("Custom YDB")).toBe("Overlapping");
    const unspecified = { ...catalog, connectors: catalog.connectors.map((connector) => ({
      ...connector, ...(connector.source ? { source: { ...connector.source, schema: {} } } : {}),
    })) };
    expect(catalogBatchStreamHandoffs(unspecified).get("YDB")).toBe("-");
  });

  it("rejects invalid handoff metadata in frontend schema compilation", () => {
    for (const [component, delivery_modes, batch_stream_handoff] of [
      ["destination", ["batch_and_stream"], "overlapping"],
      ["source", ["stream"], "exact_switchover"],
      ["source", ["batch_and_stream"], "guessed"],
    ] as Array<[string, string[], string]>) {
      expect(() => compileSchema({ type: "object", "x-ui": { capabilities: {
        component, key: "custom", delivery_modes, batch_stream_handoff, record_semantics: ["changelog"],
      } } }, productionWidgetRegistry)).toThrow();
    }
  });
  it("advertises YDB overlap batch-and-stream without a strategy selector", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    const endpoint = catalog.connectors.find((connector) => connector.key === "ydb")!.source!;
    expect(endpoint.delivery_modes).toContain("batch_and_stream");
    const schema = compileSchema(endpoint.schema, productionWidgetRegistry);
    const capabilities = configuredEndpointCapabilities(endpoint, schema, { ...endpoint.initial, replication: {} }, "source");
    expect(capabilities.delivery_modes).toContain("batch_and_stream");
    expect(JSON.stringify(endpoint.schema)).not.toContain("snapshot_strategy");
  });
  it.each(["kafka", "logbroker"])("starts %s sink with an unselected required serializer", (key) => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    const endpoint = catalog.connectors.find((connector) => connector.key === key)!.sink!;
    const schema = compileSchema(endpoint.schema, productionWidgetRegistry);
    expect(schema.kind).toBe("object");
    if (schema.kind !== "object") throw new Error("Expected sink object schema");
    const serializer = schema.properties.serializer!;
    const initial = (endpoint.initial as JsonObject).serializer!;
    expect(initial).toEqual({});
    expect(schema.required.has("serializer")).toBe(true);
    expect(isFieldComplete(serializer, initial, true)).toBe(false);
    const view = render(<SchemaForm node={serializer} value={initial} onChange={() => undefined} />);
    expect(view.getByRole("button", { name: "Not selected" })).toBeTruthy();
    expect(view.queryByRole("button", { name: "JSON" })).toBeNull();
  });
  const matrixCatalog = decodeApi("catalog_response", catalogFixture, "catalog");
  it("checks destination modes before any source is selected, using only declared properties", () => {
    const catalog: UiCatalog = { ...matrixCatalog, connectors: [{
      key: "arbitrary-store", title: "Archive", sink: {
        ...matrixCatalog.connectors.find((connector) => connector.key === "ytsaurus")!.sink!,
        schema: { type: "object", properties: { storage: {
          type: "object", title: "Snapshot storage", properties: {},
          "x-ui": { capabilities: { component: "destination", key: "arbitrary-mode",
            delivery_modes: ["batch"], record_semantics: ["append_only"],
          } },
        } } },
      },
    }] };
    for (const mode of DELIVERY_TYPES) {
      const selection = selectedEndpoints(catalog, {
        delivery_type: mode, sink: { "arbitrary-store": { storage: {} } },
      }, productionWidgetRegistry);
      expect(selection.routeError).toBeUndefined();
      expect(selection.error).toBe(mode === "batch" ? undefined
        : "Archive snapshot storage can be used only in 'batch' delivery mode.");
    }
  });
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
      "Legacy separate modes does not support 'batch_and_stream' delivery.",
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
    ).toBe("Configure Database for 'stream' delivery; the current source settings do not enable this mode.");
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
      "Destination cannot preserve the records produced by Database for 'stream' delivery. Append-only components cannot preserve updates/deletes; choose components with changelog support.",
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
    const advancedCount = view.queryAllByText("Advanced settings").length;
    for (const deliveryType of ["stream", "batch_and_stream", "batch", "stream"]) {
      view.rerender(<SchemaForm node={schema} value={postgres.initial} deliveryType={deliveryType} onChange={() => undefined} />);
      expect(view.queryAllByText("Advanced settings")).toHaveLength(advancedCount);
      expect(view.queryByText("Replication")).toBeNull();
      expect(view.queryByText("Replication bootstrap timeout")).toBeNull();
      expect(view.queryByText("Plugin") !== null).toBe(deliveryType !== "batch");
    }
  });

  it("rejects lossy queue serializers immediately for database change streams", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    for (const sinkKey of ["kafka", "logbroker"]) {
      for (const deliveryType of DELIVERY_TYPES) {
        for (const serializer of ["json", "schema_registry", "debezium"]) {
          const selection = selectedEndpoints(catalog, {
            delivery_type: deliveryType,
            source: { postgres: {} },
            sink: { [sinkKey]: { serializer: { type: serializer } } },
          }, productionWidgetRegistry);
          expect(selection.routeError).toBeUndefined();
          expect(selection.incompatibleConfiguration === true)
            .toBe(deliveryType !== "batch" && serializer !== "debezium");
          if (selection.incompatibleConfiguration) {
            expect(selection.error?.startsWith(`${serializer === "json" ? "JSON" : "Schema Registry"} serializer cannot preserve`)).toBe(true);
            expect(selection.error).toContain(`for '${deliveryType}' delivery`);
            expect(selection.error).toContain("updates/deletes");
          }
          else expect(selection.error).toBeUndefined();
        }
      }
    }
  });

  it("rejects parsed changes for static tables and recovers when either selection becomes compatible", () => {
    const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
    for (const sourceKey of ["kafka", "logbroker"]) {
      for (const parser of ["debezium", "json_parser"]) {
        for (const tableType of ["static_tables", "dynamic_tables"]) {
          const selection = selectedEndpoints(catalog, {
            delivery_type: "stream",
            source: { [sourceKey]: { parser: { common: {}, [parser]: {} } } },
            sink: { ytsaurus: { tables: { type: tableType } } },
          }, productionWidgetRegistry);
          expect(selection.routeError).toBeUndefined();
          expect(selection.incompatibleConfiguration === true)
            .toBe(tableType === "static_tables");
          if (tableType === "static_tables")
            expect(selection.error).toBe("YTsaurus static tables can be used only in 'batch' delivery mode.");
        }
      }
    }
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
    ).toBe("Configure MySQL for 'stream' delivery; the current source settings do not enable this mode.");
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
    ).toBe("Configure MySQL for 'batch' delivery; the current source settings do not enable this mode.");
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
