// @vitest-environment jsdom

import { cleanup } from "@testing-library/preact";
import { h } from "preact";
import { afterEach, describe, expect, it } from "vitest";

import catalogFixture from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { decodeApi } from "../src/api/contractDecoder";
import { validateCatalogSchemas } from "../src/delivery/editorConfig";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import {
  acceptsDraftSeed,
  compileSchema,
  draftSeedError,
  type CompiledNode,
} from "../src/schema/compiler";
import { SchemaForm } from "../src/schema/SchemaForm";
import { render } from "./support/render";

afterEach(cleanup);

function* descendants(node: CompiledNode): Generator<CompiledNode> {
  yield node;
  if (node.kind === "object") {
    for (const child of Object.values(node.properties)) yield* descendants(child);
    if (typeof node.additionalProperties === "object") yield* descendants(node.additionalProperties);
  } else if (node.kind === "array") yield* descendants(node.item);
  else if (node.kind === "nullable") yield* descendants(node.inner);
  else if (node.kind === "union") {
    for (const branch of node.branches) yield* descendants(branch.node);
  }
}

describe("Rust catalog contract", () => {
  it("registers the table selection widget for the emitted tagged union in every database source", () => {
    for (const key of ["postgres", "mysql", "clickhouse"]) {
      const source = catalogFixture.connectors.find(connector => connector.key === key)?.source;
      expect(source, key).toBeDefined();
      const node = compileSchema(source!.schema, productionWidgetRegistry);
      if (node.kind !== "object") throw new Error(`${key}: expected endpoint object`);
      const tables = node.properties.tables;
      expect(tables?.kind, key).toBe("union");
      expect(tables?.xUi.widget, key).toBe("table_selection");
      expect(acceptsDraftSeed(node, source!.initial), key).toBe(true);
    }
  });
  it("compiles every schema emitted by the Rust catalog", () => {
    const runtime = (
      globalThis as typeof globalThis & {
        process?: {
          env?: Record<string, string | undefined>;
          getBuiltinModule?: (name: "fs") => {
            readFileSync: (path: string, encoding: "utf8") => string;
          };
        };
      }
    ).process;
    const environment = runtime?.env;
    const injectedCatalog = environment?.TRANSFERIA_CATALOG_CONTRACT;
    const injectedCatalogFile = environment?.TRANSFERIA_CATALOG_CONTRACT_FILE;
    const catalog = decodeApi(
      "catalog_response",
      injectedCatalogFile !== undefined
        ? JSON.parse(
            runtime?.getBuiltinModule?.("fs").readFileSync(
              injectedCatalogFile,
              "utf8",
            ) ?? "null",
          )
        : injectedCatalog === undefined
          ? catalogFixture
          : JSON.parse(injectedCatalog),
      "catalog",
    );

    expect(() =>
      validateCatalogSchemas(catalog, productionWidgetRegistry),
    ).not.toThrow();

    const common = compileSchema(
      catalog.common_schema,
      productionWidgetRegistry,
    );
    if (common.kind !== "object")
      throw new Error("common schema must be an object");
    const commonInitial = Object.fromEntries(
      Object.entries(catalog.initial).filter(
        ([name]) => common.properties[name] !== undefined,
      ),
    );
    expect(acceptsDraftSeed(common, commonInitial)).toBe(true);
    let endpointCount = 0;
    for (const connector of catalog.connectors) {
      for (const endpoint of [connector.source, connector.sink]) {
        if (endpoint === undefined) continue;
        endpointCount += 1;
        const compiled = compileSchema(
          endpoint.schema,
          productionWidgetRegistry,
        );
        // Exercise the real form, not just the schema compiler or a widget mock.
        const view = render(h(SchemaForm, {
          node: compiled,
          value: endpoint.initial,
          disabled: true,
          onChange: () => undefined,
        }));
        view.unmount();
        expect(
          acceptsDraftSeed(compiled, endpoint.initial),
          `${connector.key} initial value must be a valid partial schema value: ${draftSeedError(compiled, endpoint.initial)}`,
        ).toBe(true);
      }
    }
    expect(endpointCount).toBeGreaterThan(0);
  });

  const endpoints = catalogFixture.connectors.flatMap(connector =>
    (["source", "sink"] as const).flatMap(role => {
      const endpoint = connector[role];
      return endpoint ? [{ name: `${connector.key}.${role}`, schema: endpoint.schema }] : [];
    }),
  );
  for (const { name, schema } of endpoints) {
    it(`${name}: rejects incompatible widgets even in inactive nested branches`, () => {
      const compiled = compileSchema(schema, productionWidgetRegistry);
      const exercised = new Set<string>();
      for (const node of descendants(compiled)) {
        const widget = node.xUi.widget;
        if (!widget) continue;
        const key = widget;
        if (exercised.has(key)) continue;
        exercised.add(key);
        const definition = productionWidgetRegistry.definition(widget)!;
        // Mutation equivalent to the regression: schema evolves, but the
        // registered widget still accepts only its previous node kinds.
        expect(() => compileSchema(schema, {
          definition: candidate => candidate === widget
            ? { ...definition, kinds: [] }
            : productionWidgetRegistry.definition(candidate),
        }), key).toThrow(`x-ui widget ${JSON.stringify(widget)} does not support`);
      }
    });
  }
});
