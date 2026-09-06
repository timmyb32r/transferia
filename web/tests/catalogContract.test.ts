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
  it("uses the same stable Hide system tables control above Tables in every database source", () => {
    let expectedClasses: string[] | undefined;
    for (const key of ["postgres", "mysql", "clickhouse"]) {
      const source = catalogFixture.connectors.find(connector => connector.key === key)!.source!;
      const compiled = compileSchema(source.schema, productionWidgetRegistry);
      if (compiled.kind !== "object") throw new Error(`${key}: expected source object`);
      const hide = compiled.properties.hide_system_tables!;
      expect(hide.kind, key).toBe("boolean");
      expect(hide.title, key).toBe("Hide system tables");
      expect(hide.xUi.order, key).toBe(1);
      expect(compiled.properties.tables!.xUi.order, key).toBe(2);
      expect(source.initial.hide_system_tables, key).toBe(true);
      // Use real emitted fields and the shared renderer, including the
      // password's connection-action anchor. No connector-local checkbox UI.
      const node = { ...compiled, properties: Object.fromEntries(Object.entries(compiled.properties)
        .filter(([name]) => ["password", "hide_system_tables", "tables"].includes(name))) };
      const form = (hide_system_tables: boolean) => h(SchemaForm, {
        node, value: { ...source.initial, hide_system_tables }, onChange: () => undefined,
        connectionAction: h("button", { type: "button" }, "Check connection"),
      });
      const view = render(form(true));
      const field = view.container.querySelector('[data-field-name="hide_system_tables"]')!;
      const checkbox = field.querySelector('input[type="checkbox"]')! as HTMLInputElement;
      const tables = view.container.querySelector('[data-field-name="tables"]')!;
      const check = view.getByRole("button", { name: "Check connection" });
      expect(checkbox.checked, key).toBe(true);
      expect(check.compareDocumentPosition(field) & Node.DOCUMENT_POSITION_FOLLOWING, key).toBeTruthy();
      expect(field.compareDocumentPosition(tables) & Node.DOCUMENT_POSITION_FOLLOWING, key).toBeTruthy();
      const classes = [field.className, checkbox.parentElement!.className, checkbox.className];
      expectedClasses ??= classes;
      expect(classes, key).toEqual(expectedClasses);
      const siblings = Array.from(field.parentElement!.children);
      checkbox.focus();
      view.rerender(form(false));
      expect(checkbox.checked, key).toBe(false);
      expect(document.activeElement, key).toBe(checkbox);
      // Toggling only updates the checked state: no replacement or insertion
      // of surrounding form controls, banners or reconnect prompts.
      expect(Array.from(field.parentElement!.children), key).toEqual(siblings);
      expect(view.getByRole("button", { name: "Check connection" }), key).toBe(check);
      expect(view.container.querySelector('[data-field-name="tables"]'), key).toBe(tables);
      view.unmount();
    }
  });

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
