// @vitest-environment jsdom

import { cleanup, fireEvent, within } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import catalogFixture from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { decodeApi } from "../src/api/contractDecoder";
import { EndpointCard } from "../src/delivery/EndpointCard";
import { DeliveryConfiguration } from "../src/delivery/DeliveryConfiguration";
import { selectedEndpoints } from "../src/delivery/editorConfig";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import { ParserDetailsForm } from "../src/features/variantDetails/VariantDetailsForms";
import { compileSchema, materializeBranch } from "../src/schema/compiler";
import { hasEditableContent } from "../src/schema/widgetRegistry";
import { render } from "./support/render";
import type { JsonObject } from "../src/types";

afterEach(cleanup);
const catalog = decodeApi("catalog_response", catalogFixture, "catalog");

it("changes the S3 parser from Tables while keeping JSON settings in a separate full-width island", () => {
  const initial = catalog.connectors.find(item => item.key === "s3")!.source!.initial;
  const onConfig = vi.fn();
  function S3Delivery() {
    const [config, setConfig] = useState<JsonObject>({ delivery_type: "batch",
      source: { s3: initial }, sink: { discard: {} } });
    return <DeliveryConfiguration catalog={catalog}
      editor={{ sessionId: "s3-tables", editing: true, localRevision: 0, name: "S3", description: "", config,
        validation: { state: "draft" }, runtime: { state: "stopped" } }}
      selection={selectedEndpoints(catalog, config, productionWidgetRegistry)}
      readOnly={false} requiredErrorScope="none" onName={() => {}} onDescription={() => {}}
      onConfig={next => { setConfig(next); onConfig(next); }} onChooseEndpoint={() => {}} />;
  }
  const view = render(<S3Delivery />);
  const tables = view.getByRole("region", { name: "Source tables" });
  const selector = within(tables).getByLabelText(/^Parser/);
  expect(selector.textContent).toContain("Parquet parser");
  const path = within(tables).getByLabelText(/^Path prefix/);
  const check = view.getByRole("button", { name: "Check connection", exact: true });
  fireEvent.click(selector);
  fireEvent.click(view.getByRole("option", { name: "JSON parser", exact: true }));
  const details = view.container.querySelector(".parser-details-card")!;
  expect(details).not.toBeNull();
  expect(details.parentElement).toBe(tables.parentElement);
  expect(details.previousElementSibling).toBe(tables);
  expect(details.querySelector(".island-form-wide.json-parser-form .column-editor")).not.toBeNull();
  expect(tables.querySelector(".column-editor")).toBeNull();
  expect(view.getByRole("region", { name: "Source tables" })).toBe(tables);
  expect(within(tables).getByLabelText(/^Path prefix/)).toBe(path);
  expect(within(tables).getByLabelText(/^Parser/)).toBe(selector);
  expect(selector.textContent).toContain("JSON parser");
  expect(view.getByRole("button", { name: "Check connection", exact: true })).toBe(check);
  const next = onConfig.mock.lastCall![0];
  expect(next.source.s3.parser.type).toBe("json");
  expect({ ...next.source.s3, parser: initial.parser }).toEqual(initial);
  expect(next.sink).toEqual({ discard: {} });
});

describe("compact island forms", () => {
  it.each(["s3", "logbroker", "kafka"])("uses the same authored JSON editor for %s without rewriting its config", key => {
    const endpoint = catalog.connectors.find(item => item.key === key)!.source!;
    const node = compileSchema(endpoint.schema, productionWidgetRegistry);
    if (node.kind !== "object" || node.properties.parser?.kind !== "union") throw new Error("Expected parser union");
    const branch = node.properties.parser.branches.find(branch => ["json_parser", "s3_json"].includes(branch.node.xUi.capabilities?.key ?? ""))!;
    const onChange = vi.fn();
    const value = { parser: materializeBranch(branch) };
    const view = render(<ParserDetailsForm node={node} value={value} onChange={onChange} />);
    expect(view.container.querySelectorAll(".json-parser-editor")).toHaveLength(1);
    expect(view.container.querySelectorAll(".column-editor")).toHaveLength(1);
    expect(view.container.querySelectorAll(".parser-secondary-section")).toHaveLength(1);
    expect(view.getAllByRole("button", { name: /Add system columns/ })).toHaveLength(1);
    expect(onChange).not.toHaveBeenCalled();
    const parser = value.parser as JsonObject;
    const settings = parser.json_parser as JsonObject;
    const next = settings.json_framing === "json_lines" ? "single_document" : "json_lines";
    fireEvent.click(view.getByLabelText(/^JSON framing/));
    fireEvent.click(view.getByRole("option", { name: next === "json_lines" ? "JSON Lines (JSONL)" : "Single JSON document" }));
    expect(onChange).toHaveBeenLastCalledWith({ parser: { ...parser, json_parser: { ...settings, json_framing: next } } });
  });
  it.each([
    ["postgres", "source"], ["mysql", "source"], ["clickhouse", "source"],
    ["mysql", "sink"], ["discard", "sink"],
  ] as const)("contains the whole %s %s form in one compact column", (key, role) => {
    const connector = catalog.connectors.find(item => item.key === key)!;
    const endpoint = connector[role]!;
    const onConfig = vi.fn();
    const view = render(<EndpointCard title={role} role={role} selectedKey={key}
      connectors={[connector]} endpoint={endpoint} config={{ [role]: { [key]: endpoint.initial } }}
      readOnly={false} showRequiredErrors={false} onChoose={() => {}} onConfig={onConfig} tablesHost={null} />);
    const column = view.container.querySelector(`.endpoint-card-${role} > .island-form`)!;
    expect(column).not.toBeNull();
    expect(column.contains(view.getByRole("heading", { name: role }))).toBe(true);
    expect(column.contains(view.getByRole("button", { name: connector.title, exact: true }))).toBe(true);
    expect(column.querySelector(".endpoint-fields")).not.toBeNull();
    expect(column.querySelector(".island-form")).toBeNull();
    expect(column.classList.contains("island-form-wide")).toBe(false);
    expect(onConfig).not.toHaveBeenCalled();
  });

  for (const key of ["logbroker", "kafka", "s3"]) {
    const endpoint = catalog.connectors.find(item => item.key === key)!.source!;
    const node = compileSchema(endpoint.schema, productionWidgetRegistry);
    if (node.kind !== "object" || node.properties.parser?.kind !== "union") throw new Error("Expected parser union");
    for (const branch of node.properties.parser.branches) {
      const capability = branch.node.xUi.capabilities;
      if (capability?.component !== "parser") continue;
      it(`keeps only JSON/TSKV full width for ${key}/${capability.key}, independent of its label`, () => {
        const onChange = vi.fn();
        const renamed = { ...node, properties: { ...node.properties, parser: {
          ...node.properties.parser!, kind: "union" as const,
          branches: [{ ...branch, label: "Renamed parser" }],
        } } };
        const view = render(<ParserDetailsForm node={renamed}
          value={{ parser: materializeBranch(branch) }} onChange={onChange} />);
        if (branch.constant !== undefined || !hasEditableContent(branch.node, productionWidgetRegistry)) {
          expect(view.container.querySelector(".parser-details-card")).toBeNull();
          expect(onChange).not.toHaveBeenCalled();
          return;
        }
        const column = view.container.querySelector(".parser-details-card.card > .island-form")!;
        expect(column).not.toBeNull();
        expect(column.classList.contains("parser-form")).toBe(true);
        expect(column.classList.contains("island-form-wide"))
          .toBe(["json_parser", "s3_json", "tskv"].includes(capability.key));
        const json = ["json_parser", "s3_json"].includes(capability.key);
        expect(column.classList.contains("json-parser-form")).toBe(json);
        if (json || capability.key === "tskv") {
          const compact = column.querySelectorAll([
            ".parser-scalar-section",
            ".schema-object-with-columns > .form-row",
            ".schema-object-with-columns > .parse-policy-row",
          ].join(","));
          expect(compact.length).toBeGreaterThanOrEqual(2);
          const schema = column.querySelector(".column-editor")!;
          expect(schema).not.toBeNull();
          expect(schema.textContent).toContain("Output columns");
          for (const section of compact) {
            expect(section.contains(schema)).toBe(false);
            expect(schema.contains(section)).toBe(false);
          }
        }
        expect(column.contains(view.getByRole("heading", { name: "Renamed parser settings" }))).toBe(true);
        expect(column.querySelector(".island-form")).toBeNull();
        expect(onChange).not.toHaveBeenCalled();
      });
    }
  }
});
