// @vitest-environment jsdom

import { cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import catalog from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import { ParserDetailsForm } from "../src/features/variantDetails/VariantDetailsForms";
import { compileSchema, materializeBranch, type CompiledNode } from "../src/schema/compiler";
import { SchemaForm } from "../src/schema/SchemaForm";
import type { JsonObject, JsonValue, UiCatalog } from "../src/types";
import { render } from "./support/render";

afterEach(cleanup);
const connectors = (catalog as unknown as UiCatalog).connectors;

describe("endpoint Performance options", () => {
  for (const connector of connectors) for (const role of ["source", "sink"] as const) {
    const endpoint = connector[role];
    if (!endpoint) continue;
    it(`${connector.key} ${role} has a collapsed performance disclosure`, () => {
      const node = compileSchema(endpoint.schema, productionWidgetRegistry);
      const value = structuredClone(endpoint.initial) as JsonObject;
      // YTsaurus deliberately hides ALL settings until the required table mode
      // is chosen; exercise the editor after that explicit choice.
      if (node.kind === "object") for (const [name, child] of Object.entries(node.properties)) {
        if (child.kind === "union" && child.xUi.reveal_rest_on_selection && child.branches[0])
          value[name] = materializeBranch(child.branches[0]);
      }
      const onChange = vi.fn();
      const view = render(<SchemaForm endpoint node={node} value={value} onChange={onChange} />);
      const summaries = view.getAllByText("Performance options", { selector: "summary" });
      expect(summaries).toHaveLength(1);
      const summary = summaries[0]!;
      expect(summary.closest("details")!.open).toBe(false);
      fireEvent.click(summary, { detail: 1 });
      expect(summary.closest("details")!.open).toBe(true);
      expect(onChange).not.toHaveBeenCalled();
    });
  }

  it("hoists nested format tuning, preserving sibling values and independent disclosure state", async () => {
    const node: CompiledNode = {
      kind: "object", required: new Set(),
      xUi: { capabilities: { component: "destination", key: "test" } },
      properties: {
        endpoint: { kind: "string", title: "Endpoint", xUi: {} },
        advanced: { kind: "string", title: "Advanced value", xUi: { section: "advanced" } },
        format: { kind: "object", required: new Set(), xUi: {}, properties: {
          precision: { kind: "string", title: "Precision", xUi: {} },
          compression: { kind: "string", title: "Compression", xUi: { section: "performance" } },
        } },
      },
    };
    const onChange = vi.fn();
    const value = { endpoint: "original", advanced: "kept", format: { precision: "full", compression: "zstd" } };
    const view = render(<SchemaForm endpoint node={node} value={value} onChange={onChange} />);
    const performance = view.getByText("Performance options").closest("details")!;
    const advanced = view.getByText("Advanced settings").closest("details")!;
    await waitFor(() => expect(performance.querySelector('[data-field-name="compression"]')).toBeTruthy());
    expect(view.getAllByText("Performance options")).toHaveLength(1);
    fireEvent.click(performance.querySelector("summary")!);
    fireEvent.click(advanced.querySelector("summary")!);
    expect(performance.open && advanced.open).toBe(true);
    fireEvent.input(within(performance).getByLabelText(/Compression/), { target: { value: "lz4" } });
    expect(onChange).toHaveBeenCalledExactlyOnceWith({ ...value, format: { precision: "full", compression: "lz4" } });
    fireEvent.click(advanced.querySelector("summary")!);
    expect(performance.open).toBe(true);
    expect(advanced.open).toBe(false);
  });

  it("keeps S3 Parquet batch rows in source Performance, not the detached parser island", async () => {
    const endpoint = connectors.find(connector => connector.key === "s3")!.source!;
    const node = compileSchema(endpoint.schema, productionWidgetRegistry);
    function Fixture() {
      const [value, setValue] = useState<JsonValue>({ ...endpoint.initial, parser: { type: "parquet", batch_rows: 1024 } });
      return <>
        <SchemaForm endpoint node={node} value={value} variantUi={{ selectionOnly: ["parser"] }} onChange={setValue} />
        <ParserDetailsForm node={node} value={value} onChange={setValue} />
        <output>{JSON.stringify(value)}</output>
      </>;
    }
    const view = render(<Fixture />);
    const performance = view.getByText("Performance options", { selector: "summary" }).closest("details")!;
    await waitFor(() => expect(performance.querySelector('[data-field-name="batch_rows"]')).toBeTruthy());
    expect(view.container.querySelectorAll('[data-field-name="batch_rows"]')).toHaveLength(1);
    expect(view.container.querySelector(".parser-details-card")).toBeNull();
    expect(view.container.querySelector(".schema-object > .schema-object")).toBeNull();
    fireEvent.click(performance.querySelector("summary")!);
    fireEvent.input(within(performance).getByLabelText(/Batch rows/i), { target: { value: "2048" } });
    expect(JSON.parse(view.container.querySelector("output")!.textContent!).parser)
      .toEqual({ type: "parquet", batch_rows: 2048 });
  });

  it("does not leave an empty nested frame below the S3 Parquet format selector", async () => {
    const endpoint = connectors.find(connector => connector.key === "s3")!.sink!;
    const node = compileSchema(endpoint.schema, productionWidgetRegistry);
    const view = render(<SchemaForm endpoint node={node} value={endpoint.initial} onChange={() => undefined} />);
    const performance = view.getByText("Performance options", { selector: "summary" }).closest("details")!;
    await waitFor(() => expect(performance.querySelector('[data-field-name="compression"]')).toBeTruthy());
    expect(view.container.querySelector('[data-field-name="format"] .nested-section')).toBeNull();
    expect(view.container.querySelectorAll('[data-field-name="compression"]')).toHaveLength(1);
  });
});
