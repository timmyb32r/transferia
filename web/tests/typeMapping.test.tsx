// @vitest-environment jsdom
import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";
import { AboutProvider, CompatibilityMatrixLauncher } from "../src/ui/CompatibilityMatrixDialog";
import type { EndpointDefinition, UiCatalog } from "../src/generated/apiContract";

afterEach(cleanup);
const endpoint: EndpointDefinition = {
  schema: {}, initial: {}, delivery_modes: ["batch"], record_semantics: ["append_only"],
  partitioned: false, connection_check: true, message_preview: false, table_preview: false,
  type_mapping: { context: "Runtime test resolver", rows: [
    { input: "native", output: "Int32", error: null },
    { input: "unsupported", output: null, error: "Exact rejection reason" },
  ] },
};
const catalog: UiCatalog = { common_schema: {}, initial: {}, connectors: [
  { key: "postgres", title: "PostgreSQL", source: endpoint, sink: endpoint },
  { key: "clickhouse", title: "ClickHouse", source: endpoint, sink: endpoint },
  ...["kafka", "logbroker", "s3"].map((key) => ({ key, title: key, source: endpoint, sink: endpoint })),
  { key: "discard", title: "Discard", sink: endpoint },
  { key: "data_generator", title: "Data generator", source: endpoint },
] };

describe("About type mappings", () => {
  it.each(["source", "sink"] as const)("opens %s from About, searches errors and restores focus", (role) => {
    const view = render(<AboutProvider catalog={catalog}><CompatibilityMatrixLauncher /></AboutProvider>);
    const link = view.getByRole("button", { name: "About" });
    link.focus();
    fireEvent.click(link);
    expect(view.getByRole("dialog", { name: "About" })).toBeTruthy();
    fireEvent.click(view.getByRole("tab", { name: role === "source" ? "Source types" : "Destination types" }));
    fireEvent.click(view.getByRole("button", { name: "PostgreSQL" }));
    expect(view.getByRole("tab", { name: role === "source" ? "Source types" : "Destination types" }).getAttribute("aria-selected")).toBe("true");
    expect(view.getByRole("button", { name: "PostgreSQL" }).getAttribute("aria-pressed")).toBe("true");
    const search = view.getByRole("searchbox", { name: "Find type" });
    fireEvent.input(search, { target: { value: "rejection" } });
    expect(view.getByText("Exact rejection reason")).toBeTruthy();
    expect(view.queryByText("native")).toBeNull();
    expect(view.getByRole("searchbox", { name: "Find type" })).toBe(search);
    fireEvent.click(view.getByRole("button", { name: "ClickHouse" }));
    expect(view.getByText("native")).toBeTruthy();
    fireEvent.click(view.getByRole("button", { name: "Close About" }));
    expect(document.activeElement).toBe(link);
  });

  it("shows the mode footer only in Matrix and keeps mapping caveats out of flow", () => {
    const view = render(<AboutProvider catalog={catalog}><CompatibilityMatrixLauncher /></AboutProvider>);
    fireEvent.click(view.getByRole("button", { name: "About" }));
    expect(view.getByText(/Some connectors require a matching mode/)).toBeTruthy();
    for (const tab of ["Entities", "Properties", "Source types", "Destination types"]) {
      fireEvent.click(view.getByRole("tab", { name: tab }));
      expect(view.queryByText(/Some connectors require a matching mode/)).toBeNull();
    }
    fireEvent.click(view.getByRole("button", { name: "PostgreSQL" }));
    expect(view.getByRole("tooltip").classList.contains("visually-hidden")).toBe(true);
    expect(view.getByRole("dialog").querySelector(".type-mapping-context")).toBeNull();
  });

  it.each(["Source types", "Destination types"])("uses explanations instead of tables for transport endpoints in %s", (tab) => {
    const view = render(<AboutProvider catalog={catalog}><CompatibilityMatrixLauncher /></AboutProvider>);
    fireEvent.click(view.getByRole("button", { name: "About" }));
    fireEvent.click(view.getByRole("tab", { name: tab }));
    for (const connector of ["kafka", "logbroker", "s3", tab === "Source types" ? "Data generator" : "Discard"]) {
      fireEvent.click(view.getByRole("button", { name: connector }));
      expect(view.queryByRole("table")).toBeNull();
      expect(view.queryByRole("searchbox", { name: "Find type" })).toBeNull();
      expect(view.getByRole("region", { name: "Type mappings" }).querySelector(".type-mapping-explanation")?.textContent).toMatch(/parser|serializer|preset|discards/);
    }
    fireEvent.click(view.getByRole("button", { name: "PostgreSQL" }));
    expect(view.getByRole("table")).toBeTruthy();
  });
});
