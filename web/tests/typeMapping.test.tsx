// @vitest-environment jsdom
import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";
import { AboutProvider, TypeMappingLink } from "../src/ui/CompatibilityMatrixDialog";
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
] };

describe("About type mappings", () => {
  it.each(["source", "sink"] as const)("opens the selected %s directly, searches errors and restores focus", (role) => {
    const view = render(<AboutProvider catalog={catalog}><TypeMappingLink role={role} connector="postgres" /></AboutProvider>);
    const link = view.getByRole("button", { name: /Type mapping/ });
    link.focus();
    fireEvent.click(link);
    expect(view.getByRole("dialog", { name: "About" })).toBeTruthy();
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
});
