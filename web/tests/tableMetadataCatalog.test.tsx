// @vitest-environment jsdom
import { cleanup, fireEvent, within } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { AvailableTablesButton } from "../src/features/tableSelection/AvailableTablesDialog";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import type { MetadataStatus } from "../src/generated/apiContract";
import { render } from "./support/render";

afterEach(cleanup);
const user = { namespace: "public", name: "events" };
const system = { namespace: "system", name: "symbols" };
const pending = { namespace: "public", name: "later" };
const metadata: MetadataStatus = { id: "session", catalog_count: 3, loaded: [user],
  errors: [{ table: system, message: "Introspection disabled" }], loading: false };

it("uses the visible catalog for both schema counters and exposes cached failure reasons in the same popup", () => {
  const preview = vi.fn();
  const form = (hide: boolean) => <TableCatalogContext.Provider value={{ tables: hide ? [user, pending] : [user, system, pending], preview, metadata }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>;
  const view = render(form(false));
  const trigger = view.getByRole("button", { name: "Browse metadata" });
  expect(trigger.textContent).toContain("Available tables (3)");
  expect(trigger.textContent).toContain("Schemas loaded 1/3 · 1 failed");
  fireEvent.click(trigger);
  const dialog = view.getByRole("dialog");
  expect(within(dialog).getByLabelText("Schema failed for system.symbols: Introspection disabled").title).toBe("Introspection disabled");
  expect(within(dialog).getByLabelText("Schema Not loaded for public.later").textContent).toBe("Not loaded");
  fireEvent.click(within(dialog).getByRole("button", { name: "Close available tables" }));
  view.rerender(form(true));
  expect(trigger.textContent).toContain("Schemas loaded 1/2");
  expect(trigger.textContent).not.toContain("failed");
  expect(view.getByRole("button", { name: "Browse metadata" })).toBe(trigger);
  expect(preview).not.toHaveBeenCalled();
});

it("keeps popup controls and rows mounted while schema progress advances", () => {
  const tables = [user, system, pending];
  const preview = vi.fn();
  const form = (status: MetadataStatus) => <TableCatalogContext.Provider value={{ tables, preview, metadata: status }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>;
  const view = render(form({ ...metadata, loaded: [], errors: [], loading: true }));
  const trigger = view.getByRole("button", { name: "Browse metadata" });
  fireEvent.click(trigger);
  const input = view.getByRole("textbox", { name: "Search tables" });
  const copy = view.getByRole("button", { name: "Copy public.events" });
  const row = copy.closest(".available-table-row");
  view.rerender(form(metadata));
  expect(view.getByRole("textbox", { name: "Search tables" })).toBe(input);
  expect(document.activeElement).toBe(input);
  expect(view.getByRole("button", { name: "Copy public.events" })).toBe(copy);
  expect(copy.closest(".available-table-row")).toBe(row);
  expect(trigger.textContent).toContain("Schemas loaded 1/3 · 1 failed");
});

it("exposes metadata polling failure without pretending the cached schemas are still loading", () => {
  const error = "Metadata session unavailable. Refresh metadata to retry.";
  const view = render(<TableCatalogContext.Provider value={{ tables: [user], preview: vi.fn(), metadata, metadataError: error }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>);
  fireEvent.click(view.getByRole("button", { name: "Browse metadata" }));
  expect(view.getByRole("status").textContent).toBe(error);
});
