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

it("opens failed tables and their full error with one click, then filters pending schemas locally", () => {
  const preview = vi.fn();
  const view = render(<TableCatalogContext.Provider value={{ tables: [user, system, pending], preview, metadata }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>);
  fireEvent.click(view.getByRole("button", { name: "Show 1 failed schemas" }));
  const dialog = view.getByRole("dialog");
  expect(within(dialog).getByRole("radio", { name: "Failed (1)" }).getAttribute("aria-checked")).toBe("true");
  expect(within(dialog).getByRole("button", { name: "Copy system.symbols" })).toBeTruthy();
  expect(within(dialog).queryByRole("button", { name: "Copy public.events" })).toBeNull();
  const error = within(dialog).getByRole("region", { name: "Schema error" });
  expect(error.textContent).toContain("Introspection disabled");
  fireEvent.click(within(dialog).getByRole("radio", { name: "Not loaded (1)" }));
  expect(within(dialog).getByRole("button", { name: "Copy public.later" })).toBeTruthy();
  expect(within(dialog).getByRole("region", { name: "Schema error" })).toBe(error);
  expect(preview).not.toHaveBeenCalled();
});

it("uses the visible catalog for both schema counters and exposes cached failure reasons in the same popup", () => {
  const preview = vi.fn();
  const form = (hide: boolean) => <TableCatalogContext.Provider value={{ tables: hide ? [user, pending] : [user, system, pending], preview, metadata }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>;
  const view = render(form(false));
  const trigger = view.getByRole("button", { name: "Browse metadata" });
  expect(trigger.textContent).toContain("Available tables (3)");
  expect(trigger.textContent).toContain("Schemas loaded 1/3");
  expect(view.getByRole("button", { name: "Show 1 failed schemas" }).textContent).toBe("1 failed");
  fireEvent.click(trigger);
  const dialog = view.getByRole("dialog");
  fireEvent.click(within(dialog).getByRole("button", { name: "Show schema error for system.symbols" }));
  expect(within(dialog).getByRole("region", { name: "Schema error" }).textContent).toContain("Introspection disabled");
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
  expect(trigger.textContent).toContain("Schemas loaded 1/3");
});

it("exposes metadata polling failure without pretending the cached schemas are still loading", () => {
  const error = "Metadata session unavailable. Refresh metadata to retry.";
  const view = render(<TableCatalogContext.Provider value={{ tables: [user], preview: vi.fn(), metadata, metadataError: error }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>);
  fireEvent.click(view.getByRole("button", { name: "Browse metadata" }));
  expect(view.getByRole("status").textContent).toBe(error);
});

it("defers filtered row removal while the pointer is over its Copy/Use controls", () => {
  const tables = [pending];
  const form = (loaded: MetadataStatus["loaded"]) => <TableCatalogContext.Provider value={{ tables, preview: vi.fn(),
    metadata: { ...metadata, loaded, errors: [], catalog_count: 1 } }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>;
  const view = render(form([]));
  fireEvent.click(view.getByRole("button", { name: "Browse metadata" }));
  fireEvent.click(view.getByRole("radio", { name: "Not loaded (1)" }));
  const list = view.getByRole("region", { name: "Available table names" });
  const copy = view.getByRole("button", { name: "Copy public.later" });
  fireEvent.pointerEnter(list);
  view.rerender(form([pending]));
  expect(view.getByRole("button", { name: "Copy public.later" })).toBe(copy);
  expect(view.getByLabelText("Schema Loaded for public.later")).toBeTruthy();
  fireEvent.pointerLeave(list);
  expect(view.queryByRole("button", { name: "Copy public.later" })).toBeNull();
});
