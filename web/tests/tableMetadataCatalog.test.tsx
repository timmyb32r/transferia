// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { AvailableTablesButton } from "../src/features/tableSelection/AvailableTablesDialog";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import type { MetadataStatus } from "../src/generated/apiContract";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });
const user = { namespace: "public", name: "events" };
const system = { namespace: "system", name: "symbols" };
const pending = { namespace: "public", name: "later" };
const metadata: MetadataStatus = { id: "session", catalog_count: 3, loaded: [user],
  errors: [{ table: system, message: "Introspection disabled" }], loading: false };

it("filters failed tables without opening error details until a failed row is clicked", () => {
  const preview = vi.fn();
  const view = render(<TableCatalogContext.Provider value={{ tables: [user, system, pending], preview, metadata }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>);
  fireEvent.click(view.getByRole("button", { name: "Show 1 failed schemas" }));
  const dialog = view.getByRole("dialog");
  expect(within(dialog).getByRole("radio", { name: "Failed (1)" }).getAttribute("aria-checked")).toBe("true");
  expect(within(dialog).getByRole("button", { name: "Copy system.symbols" })).toBeTruthy();
  expect(within(dialog).queryByRole("button", { name: "Copy public.events" })).toBeNull();
  expect(within(dialog).queryByRole("region", { name: "Schema error" })).toBeNull();
  fireEvent.click(within(dialog).getByText("system.symbols"));
  expect(within(dialog).getByRole("region", { name: "Schema error" }).textContent).toContain("Introspection disabled");
  fireEvent.click(within(dialog).getByRole("button", { name: "Close schema error" }));
  fireEvent.click(within(dialog).getByRole("radio", { name: "Not loaded (1)" }));
  expect(within(dialog).getByRole("button", { name: "Copy public.later" })).toBeTruthy();
  expect(within(dialog).queryByRole("region", { name: "Schema error" })).toBeNull();
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
  expect(within(dialog).getByLabelText("Schema Not loaded for public.later").textContent).toBe("Not loaded");
  fireEvent.click(within(dialog).getByRole("button", { name: "Show schema error for system.symbols" }));
  expect(within(dialog).getByRole("region", { name: "Schema error" }).textContent).toContain("Introspection disabled");
  fireEvent.click(within(dialog).getByRole("button", { name: "Close schema error" }));
  fireEvent.click(within(dialog).getByRole("button", { name: "Close available tables" }));
  view.rerender(form(true));
  expect(trigger.textContent).toContain("Schemas loaded 1/2");
  expect(trigger.textContent).not.toContain("failed");
  expect(view.getByRole("button", { name: "Browse metadata" })).toBe(trigger);
  expect(preview).not.toHaveBeenCalled();
});

it("has no empty Schema errors pane or error actions for loaded and pending tables", () => {
  const view = render(<TableCatalogContext.Provider value={{ tables: [user, pending], preview: vi.fn(),
    metadata: { ...metadata, errors: [] } }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>);
  fireEvent.click(view.getByRole("button", { name: "Browse metadata" }));
  expect(view.queryByText("Schema errors")).toBeNull();
  expect(view.queryByRole("button", { name: "Copy schema error" })).toBeNull();
  expect(view.queryByRole("button", { name: /Show schema error for/ })).toBeNull();
  fireEvent.click(view.getByText("public.events"));
  fireEvent.click(view.getByText("public.later"));
  expect(view.queryByRole("region", { name: "Schema error" })).toBeNull();
});

it("traps error detail focus and restores the unchanged list and trigger with Escape", () => {
  const view = render(<TableCatalogContext.Provider value={{ tables: [user, system], preview: vi.fn(), metadata }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>);
  const browse = view.getByRole("button", { name: "Browse metadata" });
  browse.focus();
  fireEvent.click(browse);
  const dialog = view.getByRole("dialog");
  const list = view.getByRole("region", { name: "Available table names" });
  list.scrollTop = 80;
  const search = view.getByRole("textbox", { name: "Search tables" });
  const trigger = view.getByRole("button", { name: "Show schema error for system.symbols" });
  const copyName = view.getByRole("button", { name: "Copy system.symbols" });
  expect(trigger.tagName).toBe("BUTTON");
  fireEvent.click(within(trigger).getByText("Failed"));
  const error = view.getByRole("region", { name: "Schema error" });
  const text = within(error).getByLabelText("Full schema error");
  const copy = within(error).getByRole("button", { name: "Copy schema error" });
  expect(document.activeElement).toBe(text);
  expect(view.queryByRole("textbox", { name: "Search tables" })).toBeNull();
  expect(search.closest("[inert]")).not.toBeNull();
  fireEvent.keyDown(text, { key: "Tab" });
  expect(document.activeElement).toBe(copy);
  fireEvent.keyDown(copy, { key: "Tab", shiftKey: true });
  expect(document.activeElement).toBe(text);
  fireEvent.keyDown(text, { key: "Escape" });
  expect(view.getByRole("dialog")).toBe(dialog);
  expect(view.queryByRole("region", { name: "Schema error" })).toBeNull();
  expect(document.activeElement).toBe(trigger);
  expect(view.getByRole("textbox", { name: "Search tables" })).toBe(search);
  expect(view.getByRole("button", { name: "Copy system.symbols" })).toBe(copyName);
  expect(view.getByRole("region", { name: "Available table names" })).toBe(list);
  expect(list.scrollTop).toBe(80);
  fireEvent.keyDown(trigger, { key: "Escape" });
  expect(view.queryByRole("dialog")).toBeNull();
  expect(document.activeElement).toBe(browse);
});

it("keeps the clicked error snapshot through polling and copies the complete message", async () => {
  const message = "Detailed schema failure\n".repeat(200);
  const tables = [system];
  const writeText = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
  const form = (status: MetadataStatus) => <TableCatalogContext.Provider value={{ tables, preview: vi.fn(), metadata: status }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>;
  const view = render(form({ ...metadata, errors: [{ table: system, message }] }));
  fireEvent.click(view.getByRole("button", { name: "Show 1 failed schemas" }));
  const list = view.getByRole("region", { name: "Available table names" });
  const trigger = view.getByRole("button", { name: "Show schema error for system.symbols" });
  fireEvent.click(trigger);
  view.rerender(form({ ...metadata, loaded: [system], errors: [] }));
  expect(view.getByLabelText("Full schema error").textContent).toBe(message);
  expect(list.querySelector(".available-table-row")).not.toBeNull();
  fireEvent.click(view.getByRole("button", { name: "Copy schema error" }));
  await waitFor(() => expect(writeText).toHaveBeenCalledExactlyOnceWith(message));
  fireEvent.click(view.getByRole("button", { name: "Close schema error" }));
  expect(document.activeElement).toBe(trigger);
  act(() => view.getByRole("textbox", { name: "Search tables" }).focus());
  expect(view.queryByRole("button", { name: "Show schema error for system.symbols" })).toBeNull();
});

it("keeps Copy and Use on a failed row separate from opening its error", async () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
  const onUse = vi.fn();
  const view = render(<TableCatalogContext.Provider value={{ tables: [system], preview: vi.fn(), metadata }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata showUse onUse={onUse} />
  </TableCatalogContext.Provider>);
  fireEvent.click(view.getByRole("button", { name: "Browse metadata" }));
  fireEvent.click(view.getByRole("button", { name: "Copy system.symbols" }));
  await waitFor(() => expect(writeText).toHaveBeenCalledExactlyOnceWith("system.symbols"));
  expect(view.queryByRole("region", { name: "Schema error" })).toBeNull();
  fireEvent.click(view.getByRole("button", { name: "Use system.symbols in Include" }));
  expect(onUse).toHaveBeenCalledExactlyOnceWith(system);
  expect(view.queryByRole("dialog")).toBeNull();
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
  const error = "Metadata session unavailable. Refresh tables to retry.";
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
  expect(view.getByLabelText("Schema Not loaded for public.later")).toBeTruthy();
  fireEvent.pointerLeave(list);
  expect(view.queryByRole("button", { name: "Copy public.later" })).toBeNull();
});

it.each(["pointer", "keyboard"])("defers a new Failed action until the %s leaves the table list", (interaction) => {
  const tables = [pending];
  const form = (failed: boolean) => <TableCatalogContext.Provider value={{ tables, preview: vi.fn(),
    metadata: { ...metadata, loaded: [], errors: failed ? [{ table: pending, message: "Cannot read schema" }] : [] } }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>;
  const view = render(form(false));
  fireEvent.click(view.getByRole("button", { name: "Browse metadata" }));
  const list = view.getByRole("region", { name: "Available table names" });
  const search = view.getByRole("textbox", { name: "Search tables" });
  const copy = view.getByRole("button", { name: "Copy public.later" });
  const details = copy.closest(".available-table-row")!.querySelector(".available-table-details");
  if (interaction === "pointer") fireEvent.pointerEnter(list);
  else act(() => copy.focus());
  view.rerender(form(true));
  expect(copy.closest(".available-table-row")!.querySelector(".available-table-details")).toBe(details);
  expect(view.queryByRole("button", { name: "Show schema error for public.later" })).toBeNull();
  // Moving between pointer and keyboard controls inside the held list must not
  // replace its snapshot with the newer polling result.
  if (interaction === "pointer") act(() => copy.focus());
  else fireEvent.pointerEnter(list);
  expect(view.queryByRole("button", { name: "Show schema error for public.later" })).toBeNull();
  fireEvent.pointerLeave(list);
  expect(view.queryByRole("button", { name: "Show schema error for public.later" })).toBeNull();
  act(() => search.focus());
  const failed = view.getByRole("button", { name: "Show schema error for public.later" });
  fireEvent.click(failed);
  expect(view.getByLabelText("Full schema error").textContent).toBe("Cannot read schema");
});

it("keeps a focused Failed action and its diagnostic until focus leaves the list", () => {
  const tables = [system];
  const form = (resolved: boolean) => <TableCatalogContext.Provider value={{ tables, preview: vi.fn(),
    metadata: resolved ? { ...metadata, loaded: tables, errors: [] } : metadata }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>;
  const view = render(form(false));
  fireEvent.click(view.getByRole("button", { name: "Browse metadata" }));
  const trigger = view.getByRole("button", { name: "Show schema error for system.symbols" });
  act(() => trigger.focus());
  view.rerender(form(true));
  expect(view.getByRole("button", { name: "Show schema error for system.symbols" })).toBe(trigger);
  expect(document.activeElement).toBe(trigger);
  fireEvent.click(trigger);
  expect(view.getByLabelText("Full schema error").textContent).toBe("Introspection disabled");
  fireEvent.click(view.getByRole("button", { name: "Close schema error" }));
  expect(document.activeElement).toBe(trigger);
  act(() => view.getByRole("textbox", { name: "Search tables" }).focus());
  expect(view.queryByRole("button", { name: "Show schema error for system.symbols" })).toBeNull();
  expect(view.getByLabelText("Schema Loaded for system.symbols")).toBeTruthy();
});

it("applies a deliberate schema filter change even while the old list is held", () => {
  const tables = [pending];
  const form = (failed: boolean) => <TableCatalogContext.Provider value={{ tables, preview: vi.fn(),
    metadata: { ...metadata, loaded: [], errors: failed ? [{ table: pending, message: "Schema failed" }] : [] } }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>;
  const view = render(form(false));
  fireEvent.click(view.getByRole("button", { name: "Browse metadata" }));
  fireEvent.pointerEnter(view.getByRole("region", { name: "Available table names" }));
  view.rerender(form(true));
  expect(view.queryByRole("button", { name: "Show schema error for public.later" })).toBeNull();
  fireEvent.click(view.getByRole("radio", { name: "Failed (1)" }));
  expect(view.getByRole("button", { name: "Show schema error for public.later" })).toBeTruthy();
  fireEvent.click(view.getByRole("radio", { name: "All (1)" }));
  expect(view.getByRole("button", { name: "Show schema error for public.later" })).toBeTruthy();
});

it.each(["query", "mode"])("does not revive held schema states after a %s round trip", (change) => {
  const tables = [pending];
  const form = (failed: boolean) => <TableCatalogContext.Provider value={{ tables, preview: vi.fn(),
    metadata: { ...metadata, loaded: [], errors: failed ? [{ table: pending, message: "Schema failed" }] : [] } }}>
    <AvailableTablesButton label="Browse metadata" title="Browse metadata" showMetadata />
  </TableCatalogContext.Provider>;
  const view = render(form(false));
  fireEvent.click(view.getByRole("button", { name: "Browse metadata" }));
  fireEvent.pointerEnter(view.getByRole("region", { name: "Available table names" }));
  view.rerender(form(true));
  expect(view.queryByRole("button", { name: "Show schema error for public.later" })).toBeNull();
  if (change === "query") {
    const search = view.getByRole("textbox", { name: "Search tables" });
    fireEvent.input(search, { target: { value: "public" } });
    fireEvent.input(search, { target: { value: "" } });
  } else {
    const mode = view.getByRole("button", { name: "Search tables regex" });
    fireEvent.click(mode);
    fireEvent.click(mode);
  }
  expect(view.getByRole("button", { name: "Show schema error for public.later" })).toBeTruthy();
});
