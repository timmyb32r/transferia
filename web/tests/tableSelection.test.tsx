// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { TableSelectionEditor } from "../src/features/tableSelection/TableSelectionEditor";
import { exactPattern, qualifiedName } from "../src/features/tableSelection/model";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import { useEndpointActions } from "../src/delivery/useEndpointActions";
import { httpControlPlane } from "../src/infrastructure/controlPlane/httpControlPlane";
import type { ConnectionCheckResult, SelectionPreview } from "../src/generated/apiContract";
import { render, renderHook } from "./support/render";
import { useState } from "preact/hooks";
import type { JsonValue } from "../src/json";
import { tableConnectionIdentity } from "../src/delivery/useEndpointActions";
import { visibleTableCatalog } from "../src/features/tableSelection/catalog";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

it("preserves identifier boundaries and inserts exact escaped patterns", () => {
  const table = { namespace: "a.b", name: "c*?\\x" };
  expect(qualifiedName(table)).toBe("a\\.b.c*?\\\\x");
  expect(qualifiedName(table)).not.toBe(qualifiedName({ namespace: "a", name: "b.c*?\\x" }));
  const expression = exactPattern(table, "regex");
  expect(new RegExp(`^(?:${expression})$`, "u").test(qualifiedName(table))).toBe(true);
});

it("gates additions until a verified catalog is present", () => {
  const onChange = vi.fn();
  const view = render(<TableSelectionEditor value={{ type: "selected", rules: [] }} onChange={onChange} />);
  const add = view.getByRole("button", { name: "Add table rule" }) as HTMLButtonElement;
  expect(add.disabled).toBe(true);
  fireEvent.click(add);
  expect(onChange).not.toHaveBeenCalled();
});

it("reports an empty rule without offering an empty-match policy", async () => {
  const preview = vi.fn().mockResolvedValue({
    cards: [{ selected: [], excluded: [] }], issues: [{ kind: "empty_match", card: 0 }],
  });
  const view = render(<TableCatalogContext.Provider value={{ tables: [], preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.*" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByText("Rule 1 selects no tables.")).toBeTruthy());
  expect(view.queryByText("If a table rule matches nothing")).toBeNull();
  expect(view.queryByText("Allow empty matches")).toBeNull();
});

it("expands the complete matched list only on request into a bounded inline viewport", async () => {
  const tables = Array.from({ length: 40 }, (_, index) => ({ namespace: "db", name: `t${index}` }));
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.*" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("button", { name: "Matched tables 40" })).toBeTruthy());
  expect(view.queryByLabelText("All matched tables")).toBeNull();
  fireEvent.click(view.getByRole("button", { name: "Matched tables 40" }));
  const region = view.getByLabelText("All matched tables");
  for (const field of view.container.querySelectorAll("input")) {
    expect(field.getAttribute("type")).toBe("text");
  }
  expect(region.children.length).toBe(40);
  expect(view.getByLabelText("All matched tables")).toBe(region);
  const runtime = (globalThis as typeof globalThis & {
    process?: { getBuiltinModule?: (name: "fs") => {
      readFileSync: (path: string, encoding: "utf8") => string;
    } };
  }).process;
  const css = runtime?.getBuiltinModule?.("fs").readFileSync("src/style.css", "utf8") ?? "";
  expect(css).toMatch(/\.table-rule-matches\s*\{[^}]*height: 140px;[^}]*overflow: auto;/);
  expect(css).toMatch(/\.table-selection-footer\s*\{[^}]*height: var\(--control-height\);/);
  expect(css).toMatch(/\.segmented-control\s*\{[^}]*grid-auto-columns: minmax\(max-content, 1fr\);/);
  expect(css).toMatch(/\.segmented-control > button\s*\{[^}]*min-width: max-content;/);
  expect(css).toMatch(/\.regex-toggle\s*\{[^}]*height: calc\(var\(--control-height\) - 8px\);/);
  expect(css).toMatch(/\.regex-toggle\[aria-pressed="true"\]\s*\{[^}]*background: var\(--blue\);[^}]*color: var\(--on-accent\);/);
  expect(css).toMatch(/\.table-rule-result\s*\{[^}]*height: 24px;/);
});

it("collects every card into one matched list without duplicating table identities", async () => {
  const first = { namespace: "db", name: "users" };
  const second = { namespace: "db", name: "reports" };
  const sameNameElsewhere = { namespace: "other", name: "users" };
  const tables = [first, second, sameNameElsewhere];
  const preview = vi.fn().mockResolvedValue({
    cards: [{ selected: [first, second], excluded: [] }, { selected: [first, sameNameElsewhere], excluded: [] }],
    issues: [],
  });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.*" }, { include: "*.users" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("button", { name: "Matched tables 3" })).toBeTruthy());
  expect(view.getAllByRole("button", { name: /^Matched tables for rule/ })).toHaveLength(2);
  fireEvent.click(view.getByRole("button", { name: "Matched tables for rule 1" }));
  expect([...view.getByLabelText("Matches for rule 1").children].map(child => child.textContent))
    .toEqual(["db.users", "db.reports"]);
  fireEvent.click(view.getByRole("button", { name: "Matched tables 3" }));
  expect([...view.getByLabelText("All matched tables").children].map(child => child.textContent))
    .toEqual(["db.users", "db.reports", "other.users"]);
});

it("offers an initial empty row and never previews unfinished includes", async () => {
  const preview = vi.fn();
  const tables = [{ namespace: "db", name: "users" }];
  function Editor() {
    const [value, setValue] = useState<JsonValue>({ type: "selected", rules: [] });
    return <TableCatalogContext.Provider value={{ tables, preview }}>
      <TableSelectionEditor value={value} onChange={setValue} />
    </TableCatalogContext.Provider>;
  }
  vi.useFakeTimers();
  try {
    const view = render(<Editor />);
    expect((view.getByLabelText("Include rule 1") as HTMLInputElement).value).toBe("");
    fireEvent.click(view.getByRole("button", { name: "Add table rule" }));
    expect(view.getByLabelText("Include rule 2")).toBeTruthy();
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(preview).not.toHaveBeenCalled();
    expect(view.getByRole("status").textContent).toBe("Enter a table name or pattern.");
    expect(view.getByRole("status").getAttribute("aria-busy")).toBe("false");
  } finally { vi.useRealTimers(); }
});

it("does not apply an obsolete preview to edited rules", async () => {
  let finish!: (preview: SelectionPreview) => void;
  const tables = [{ namespace: "db", name: "old" }];
  const preview = vi.fn(() => new Promise<SelectionPreview>(resolve => { finish = resolve; }));
  const component = (include: string) => <TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>;
  const view = render(component("db.old"));
  await waitFor(() => expect(preview).toHaveBeenCalledTimes(1));
  const first = finish;
  view.rerender(component("db.new"));
  await act(async () => first({ cards: [{ selected: tables, excluded: [] }], issues: [] }));
  expect(view.container.querySelector(".table-rule-matches")).toBeNull();
  expect(view.getByRole("status").textContent).toBe("");
  expect(view.getByRole("status").getAttribute("aria-busy")).toBe("true");
});

it("clears displayed matches while edited rules are pending without replacing the footer controls", async () => {
  const tables = [{ namespace: "db", name: "old" }, { namespace: "db", name: "new" }];
  let finish!: (preview: SelectionPreview) => void;
  const preview = vi.fn()
    .mockResolvedValueOnce({ cards: [{ selected: [tables[0]], excluded: [] }], issues: [] })
    .mockImplementationOnce(() => new Promise<SelectionPreview>(resolve => { finish = resolve; }));
  const component = (include: string) => <TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>;
  const view = render(component("db.old"));
  await waitFor(() => expect(view.getByRole("button", { name: "Matched tables 1" })).toBeTruthy());
  const toggle = view.getByRole("button", { name: "Matched tables 1" });
  const add = view.getByRole("button", { name: "Add table rule" });
  const footer = toggle.parentElement;
  fireEvent.click(toggle);
  const region = view.getByLabelText("All matched tables");
  expect(region.textContent).toBe("db.old");
  view.rerender(component("db.new"));
  expect(region.textContent).toBe("Waiting for a valid table selection…");
  expect(region.getAttribute("aria-busy")).toBe("true");
  expect(view.getByRole("status").textContent).toBe("");
  expect(view.getByRole("status").getAttribute("aria-busy")).toBe("true");
  expect(view.getByRole("button", { name: "Matched tables —" })).toBe(toggle);
  expect((toggle as HTMLButtonElement).disabled).toBe(true);
  expect(view.getByRole("button", { name: "Add table rule" })).toBe(add);
  expect(add.parentElement).toBe(footer);
  await waitFor(() => expect(preview).toHaveBeenCalledTimes(2));
  await act(async () => finish({ cards: [{ selected: [tables[1]!], excluded: [] }], issues: [] }));
  expect(region.textContent).toBe("db.new");
  expect(region.getAttribute("aria-busy")).toBe("false");
  expect(toggle.parentElement).toBe(footer);
});

it("removes stale matches immediately when the authenticated catalog is invalidated", async () => {
  const tables = [{ namespace: "db", name: "verified_table" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const editor = <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.*" }] }} onChange={() => undefined} />;
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>{editor}</TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("button", { name: "Matched tables 1" })).toBeTruthy());
  fireEvent.click(view.getByRole("button", { name: "Matched tables 1" }));
  expect(view.getByLabelText("All matched tables").textContent).toBe("db.verified_table");
  view.rerender(<TableCatalogContext.Provider value={undefined}>{editor}</TableCatalogContext.Provider>);
  expect(view.getByLabelText("All matched tables").textContent).toBe("Waiting for a valid table selection…");
  expect((view.getByRole("button", { name: "Matched tables —" }) as HTMLButtonElement).disabled).toBe(true);
});

it("shows all tables without any include or exclude fields and previews the complete catalog", async () => {
  const tables = [{ namespace: "db", name: "users" }, { namespace: "db", name: "reports" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "all" }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  expect(view.queryByRole("textbox")).toBeNull();
  expect(view.queryByRole("button", { name: "Add table rule" })).toBeNull();
  expect(view.queryByText("Exclude")).toBeNull();
  await waitFor(() => expect(preview).toHaveBeenCalledTimes(1));
  expect(preview.mock.lastCall?.[0]).toEqual({ selection: { type: "all" }, catalog: tables });
  fireEvent.click(view.getByRole("button", { name: "Matched tables 2" }));
  expect([...view.getByLabelText("All matched tables").children].map(child => child.textContent))
    .toEqual(["db.users", "db.reports"]);
});

it("keeps inactive drafts, independent regex modes and keyboard segment selection", async () => {
  const tables = [{ namespace: "db", name: "users" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  function Editor() {
    const [value, setValue] = useState<JsonValue>({ type: "selected", rules: [{ include: "db.users" }] });
    return <TableCatalogContext.Provider value={{ tables, preview }}><TableSelectionEditor value={value} onChange={setValue} /></TableCatalogContext.Provider>;
  }
  const view = render(<Editor />);
  await waitFor(() => expect(preview).toHaveBeenCalled());
  expect(view.getByRole("button", { name: "Matched tables 1" })).toBeTruthy();
  fireEvent.input(view.getByLabelText("Exclude rule 1"), { target: { value: "db.temp.*" } });
  fireEvent.click(view.getByRole("button", { name: "exclude regex rule 1" }));
  expect(view.getByRole("button", { name: "exclude regex rule 1" }).getAttribute("aria-pressed")).toBe("true");
  expect(view.getByRole("button", { name: "include regex rule 1" }).getAttribute("aria-pressed")).toBe("false");
  fireEvent.keyDown(view.getByRole("radio", { name: "Selected tables" }), { key: "ArrowRight" });
  expect(view.queryByLabelText("Include rule 1")).toBeNull();
  expect(view.queryByLabelText("Exclude rule 1")).toBeNull();
  await waitFor(() => expect(preview.mock.lastCall?.[0].selection).toEqual({ type: "all" }));
  fireEvent.click(view.getByRole("radio", { name: "Selected tables" }));
  expect((view.getByLabelText("Include rule 1") as HTMLInputElement).value).toBe("db.users");
  expect((view.getByLabelText("Exclude rule 1") as HTMLInputElement).value).toBe("db.temp.*");
  expect(view.getByRole("button", { name: "exclude regex rule 1" }).getAttribute("aria-pressed")).toBe("true");
  fireEvent.click(view.getByRole("radio", { name: "All tables" }));
  expect(view.queryByLabelText("Exclude rule 1")).toBeNull();
});

it("keeps a catalog across rule edits, invalidates it on connection edits, and deduplicates checks", async () => {
  let finish!: (result: ConnectionCheckResult) => void;
  const checkConnection = vi.fn(() => new Promise<ConnectionCheckResult>(resolve => { finish = resolve; }));
  const api = { ...httpControlPlane, checkConnection };
  const config = { host: "first", tables: { type: "selected", rules: [{ include: "db.*" }] } };
  const hook = renderHook(({ config }) => useEndpointActions({ api, role: "source", connector: "mysql", config }), { initialProps: { config } });
  act(() => { void hook.result.current.checkConnection(); void hook.result.current.checkConnection(); });
  expect(hook.result.current.check.state).toBe("checking");
  expect(checkConnection).toHaveBeenCalledTimes(1);
  await act(async () => finish({ status: "verified", options: {}, tables: [] }));
  expect(hook.result.current.check.state).toBe("success");
  hook.rerender({ config: { ...config, tables: { type: "selected", rules: [{ include: "db.other" }] } } });
  expect(hook.result.current.check.state).toBe("success");
  hook.rerender({ config: { ...config, host: "second" } });
  expect(hook.result.current.check.state).toBe("idle");
});

it("keeps the full ClickHouse catalog and connection identity when hiding system tables", async () => {
  const namespaces = ["system", "_system", "information_schema", "information_schema_extra", "INFORMATION_SCHEMA",
    "default", "system_backup", "_system_backup", "my_information_schema", "INFORMATION_SCHEMA_extra", "System"];
  const tables = namespaces.map(namespace => ({ namespace, name: "t" }));
  const checkConnection = vi.fn().mockResolvedValue({ status: "verified", options: {}, tables });
  const api = { ...httpControlPlane, checkConnection };
  const config = { host: "first", hide_system_tables: true, tables: { type: "all" } };
  const hook = renderHook(({ config }) => useEndpointActions({ api, connector: "clickhouse", role: "source", config }), { initialProps: { config } });
  await act(async () => { await hook.result.current.checkConnection(); });
  const identity = tableConnectionIdentity("clickhouse", config);
  for (const hide_system_tables of [false, true, false]) {
    hook.rerender({ config: { ...config, hide_system_tables } });
    expect(hook.result.current.check.state).toBe("success");
    expect(hook.result.current.check).toMatchObject({ tables });
    expect(tableConnectionIdentity("clickhouse", { ...config, hide_system_tables })).toBe(identity);
    expect(visibleTableCatalog("clickhouse", hide_system_tables, tables).map(table => table.namespace))
      .toEqual(hide_system_tables ? namespaces.slice(5) : namespaces);
  }
  expect(checkConnection).toHaveBeenCalledTimes(1);
  hook.rerender({ config: { ...config, host: "second" } });
  expect(hook.result.current.check.state).toBe("idle");
  expect(visibleTableCatalog("postgres", true, tables)).toBe(tables);
});

it("previews completed cards beside empty drafts and preserves card indices", async () => {
  const tables = [{ namespace: "schema", name: "reports" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "" }, { include: "schema*" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("button", { name: "Matched tables for rule 2" }).textContent).toContain("1"));
  expect(preview.mock.lastCall?.[0].selection.rules).toEqual([{ include: "schema*" }]);
  fireEvent.click(view.getByRole("button", { name: "Matched tables for rule 2" }));
  expect(view.getByLabelText("Matches for rule 2").textContent).toBe("schema.reports");
});

it("keeps an expanded rule preview collapsible when a pattern becomes an exact name", async () => {
  const tables = [{ namespace: "schema", name: "reports" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  function Editor() {
    const [value, setValue] = useState<JsonValue>({ type: "selected", rules: [{ include: "schema*" }] });
    return <TableCatalogContext.Provider value={{ tables, preview }}>
      <TableSelectionEditor value={value} onChange={setValue} />
    </TableCatalogContext.Provider>;
  }
  const view = render(<Editor />);
  const toggle = view.getByRole("button", { name: "Matched tables for rule 1" });
  await waitFor(() => expect((toggle as HTMLButtonElement).disabled).toBe(false));
  fireEvent.click(toggle);
  const region = view.getByLabelText("Matches for rule 1");
  fireEvent.input(view.getByRole("combobox", { name: "Include rule 1" }), { target: { value: "schema.reports" } });
  expect(view.getByLabelText("Matches for rule 1")).toBe(region);
  expect(view.getByRole("button", { name: "Matched tables for rule 1" })).toBe(toggle);
  await waitFor(() => expect((toggle as HTMLButtonElement).disabled).toBe(false));
  fireEvent.click(toggle);
  expect(view.queryByLabelText("Matches for rule 1")).toBeNull();
  expect(view.queryByRole("button", { name: "Matched tables for rule 1" })).toBeNull();
});

it("maps invalid-pattern diagnostics back to the original draft row", async () => {
  const preview = vi.fn().mockRejectedValue(new Error("Invalid table rule at card index 0, Include: invalid regex"));
  const view = render(<TableCatalogContext.Provider value={{ tables: [], preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "" }, { include: "[", include_mode: "regex" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("status").textContent).toBe("Rule 2, Include: invalid regex"));
});
