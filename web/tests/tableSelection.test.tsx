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

it("expands matches only on request into a bounded inline viewport", async () => {
  const tables = Array.from({ length: 40 }, (_, index) => ({ namespace: "db", name: `t${index}` }));
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.*" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("button", { name: /Matched tables \(40\)/ })).toBeTruthy());
  expect(view.queryByLabelText("Matched tables for rule 1")).toBeNull();
  fireEvent.click(view.getByRole("button", { name: /Matched tables \(40\)/ }));
  const region = view.getByLabelText("Matched tables for rule 1");
  for (const field of view.container.querySelectorAll("input")) {
    expect(field.getAttribute("type")).toBe("text");
  }
  expect(region.children.length).toBe(40);
  expect(view.getByLabelText("Matched tables for rule 1")).toBe(region);
  const runtime = (globalThis as typeof globalThis & {
    process?: { getBuiltinModule?: (name: "fs") => {
      readFileSync: (path: string, encoding: "utf8") => string;
    } };
  }).process;
  const css = runtime?.getBuiltinModule?.("fs").readFileSync("src/style.css", "utf8") ?? "";
  expect(css).toMatch(/\.table-rule-matches\s*\{[^}]*height: 140px;[^}]*overflow: auto;/);
  expect(css).toMatch(/\.table-selection-status\s*\{[^}]*height: 24px;/);
  expect(css).toContain("flex: 1 0 auto; min-width: max-content;");
  expect(css).toContain("height: calc(100% - 10px); min-height: 0;");
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
  expect(view.getByRole("status").textContent).toBe("Updating matched tables…");
});

it("removes stale matches immediately when the authenticated catalog is invalidated", async () => {
  const tables = [{ namespace: "db", name: "verified_table" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const editor = <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.*" }] }} onChange={() => undefined} />;
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>{editor}</TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("button", { name: /Matched tables \(1\)/ })).toBeTruthy());
  fireEvent.click(view.getByRole("button", { name: /Matched tables \(1\)/ }));
  await waitFor(() => expect(view.getByLabelText("Matched tables for rule 1").children.length).toBe(1));
  view.rerender(<TableCatalogContext.Provider value={undefined}>{editor}</TableCatalogContext.Provider>);
  expect(view.getByLabelText("Matched tables for rule 1").children.length).toBe(0);
  expect((view.getByRole("button", { name: /Matched tables/ }) as HTMLButtonElement).disabled).toBe(true);
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
  expect(view.queryByRole("button", { name: /Matched tables/ })).toBeNull();
  fireEvent.click(view.getByRole("button", { name: "exclude regex rule 1" }));
  expect(view.getByRole("button", { name: "exclude regex rule 1" }).getAttribute("aria-pressed")).toBe("true");
  expect(view.getByRole("button", { name: "include regex rule 1" }).getAttribute("aria-pressed")).toBe("false");
  fireEvent.keyDown(view.getByRole("radio", { name: "Selected tables" }), { key: "ArrowRight" });
  expect(view.queryByLabelText("Include rule 1")).toBeNull();
  fireEvent.input(view.getByLabelText("Exclude rule 1"), { target: { value: "db.temp*" } });
  await waitFor(() => expect(preview.mock.lastCall?.[0].selection).toEqual({ type: "all", exclude: "db.temp*", exclude_mode: "glob" }));
  fireEvent.click(view.getByRole("radio", { name: "Selected tables" }));
  expect((view.getByLabelText("Include rule 1") as HTMLInputElement).value).toBe("db.users");
  expect(view.getByRole("button", { name: "exclude regex rule 1" }).getAttribute("aria-pressed")).toBe("true");
  fireEvent.click(view.getByRole("radio", { name: "All tables" }));
  expect((view.getByLabelText("Exclude rule 1") as HTMLInputElement).value).toBe("db.temp*");
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
