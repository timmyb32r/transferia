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
  const view = render(<TableSelectionEditor value={{ rules: [] }} onChange={onChange} />);
  const add = view.getByRole("button", { name: "Add table rule" }) as HTMLButtonElement;
  expect(add.disabled).toBe(true);
  fireEvent.click(add);
  expect(onChange).not.toHaveBeenCalled();
});

it("reports an empty combined selection even when individual empty matches are allowed", async () => {
  const preview = vi.fn().mockResolvedValue({
    cards: [{ selected: [], excluded: [] }], issues: [{ kind: "no_tables" }],
  });
  const view = render(<TableCatalogContext.Provider value={{ tables: [], preview }}>
    <TableSelectionEditor value={{ rules: [{ include: "db.*" }], empty_matches: "allow_empty_matches" }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByText(/No tables selected\. A delivery must select at least one table/)).toBeTruthy());
});

it("bounds match previews and expands without removing the fixed viewport", async () => {
  const tables = Array.from({ length: 40 }, (_, index) => ({ namespace: "db", name: `t${index}` }));
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ rules: [{ include: "db.*" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByText("Matched tables: 40")).toBeTruthy());
  const region = view.getByLabelText("Matched tables for rule 1");
  for (const field of view.container.querySelectorAll("input")) {
    expect(field.getAttribute("type")).toBe("text");
  }
  expect(region.children.length).toBe(5);
  fireEvent.click(view.getByRole("button", { name: "Show all" }));
  expect(region.children.length).toBe(40);
  expect(view.getByLabelText("Matched tables for rule 1")).toBe(region);
  const runtime = (globalThis as typeof globalThis & {
    process?: { getBuiltinModule?: (name: "fs") => {
      readFileSync: (path: string, encoding: "utf8") => string;
    } };
  }).process;
  const css = runtime?.getBuiltinModule?.("fs").readFileSync("src/style.css", "utf8") ?? "";
  expect(css).toMatch(/\.table-rule-matches\s*\{[^}]*height: 140px;[^}]*overflow: auto;/);
  expect(css).toMatch(/\.table-selection-status\s*\{[^}]*height: 72px;/);
});

it("does not apply an obsolete preview to edited rules", async () => {
  let finish!: (preview: SelectionPreview) => void;
  const tables = [{ namespace: "db", name: "old" }];
  const preview = vi.fn(() => new Promise<SelectionPreview>(resolve => { finish = resolve; }));
  const component = (include: string) => <TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ rules: [{ include }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>;
  const view = render(component("db.old"));
  await waitFor(() => expect(preview).toHaveBeenCalledTimes(1));
  const first = finish;
  view.rerender(component("db.new"));
  await act(async () => first({ cards: [{ selected: tables, excluded: [] }], issues: [] }));
  expect(view.queryByText("db.old")).toBeNull();
});

it("removes stale matches immediately when the authenticated catalog is invalidated", async () => {
  const tables = [{ namespace: "db", name: "verified_table" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const editor = <TableSelectionEditor value={{ rules: [{ include: "db.*" }] }} onChange={() => undefined} />;
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>{editor}</TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByLabelText("Matched tables for rule 1").children.length).toBe(1));
  view.rerender(<TableCatalogContext.Provider value={undefined}>{editor}</TableCatalogContext.Provider>);
  expect(view.getByLabelText("Matched tables for rule 1").children.length).toBe(0);
  expect((view.getByRole("button", { name: "Show all" }) as HTMLButtonElement).disabled).toBe(true);
});

it("keeps a catalog across rule edits, invalidates it on connection edits, and deduplicates checks", async () => {
  let finish!: (result: ConnectionCheckResult) => void;
  const checkConnection = vi.fn(() => new Promise<ConnectionCheckResult>(resolve => { finish = resolve; }));
  const api = { ...httpControlPlane, checkConnection };
  const config = { host: "first", tables: { rules: [{ include: "db.*" }] } };
  const hook = renderHook(({ config }) => useEndpointActions({ api, role: "source", connector: "mysql", config }), { initialProps: { config } });
  act(() => { void hook.result.current.checkConnection(); void hook.result.current.checkConnection(); });
  expect(hook.result.current.check.state).toBe("checking");
  expect(checkConnection).toHaveBeenCalledTimes(1);
  await act(async () => finish({ status: "verified", options: {}, tables: [] }));
  expect(hook.result.current.check.state).toBe("success");
  hook.rerender({ config: { ...config, tables: { rules: [{ include: "db.other" }] } } });
  expect(hook.result.current.check.state).toBe("success");
  hook.rerender({ config: { ...config, host: "second" } });
  expect(hook.result.current.check.state).toBe("idle");
});
