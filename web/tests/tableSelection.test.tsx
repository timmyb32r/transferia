// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
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

it.each([false, true])("keeps pattern help on the fields, not the table-mode selector (fixed=%s)", fixed => {
  const form = (type: "selected" | "all") => <TableSelectionEditor
    value={type === "selected" ? { type, rules: [] } : { type }} fixed={fixed} onChange={() => undefined} />;
  const view = render(form("selected"));
  const toolbar = view.container.querySelector(".table-selection-toolbar")!;
  expect(toolbar.querySelector(".help, [title], [role=tooltip]")).toBeNull();
  const fields = view.container.querySelectorAll(".table-rule-patterns .form-row");
  expect(fields).toHaveLength(2);
  for (const field of fields) {
    expect(field.querySelector('[role="tooltip"]')?.textContent).toContain("Default: glob / wildcard");
  }
  const includeHelp = fields[0]!.querySelector('[role="tooltip"]')!.textContent;
  expect(includeHelp).toContain("Preview uses the last successful connection check");
  expect(includeHelp?.includes("Tables created later are not added automatically")).toBe(fixed);
  view.rerender(form("all"));
  expect(view.container.querySelector(".table-selection-toolbar")).toBe(toolbar);
  expect(toolbar.querySelector(".help, [title], [role=tooltip]")).toBeNull();
});

it("confirms an exact match below its row without changing the reserved result slot", async () => {
  const table = { namespace: "db", name: "events" };
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [table], excluded: [] }], issues: [] });
  const view = render(<TableCatalogContext.Provider value={{ tables: [table], preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.events" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  const row = view.getByLabelText("Table rule 1");
  const slot = row.querySelector(".table-rule-result")!;
  const following = view.getByRole("button", { name: "Add table rule" });
  const input = view.getByRole("combobox", { name: "Include rule 1" });
  expect(within(row).queryByText("Table found")).toBeNull();
  input.focus();
  await waitFor(() => expect(within(row).getByText("Table found")).toBeTruthy());
  expect(row.querySelector(".table-rule-result")).toBe(slot);
  expect(slot.getAttribute("aria-live")).toBe("polite");
  expect(view.getByRole("button", { name: "Add table rule" })).toBe(following);
  expect(document.activeElement).toBe(input);
  expect(within(row).queryByRole("button", { name: "Matched tables for rule 1" })).toBeNull();
});

it.each(["missing", "excluded", "multiple_includes", "include_exclude", "error"] as const)(
  "does not confirm an exact table when the result is %s", async kind => {
    const table = { namespace: "db", name: "events" };
    const response: SelectionPreview = {
      cards: [{ selected: kind === "missing" || kind === "excluded" ? [] : [table],
        excluded: kind === "excluded" ? [table] : [] }],
      issues: kind === "missing" || kind === "excluded" ? [{ kind: "empty_match", card: 0 }]
        : kind === "error" ? []
          : [{ kind: "conflict", table, first_card: 0, second_card: 1, conflict: kind }],
    };
    const preview = kind === "error" ? vi.fn().mockRejectedValue(new Error("Preview failed"))
      : vi.fn().mockResolvedValue(response);
    const view = render(<TableCatalogContext.Provider value={{ tables: [table], preview }}>
      <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.events" }] }} onChange={() => undefined} />
    </TableCatalogContext.Provider>);
    await waitFor(() => expect(view.getByRole("status").textContent).not.toBe(""));
    expect(view.queryByText("Table found")).toBeNull();
  },
);

it("removes exact-match confirmation immediately when the name or catalog changes", async () => {
  const table = { namespace: "db", name: "events" };
  const tables = [table];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const form = (include: string, catalog: typeof tables | undefined) =>
    <TableCatalogContext.Provider value={catalog ? { tables: catalog, preview } : undefined}>
      <TableSelectionEditor value={{ type: "selected", rules: [{ include }] }} onChange={() => undefined} />
    </TableCatalogContext.Provider>;
  const view = render(form("db.events", tables));
  await waitFor(() => expect(view.getByText("Table found")).toBeTruthy());
  view.rerender(form("db.other", tables));
  expect(view.queryByText("Table found")).toBeNull();
  view.rerender(form("db.events", tables));
  await waitFor(() => expect(view.getByText("Table found")).toBeTruthy());
  view.rerender(form("db.events", []));
  expect(view.queryByText("Table found")).toBeNull();
  view.rerender(form("db.events", undefined));
  expect(view.queryByText("Table found")).toBeNull();
});

it("leaves glob dots literal while preserving regex and identifier-boundary escaping", () => {
  const plain = { namespace: "schema", name: "table" };
  expect(exactPattern(plain, "glob")).toBe("schema.table");
  expect(exactPattern(plain, "regex")).toBe(String.raw`schema\.table`);
  expect(exactPattern({ namespace: "a.b", name: "c" }, "glob")).toBe(String.raw`a\\.b.c`);
  expect(exactPattern({ namespace: "a", name: "b.c" }, "glob")).toBe(String.raw`a.b\\.c`);
  expect(exactPattern({ namespace: "db", name: "a*b?c\\d" }, "glob")).toBe(String.raw`db.a\*b\?c\\\\d`);
});

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
  await waitFor(() => expect(view.getByRole("button", { name: "All matched tables 40" })).toBeTruthy());
  expect(view.queryByLabelText("All matched tables")).toBeNull();
  fireEvent.click(view.getByRole("button", { name: "All matched tables 40" }));
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
  expect(css).toMatch(/\.table-rule-matches\s*\{[^}]*min-height: 140px;[^}]*resize: none;/);
  expect(css).toMatch(/\.table-selection-footer\s*\{[^}]*height: var\(--control-height\);/);
  // Right anchoring isolates the action from changing match counts. Both
  // complete icon/label pairs retain their grid footprint, so toggling height
  // cannot move the button or leave the shorter visible pair off-center.
  expect(css).toMatch(/\.table-matches-height-toggle\s*\{[^}]*flex: 0 0 auto;[^}]*margin-left: auto;[^}]*height: 22px;/);
  expect(css).toMatch(/\.table-matches-height-toggle\s*\{[^}]*display: inline-grid;[^}]*place-items: center;/);
  expect(css).toMatch(/\.table-matches-height-content\s*\{[^}]*grid-area: 1 \/ 1;[^}]*display: inline-flex;[^}]*align-items: center;[^}]*white-space: nowrap;/);
  expect(css).toMatch(/\.table-matches-height-toggle,\s*\.table-matches-height-toggle \*\s*\{[^}]*scrollbar-gutter: auto;/);
  expect(css).toMatch(/\.table-matches-height-icon\s*\{[^}]*flex: 0 0 14px;[^}]*width: 14px;[^}]*min-width: 14px;[^}]*height: 14px;/);
  // Shared icons explicitly opt into visibility; this overlay must instead
  // inherit its hidden pair's state or both arrows will be painted at once.
  expect(css).toMatch(/\.table-matches-height-icon\s*\{[^}]*visibility: inherit;/);
  expect(css).toMatch(/\.table-matches-height-toggle:hover:not\(:disabled\)\s*\{[^}]*background: var\(--surface-hover\);/);
  expect(css).toMatch(/\.segmented-control\s*\{[^}]*grid-auto-columns: minmax\(max-content, 1fr\);/);
  expect(css).toMatch(/\.segmented-control > button\s*\{[^}]*min-width: max-content;/);
  expect(css).toMatch(/\.regex-toggle\s*\{[^}]*height: calc\(var\(--control-height\) - 8px\);/);
  expect(css).toMatch(/\.regex-toggle\[aria-pressed="true"\]\s*\{[^}]*background: var\(--blue\);[^}]*color: var\(--on-accent\);/);
  expect(css).toMatch(/\.table-rule-result\s*\{[^}]*height: 24px;/);
  expect(css).toMatch(/\.table-rule-found\s*\{[^}]*height: 100%;[^}]*white-space: nowrap;/);
  // Compact spacing must not remove the fixed match-status slot: typing a
  // wildcard or receiving a preview must not move the next row or Add button.
  expect(css).toMatch(/\.table-selection-editor\s*\{[^}]*gap: 4px;/);
  expect(css).toMatch(/\.table-rule-row\s*\{[^}]*gap: 0;/);
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
  await waitFor(() => expect(view.getByRole("button", { name: "All matched tables 3" })).toBeTruthy());
  expect(view.getAllByRole("button", { name: /^Matched tables for rule/ })).toHaveLength(2);
  fireEvent.click(view.getByRole("button", { name: "Matched tables for rule 1" }));
  expect([...view.getByLabelText("Matches for rule 1").children].map(child => child.textContent))
    .toEqual(["db.users", "db.reports"]);
  fireEvent.click(view.getByRole("button", { name: "All matched tables 3" }));
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

it.each(["rule", "selected-total", "all-total"] as const)(
  "shows all %s matches on one click and restores the previous height", async kind => {
    const tables = Array.from({ length: 40 }, (_, index) => ({ namespace: "db", name: `t${index}` }));
    const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
    const selection: JsonValue = kind === "all-total" ? { type: "all" }
      : { type: "selected", rules: [{ include: "db.*" }] };
    const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
      <TableSelectionEditor value={selection} onChange={() => undefined} />
    </TableCatalogContext.Provider>);
    await waitFor(() => expect(view.getByRole("button", { name: "All matched tables 40" })).toBeTruthy());
    expect(view.queryByRole("button", { name: "Show all", exact: true })).toBeNull();
    const disclosure = view.getByRole("button", {
      name: kind === "rule" ? "Matched tables for rule 1" : "All matched tables 40",
    });
    const header = disclosure.parentElement;
    fireEvent.click(disclosure);
    const region = view.getByLabelText(kind === "rule" ? "Matches for rule 1" : "All matched tables");
    Object.defineProperties(region, {
      scrollHeight: { configurable: true, value: 680 },
      offsetHeight: { configurable: true, value: 140 },
      clientHeight: { configurable: true, value: 138 },
    });
    const heightToggle = view.getByRole("button", { name: "Show all", exact: true });
    expect(heightToggle.classList.contains("table-matches-height-toggle")).toBe(true);
    expect(heightToggle.parentElement).toBe(header);
    expect(heightToggle.getAttribute("aria-controls")).toBe(region.id);
    expect(heightToggle.getAttribute("title")).toMatch(/all|expand|fit/i);
    const labels = heightToggle.querySelectorAll<HTMLElement>(".table-matches-height-content");
    expect([...labels].map(label => label.textContent)).toEqual(["Show all", "Restore height"]);
    expect([...labels].map(label => label.style.visibility)).toEqual(["visible", "hidden"]);
    expect(heightToggle.querySelector("svg")).toBeNull();
    for (const label of labels) {
      const icon = label.querySelector(".ui-icon.table-matches-height-icon");
      expect(icon?.getAttribute("aria-hidden")).toBe("true");
      expect(icon?.parentElement).toBe(label);
    }
    expect([...labels].map(label => label.getAttribute("aria-hidden"))).toEqual(["false", "true"]);
    expect(region.style.height).toBe("");
    heightToggle.focus();

    fireEvent.click(heightToggle);

    expect(region.style.height).toBe("682px");
    expect(view.getByRole("button", { name: "Restore height", exact: true })).toBe(heightToggle);
    expect([...labels].map(label => label.style.visibility)).toEqual(["hidden", "visible"]);
    expect([...labels].map(label => label.getAttribute("aria-hidden"))).toEqual(["true", "false"]);
    expect(document.activeElement).toBe(heightToggle);
    expect(view.getByRole("button", {
      name: kind === "rule" ? "Matched tables for rule 1" : "All matched tables 40",
    })).toBe(disclosure);
    expect(disclosure.parentElement).toBe(header);
    expect(header?.contains(heightToggle)).toBe(true);
    expect(heightToggle.getAttribute("title")).toMatch(/restore|previous/i);
    fireEvent.click(heightToggle);
    expect(region.style.height).toBe("");
    expect(view.getByRole("button", { name: "Show all", exact: true })).toBe(heightToggle);
  },
);

it.each(["rule", "selected-total", "all-total"] as const)(
  "preserves the explicitly expanded %s list height across catalog updates", async kind => {
    const oldTables = [{ namespace: "db", name: "old" }];
    const newTables = Array.from({ length: 40 }, (_, index) => ({ namespace: "db", name: `new${index}` }));
    let finish!: (result: SelectionPreview) => void;
    const preview = vi.fn()
      .mockResolvedValueOnce({ cards: [{ selected: oldTables, excluded: [] }], issues: [] })
      .mockImplementationOnce(() => new Promise<SelectionPreview>(resolve => { finish = resolve; }));
    const selection: JsonValue = kind === "all-total" ? { type: "all" }
      : { type: "selected", rules: [{ include: "db.*" }] };
    const form = (tables: typeof oldTables) => <TableCatalogContext.Provider value={{ tables, preview }}>
      <TableSelectionEditor value={selection} onChange={() => undefined} />
      <button type="button">Following control</button>
    </TableCatalogContext.Provider>;
    const view = render(form(oldTables));
    await waitFor(() => expect(view.getByRole("button", { name: "All matched tables 1" })).toBeTruthy());
    const disclosure = view.getByRole("button", {
      name: kind === "rule" ? "Matched tables for rule 1" : "All matched tables 1",
    });
    fireEvent.click(disclosure);
    const label = kind === "rule" ? "Matches for rule 1" : "All matched tables";
    const region = view.getByLabelText(label);
    const following = view.getByRole("button", { name: "Following control" });
    expect(region.classList.contains("table-rule-matches")).toBe(true);
    Object.defineProperties(region, {
      scrollHeight: { configurable: true, value: 298 },
      offsetHeight: { configurable: true, value: 140 },
      clientHeight: { configurable: true, value: 138 },
    });
    const heightToggle = view.getByRole("button", { name: "Show all", exact: true });
    fireEvent.click(heightToggle);
    expect(region.style.height).toBe("300px");
    following.focus();
    view.rerender(form(newTables));
    expect(view.getByLabelText(label)).toBe(region);
    expect(region.style.height).toBe("300px");
    expect(region.textContent).toBe("Waiting for a valid table selection…");
    expect(region.getAttribute("aria-busy")).toBe("true");
    expect(view.getByRole("button", { name: "Restore height", exact: true })).toBe(heightToggle);
    expect(document.activeElement).toBe(following);
    Object.defineProperty(region, "scrollHeight", { configurable: true, value: 1000 });
    await waitFor(() => expect(preview).toHaveBeenCalledTimes(2));
    await act(async () => finish({ cards: [{ selected: newTables, excluded: [] }], issues: [] }));
    expect(view.getByLabelText(label)).toBe(region);
    expect(region.style.height).toBe("300px");
    expect(region.textContent).toBe(newTables.map(qualifiedName).join(""));
    expect(view.getByRole("button", {
      name: kind === "rule" ? "Matched tables for rule 1" : "All matched tables 40",
    })).toBe(disclosure);
    expect(view.getByRole("button", { name: "Following control" })).toBe(following);
    expect(document.activeElement).toBe(following);
  },
);

it.each(["rule", "selected-total", "all-total"] as const)(
  "allows restoring the %s height but never fits pending matches", async kind => {
    const tables = [{ namespace: "db", name: "old" }];
    const newTables = [{ namespace: "db", name: "new" }];
    let finish!: (result: SelectionPreview) => void;
    const preview = vi.fn()
      .mockResolvedValueOnce({ cards: [{ selected: tables, excluded: [] }], issues: [] })
      .mockImplementationOnce(() => new Promise<SelectionPreview>(resolve => { finish = resolve; }));
    const selection: JsonValue = kind === "all-total" ? { type: "all" }
      : { type: "selected", rules: [{ include: "db.*" }] };
    const form = (catalog: typeof tables) => <TableCatalogContext.Provider value={{ tables: catalog, preview }}>
      <TableSelectionEditor value={selection} onChange={() => undefined} />
    </TableCatalogContext.Provider>;
    const view = render(form(tables));
    await waitFor(() => expect(view.getByRole("button", { name: "All matched tables 1" })).toBeTruthy());
    fireEvent.click(view.getByRole("button", {
      name: kind === "rule" ? "Matched tables for rule 1" : "All matched tables 1",
    }));
    const region = view.getByLabelText(kind === "rule" ? "Matches for rule 1" : "All matched tables");
    const heightToggle = view.getByRole("button", { name: "Show all", exact: true }) as HTMLButtonElement;
    const measure = vi.fn(() => 600);
    Object.defineProperties(region, {
      scrollHeight: { configurable: true, get: measure },
      offsetHeight: { configurable: true, value: 140 },
      clientHeight: { configurable: true, value: 138 },
    });
    fireEvent.click(heightToggle);
    expect(region.style.height).toBe("602px");
    measure.mockClear();
    heightToggle.focus();

    view.rerender(form(newTables));

    expect(view.getByRole("button", { name: "Restore height", exact: true })).toBe(heightToggle);
    expect(heightToggle.disabled).toBe(false);
    expect(document.activeElement).toBe(heightToggle);
    fireEvent.click(heightToggle);
    expect(view.getByRole("button", { name: "Show all", exact: true })).toBe(heightToggle);
    expect(heightToggle.disabled).toBe(true);
    fireEvent.click(heightToggle);
    expect(region.style.height).toBe("");
    expect(measure).not.toHaveBeenCalled();
    expect(region.textContent).toBe("Waiting for a valid table selection…");
    await waitFor(() => expect(preview).toHaveBeenCalledTimes(2));
    await act(async () => finish({ cards: [{ selected: newTables, excluded: [] }], issues: [] }));
    expect(heightToggle.disabled).toBe(false);
    expect(region.style.height).toBe("");
    expect(measure).not.toHaveBeenCalled();
    expect(region.textContent).toBe("db.new");
  },
);

it("does not shrink an existing taller height when all matches already fit", async () => {
  const tables = [{ namespace: "db", name: "table" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "all" }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("button", { name: "All matched tables 1" })).toBeTruthy());
  fireEvent.click(view.getByRole("button", { name: "All matched tables 1" }));
  const region = view.getByLabelText("All matched tables");
  region.style.height = "240px";
  Object.defineProperties(region, {
    scrollHeight: { configurable: true, value: 100 },
    offsetHeight: { configurable: true, value: 240 },
    clientHeight: { configurable: true, value: 238 },
  });

  fireEvent.click(view.getByRole("button", { name: "Show all", exact: true }));

  expect(region.style.height).toBe("240px");
  fireEvent.click(view.getByRole("button", { name: "Restore height", exact: true }));
  expect(region.style.height).toBe("240px");
});

it("sizes rule and total match lists independently and resets a reopened list", async () => {
  const tables = [{ namespace: "db", name: "first" }, { namespace: "db", name: "second" }];
  const preview = vi.fn().mockResolvedValue({
    cards: tables.map(table => ({ selected: [table], excluded: [] })), issues: [],
  });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.f*" }, { include: "db.s*" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("button", { name: "All matched tables 2" })).toBeTruthy());
  const disclosures = ["Matched tables for rule 1", "Matched tables for rule 2", "All matched tables 2"]
    .map(name => view.getByRole("button", { name }));
  disclosures.forEach(disclosure => fireEvent.click(disclosure));
  const regions = ["Matches for rule 1", "Matches for rule 2", "All matched tables"]
    .map(label => view.getByLabelText(label));
  const buttons = disclosures.map(disclosure => within(disclosure.parentElement!)
    .getByRole("button", { name: "Show all", exact: true }));
  regions.forEach((region, index) => Object.defineProperties(region, {
    scrollHeight: { configurable: true, value: 298 + index * 100 },
    offsetHeight: { configurable: true, value: 140 },
    clientHeight: { configurable: true, value: 138 },
  }));

  fireEvent.click(buttons[0]!);
  expect(regions.map(region => region.style.height)).toEqual(["300px", "", ""]);
  fireEvent.click(buttons[1]!);
  fireEvent.click(buttons[2]!);
  expect(regions.map(region => region.style.height)).toEqual(["300px", "400px", "500px"]);
  fireEvent.click(buttons[1]!);
  expect(regions.map(region => region.style.height)).toEqual(["300px", "", "500px"]);
  fireEvent.click(disclosures[0]!);
  expect(view.queryByLabelText("Matches for rule 1")).toBeNull();
  fireEvent.click(disclosures[0]!);
  expect(view.getByLabelText("Matches for rule 1").style.height).toBe("");
  expect(regions[2]!.style.height).toBe("500px");
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
  await waitFor(() => expect(view.getByRole("button", { name: "All matched tables 1" })).toBeTruthy());
  const toggle = view.getByRole("button", { name: "All matched tables 1" });
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
  expect(view.getByRole("button", { name: "All matched tables —" })).toBe(toggle);
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
  await waitFor(() => expect(view.getByRole("button", { name: "All matched tables 1" })).toBeTruthy());
  fireEvent.click(view.getByRole("button", { name: "All matched tables 1" }));
  expect(view.getByLabelText("All matched tables").textContent).toBe("db.verified_table");
  view.rerender(<TableCatalogContext.Provider value={undefined}>{editor}</TableCatalogContext.Provider>);
  expect(view.getByLabelText("All matched tables").textContent).toBe("Waiting for a valid table selection…");
  expect((view.getByRole("button", { name: "All matched tables —" }) as HTMLButtonElement).disabled).toBe(true);
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
  fireEvent.click(view.getByRole("button", { name: "All matched tables 2" }));
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
  expect(view.getByRole("button", { name: "All matched tables 1" })).toBeTruthy();
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

it.each([
  { connector: "clickhouse", hidden: ["system", "_system", "information_schema", "information_schema_extra", "INFORMATION_SCHEMA"],
    visible: ["default", "system_backup", "_system_backup", "my_information_schema", "INFORMATION_SCHEMA_extra", "System"] },
  { connector: "mysql", hidden: ["mysql", "information_schema", "performance_schema", "sys"],
    visible: ["reports", "mysql_backup", "information_schema_extra", "performance_schema_backup", "syslog"] },
  { connector: "postgres", hidden: ["pg_catalog", "pg_toast", "pg_temp_1", "pg_toast_temp_1", "information_schema"],
    visible: ["public", "reports", "pgreports", "information_schema_extra", "PG_CATALOG"] },
])("keeps the full $connector catalog and connection identity when hiding system tables", async ({ connector, hidden, visible }) => {
  const namespaces = [...hidden, ...visible];
  const tables = namespaces.map(namespace => ({ namespace, name: "t" }));
  const checkConnection = vi.fn().mockResolvedValue({ status: "verified", options: {}, tables });
  const api = { ...httpControlPlane, checkConnection };
  const config = { host: "first", hide_system_tables: true, tables: { type: "all" } };
  const hook = renderHook(({ config }) => useEndpointActions({ api, connector, role: "source", config }), { initialProps: { config } });
  await act(async () => { await hook.result.current.checkConnection(); });
  const identity = tableConnectionIdentity(connector, config);
  for (const hide_system_tables of [false, true, false]) {
    hook.rerender({ config: { ...config, hide_system_tables } });
    expect(hook.result.current.check.state).toBe("success");
    expect(hook.result.current.check).toMatchObject({ tables });
    expect(tableConnectionIdentity(connector, { ...config, hide_system_tables })).toBe(identity);
    expect(visibleTableCatalog(connector, hide_system_tables, tables).map(table => table.namespace))
      .toEqual(hide_system_tables ? visible : namespaces);
  }
  expect(checkConnection).toHaveBeenCalledTimes(1);
  hook.rerender({ config: { ...config, host: "second" } });
  expect(hook.result.current.check.state).toBe("idle");
  expect(visibleTableCatalog("other", true, tables)).toBe(tables);
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
  expect(view.getByText("Table found")).toBeTruthy();
});

it("maps invalid-pattern diagnostics back to the original draft row", async () => {
  const preview = vi.fn().mockRejectedValue(new Error("Invalid table rule at card index 0, Include: invalid regex"));
  const view = render(<TableCatalogContext.Provider value={{ tables: [], preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "" }, { include: "[", include_mode: "regex" }] }} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  await waitFor(() => expect(view.getByRole("status").textContent).toBe("Rule 2, Include: invalid regex"));
});
