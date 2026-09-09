// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, expect, it, vi } from "vitest";
import { TablePatternInput } from "../src/features/tableSelection/TablePatternInput";
import { completionPattern, exactPattern, literalPatternPrefix } from "../src/features/tableSelection/model";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import type { TableSelectionPreviewResult } from "../src/generated/apiContract";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

it("opens Include search immediately on empty focus and ranks subsequences from the cached catalog", () => {
  const tables = [
    { namespace: "system", name: "query_log" },
    { namespace: "public", name: "sql_queries" },
    { namespace: "sql", name: "events" },
    { namespace: "public", name: "orders" },
  ];
  const preview = vi.fn();
  function Field() {
    const [value, setValue] = useState("");
    return <div><TableCatalogContext.Provider value={{ tables, preview }}>
      <TablePatternInput id="search" label="Include" value={value} mode="glob" disabled={false} required invalid={false}
        searchSuggestions onChange={setValue} onModeChange={() => {}} />
    </TableCatalogContext.Provider><button>Following</button></div>;
  }
  const view = render(<Field />);
  const input = view.getByRole("combobox") as HTMLInputElement;
  const following = view.getByRole("button", { name: "Following" });
  act(() => input.focus());
  const menu = view.getByRole("listbox");
  expect(menu.getAttribute("aria-busy")).toBe("false");
  expect(menu.parentElement?.classList.contains("select-menu-floating")).toBe(true);
  expect(view.getAllByRole("option")).toHaveLength(4);
  for (const query of ["SQL", "ыйд"]) {
    fireEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent.input(input, { target: { value: query } });
    expect(input.hasAttribute("aria-activedescendant")).toBe(false);
    expect(view.getAllByRole("option").map(option => option.textContent))
      .toEqual(["sql.events", "public.sql_queries", "system.query_log"]);
    expect([...view.getByRole("option", { name: "system.query_log" }).querySelectorAll("strong")]
      .map(node => node.textContent).join("")).toBe("sql");
    expect(view.getByRole("listbox")).toBe(menu);
    expect(view.getByRole("combobox")).toBe(input);
    expect(document.activeElement).toBe(input);
    expect(view.getByRole("button", { name: "Following" })).toBe(following);
  }
  fireEvent.input(input, { target: { value: "no_such_table" } });
  expect(view.queryByRole("option")).toBeNull();
  expect(view.getByText("No matching tables")).toBeTruthy();
  fireEvent.input(input, { target: { value: "" } });
  expect(view.getAllByRole("option")).toHaveLength(4);
  expect(preview).not.toHaveBeenCalled();
});

it.each(["glob", "regex"] as const)("keeps a %s pattern unchanged until explicit selection and inserts the exact escaped table", mode => {
  const table = { namespace: "a.b", name: "reports*?" };
  const preview = vi.fn(), onChange = vi.fn();
  const value = mode === "glob" ? "*" : ".*";
  const view = render(<TableCatalogContext.Provider value={{ tables: [table], preview }}>
    <TablePatternInput id="search" label="Include" value={value} mode={mode} disabled={false} required invalid={false}
      searchSuggestions onChange={onChange} onModeChange={() => {}} />
  </TableCatalogContext.Provider>);
  const input = view.getByRole("combobox") as HTMLInputElement;
  act(() => input.focus());
  expect(view.getAllByRole("option")).toHaveLength(1);
  expect(onChange).not.toHaveBeenCalled();
  fireEvent.keyDown(input, { key: "Enter" });
  expect(input.value).toBe(value);
  expect(onChange).not.toHaveBeenCalled();
  act(() => input.focus());
  fireEvent.keyDown(input, { key: "ArrowDown" });
  fireEvent.keyDown(input, { key: "Enter" });
  expect(onChange).toHaveBeenCalledExactlyOnceWith(exactPattern(table, mode));
  expect(view.queryByRole("listbox")).toBeNull();
  expect(document.activeElement).toBe(input);
  expect(preview).not.toHaveBeenCalled();
});

it("reopens Include search on click, and closes on Escape or Tab without silently completing text", () => {
  const onChange = vi.fn();
  const view = render(<TableCatalogContext.Provider value={{ tables: [{ namespace: "db", name: "events" }], preview: vi.fn() }}>
    <TablePatternInput id="search" label="Include" value="ev" mode="glob" disabled={false} required invalid={false}
      searchSuggestions onChange={onChange} onModeChange={() => {}} />
  </TableCatalogContext.Provider>);
  const input = view.getByRole("combobox");
  act(() => input.focus());
  fireEvent.keyDown(input, { key: "Escape" });
  expect(view.queryByRole("listbox")).toBeNull();
  expect(document.activeElement).toBe(input);
  fireEvent.click(input);
  expect(view.getByRole("option")).toBeTruthy();
  fireEvent.keyDown(input, { key: "Tab" });
  expect(view.queryByRole("listbox")).toBeNull();
  expect(onChange).not.toHaveBeenCalled();
});

it("clears stale Include search options immediately when the catalog changes and never offers disabled edits", () => {
  const preview = vi.fn(), onChange = vi.fn();
  const component = (name: string, disabled = false) => <TableCatalogContext.Provider value={{ tables: [{ namespace: "db", name }], preview }}>
    <TablePatternInput id="search" label="Include" value="" mode="glob" disabled={disabled} required invalid={false}
      searchSuggestions onChange={onChange} onModeChange={() => {}} />
  </TableCatalogContext.Provider>;
  const view = render(component("old"));
  act(() => view.getByRole("combobox").focus());
  expect(view.getByRole("option", { name: "db.old" })).toBeTruthy();
  fireEvent.keyDown(view.getByRole("combobox"), { key: "ArrowDown" });
  view.rerender(component("new"));
  expect(view.queryByRole("option", { name: "db.old" })).toBeNull();
  expect(view.getByRole("option", { name: "db.new" })).toBeTruthy();
  expect(view.getByRole("combobox").hasAttribute("aria-activedescendant")).toBe(false);
  fireEvent.keyDown(view.getByRole("combobox"), { key: "Enter" });
  expect(onChange).not.toHaveBeenCalled();
  act(() => view.getByRole("combobox").focus());
  view.rerender(component("new", true));
  expect(view.queryByRole("listbox")).toBeNull();
  expect(preview).not.toHaveBeenCalled();
});

it.each([false, true])("shows an immediate, layout-free full value only when the field is truncated (%s)", truncated => {
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue({ measureText: () => ({ width: truncated ? 400 : 20 }), font: "" } as unknown as CanvasRenderingContext2D);
  const value = "schema.a_very_long_table_name";
  const view = render(<div><TablePatternInput id="overflow" label="Include" value={value} mode="glob" disabled={false}
    required invalid={false} onChange={vi.fn()} onModeChange={vi.fn()} /><button>Following</button></div>);
  const input = view.getByRole("textbox");
  Object.defineProperty(input, "clientWidth", { value: 100 });
  const following = view.getByRole("button", { name: "Following" });
  fireEvent.mouseEnter(input);
  const tooltip = view.queryByRole("tooltip");
  if (truncated) {
    expect(tooltip?.textContent).toBe(value);
    expect(tooltip?.parentElement).toBe(document.body);
    expect(input.getAttribute("aria-describedby")).toBe(tooltip?.id);
  } else expect(tooltip).toBeNull();
  expect(input.hasAttribute("title")).toBe(false);
  expect(view.getByRole("button", { name: "Following" })).toBe(following);
  fireEvent.mouseLeave(input);
  expect(view.queryByRole("tooltip")).toBeNull();
});

it("uses prefix completions for plain glob input and preserves authored patterns", () => {
  expect(completionPattern("schema", "glob")).toBe("schema*");
  expect(completionPattern("schema*", "glob")).toBe("schema*");
  expect(completionPattern("schema.?", "glob")).toBe("schema.?");
  expect(completionPattern("schema\\*", "glob")).toBe("schema\\**");
  expect(completionPattern("schema.*", "regex")).toBe("schema.*");
  expect(completionPattern("schema", "regex")).toBe("schema");
  expect(literalPatternPrefix("schema\\.reports*", "glob")).toBe("schema.reports");
});

it("shows anchored server suggestions without native datalist and selects exact names by explicit click", async () => {
  const tables = [{ namespace: "schema", name: "reports" }, { namespace: "information_schema", name: "reports" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [tables[0]], excluded: [] }], issues: [] });
  function Field() {
    const [value, setValue] = useState("");
    return <TableCatalogContext.Provider value={{ tables, preview }}>
      <TablePatternInput id="include" label="Include rule 1" value={value} mode="glob" disabled={false} required invalid={false}
        onChange={setValue} onModeChange={() => undefined} />
    </TableCatalogContext.Provider>;
  }
  const view = render(<Field />);
  const input = view.getByRole("combobox");
  fireEvent.input(input, { target: { value: "schema" } });
  expect(view.getByRole("listbox").getAttribute("aria-busy")).toBe("true");
  expect(view.queryByRole("option")).toBeNull();
  await waitFor(() => expect(view.getByRole("option", { name: "schema.reports" })).toBeTruthy());
  expect(preview.mock.lastCall?.[0].selection.rules[0]).toEqual({ include: "schema*", include_mode: "glob" });
  expect(view.queryByRole("option", { name: "information_schema.reports" })).toBeNull();
  expect(view.getByRole("option").querySelector("strong")?.textContent).toBe("schema");
  expect(input.hasAttribute("list")).toBe(false);
  expect(view.container.querySelector("datalist")).toBeNull();
  fireEvent.click(view.getByRole("option", { name: "schema.reports" }));
  expect((input as HTMLInputElement).value).toBe("schema.reports");
  expect(input.getAttribute("aria-expanded")).toBe("false");
});

const fields = [{ label: "Include rule 1", required: true }, { label: "Exclude rule 1", required: false }];

it.each(fields.flatMap(field => (["glob", "regex"] as const).map(mode => ({ ...field, mode }))))(
  "Enter finishes $label in $mode mode without accepting the highlighted suggestion", async ({ label, required, mode }) => {
    const tables = [{ namespace: "system", name: "query_log" }];
    const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
    const onChange = vi.fn();
    const onKeyDown = vi.fn();
    const value = mode === "glob" ? "system*" : "system.*";
    const view = render(<div onKeyDown={onKeyDown}>
      <TableCatalogContext.Provider value={{ tables, preview }}>
        <TablePatternInput id="pattern" label={label} value={value} mode={mode} disabled={false} required={required} invalid={false}
          onChange={onChange} onModeChange={() => undefined} />
      </TableCatalogContext.Provider>
      <button type="button">Following control</button>
    </div>);
    const input = view.getByRole("combobox") as HTMLInputElement;
    const following = view.getByRole("button", { name: "Following control" });
    act(() => input.focus());
    await waitFor(() => expect(view.getByRole("option")).toBeTruthy());
    fireEvent.keyDown(input, { key: "ArrowDown" });
    expect(input.getAttribute("aria-activedescendant")).toBe("pattern-suggestion-0");
    onKeyDown.mockClear();
    const enter = new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true });
    fireEvent(input, enter);
    expect(enter.defaultPrevented).toBe(true);
    expect(onKeyDown).not.toHaveBeenCalled();
    expect(document.activeElement).toBe(document.body);
    expect(input.value).toBe(value);
    expect(onChange).not.toHaveBeenCalled();
    expect(input.getAttribute("aria-expanded")).toBe("false");
    expect(input.hasAttribute("aria-activedescendant")).toBe(false);
    expect(view.queryByRole("listbox")).toBeNull();
    expect(view.getByRole("button", { name: "Following control" })).toBe(following);
  },
);

it.each(fields)("Enter finishes $label while suggestions are loading and a late response cannot reopen them", async ({ label, required }) => {
  const tables = [{ namespace: "system", name: "query_log" }];
  let finish!: (result: TableSelectionPreviewResult) => void;
  const preview = vi.fn(() => new Promise<TableSelectionPreviewResult>(resolve => { finish = resolve; }));
  const onChange = vi.fn();
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TablePatternInput id="pattern" label={label} value="system*" mode="glob" disabled={false} required={required} invalid={false}
      onChange={onChange} onModeChange={() => undefined} />
  </TableCatalogContext.Provider>);
  const input = view.getByRole("combobox") as HTMLInputElement;
  act(() => input.focus());
  await waitFor(() => expect(preview).toHaveBeenCalledOnce());
  expect(view.getByRole("listbox").getAttribute("aria-busy")).toBe("true");
  fireEvent.keyDown(input, { key: "Enter" });
  expect(document.activeElement).toBe(document.body);
  expect(view.queryByRole("listbox")).toBeNull();
  await act(async () => finish({ cards: [{ selected: tables, excluded: [] }], issues: [] }));
  expect(view.queryByRole("listbox")).toBeNull();
  expect(input.value).toBe("system*");
  expect(onChange).not.toHaveBeenCalled();
});

it.each(fields)("Enter also finishes an empty $label with no suggestion menu", ({ label, required }) => {
  const view = render(<TablePatternInput id="pattern" label={label} value="" mode="glob" disabled={false} required={required} invalid={false}
    onChange={() => undefined} onModeChange={() => undefined} />);
  const input = view.getByRole("textbox");
  act(() => input.focus());
  expect(view.queryByRole("listbox")).toBeNull();
  expect(fireEvent.keyDown(input, { key: "Enter" })).toBe(false);
  expect(document.activeElement).toBe(document.body);
});

it.each(fields)("Enter does not finish $label during IME composition", ({ label, required }) => {
  const onChange = vi.fn();
  const view = render(<TablePatternInput id="pattern" label={label} value="system*" mode="glob" disabled={false} required={required} invalid={false}
    onChange={onChange} onModeChange={() => undefined} />);
  const input = view.getByRole("textbox");
  act(() => input.focus());
  expect(fireEvent.keyDown(input, { key: "Enter", isComposing: true })).toBe(true);
  expect(document.activeElement).toBe(input);
  expect(input.hasAttribute("aria-expanded")).toBe(false);
  expect(onChange).not.toHaveBeenCalled();
});

it("closes suggestions and restores input focus when Escape is pressed on the regex button", async () => {
  const tables = [{ namespace: "schema", name: "reports" }];
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TablePatternInput id="include" label="Include rule 1" value="schema" mode="glob" disabled={false} required invalid={false}
      onChange={() => undefined} onModeChange={() => undefined} />
  </TableCatalogContext.Provider>);
  const input = view.getByRole("combobox");
  act(() => input.focus());
  await waitFor(() => expect(view.getByRole("option")).toBeTruthy());
  const regex = view.getByRole("button", { name: "include regex rule 1" });
  act(() => regex.focus());
  fireEvent.keyDown(regex, { key: "Escape" });
  expect(document.activeElement).toBe(input);
  expect(input.getAttribute("aria-expanded")).toBe("false");
  expect(view.queryByRole("listbox")).toBeNull();
});

it("never displays a stale suggestion response after the pattern changes", async () => {
  const tables = [{ namespace: "schema", name: "old" }];
  const finish: ((result: TableSelectionPreviewResult) => void)[] = [];
  const preview = vi.fn(() => new Promise<TableSelectionPreviewResult>(resolve => finish.push(resolve)));
  const component = (value: string) => <TableCatalogContext.Provider value={{ tables, preview }}>
    <TablePatternInput id="include" label="Include rule 1" value={value} mode="glob" disabled={false} required invalid={false}
      onChange={() => undefined} onModeChange={() => undefined} />
  </TableCatalogContext.Provider>;
  const view = render(component("old"));
  act(() => view.getByRole("combobox").focus());
  await waitFor(() => expect(preview).toHaveBeenCalledTimes(1));
  view.rerender(component("new"));
  await act(async () => finish[0]!({ cards: [{ selected: tables, excluded: [] }], issues: [] }));
  expect(view.queryByRole("option")).toBeNull();
});
