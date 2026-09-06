// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, expect, it, vi } from "vitest";
import { TablePatternInput } from "../src/features/tableSelection/TablePatternInput";
import { completionPattern, literalPatternPrefix } from "../src/features/tableSelection/model";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import type { SelectionPreview } from "../src/generated/apiContract";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

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
  let finish!: (result: SelectionPreview) => void;
  const preview = vi.fn(() => new Promise<SelectionPreview>(resolve => { finish = resolve; }));
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
  const finish: ((result: SelectionPreview) => void)[] = [];
  const preview = vi.fn(() => new Promise<SelectionPreview>(resolve => finish.push(resolve)));
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
