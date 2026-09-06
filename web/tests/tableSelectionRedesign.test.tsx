// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, expect, it, vi } from "vitest";
import { TableSelectionEditor } from "../src/features/tableSelection/TableSelectionEditor";
import { exactPattern, qualifiedName } from "../src/features/tableSelection/model";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import type { SelectionPreview } from "../src/generated/apiContract";
import type { JsonValue } from "../src/json";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });
const table = { namespace: "a.b", name: "reports*?" };
const tables = [table, { namespace: "db", name: "other" }];

it("closes an open picker on metadata invalidation and does not reopen it on reconnect", () => {
  const preview = vi.fn().mockResolvedValue({ cards: [], issues: [] }), onChange = vi.fn();
  const form = (known: boolean, disabled = false) => <TableCatalogContext.Provider value={known ? { tables, preview } : undefined}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "old" }] }} disabled={disabled} onChange={onChange} />
  </TableCatalogContext.Provider>;
  const view = render(form(true));
  const browse = view.getByRole("button", { name: "Browse tables for Include rule 1" }) as HTMLButtonElement;
  fireEvent.click(browse);
  expect(view.getByRole("dialog")).toBeTruthy();
  view.rerender(form(false));
  expect(view.queryByRole("dialog")).toBeNull();
  expect(browse.disabled).toBe(true);
  view.rerender(form(true));
  expect(view.queryByRole("dialog")).toBeNull();
  fireEvent.click(browse);
  view.rerender(form(true, true));
  expect(view.queryByRole("dialog")).toBeNull();
  expect(onChange).not.toHaveBeenCalled();
});

it.each(["glob", "regex"] as const)("Use from Include's magnifier preserves %s mode, Exclude and other rules", async mode => {
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  let value: JsonValue;
  function Form() {
    const [selection, setSelection] = useState<JsonValue>({ type: "selected", rules: [
      { include: "old", include_mode: mode, exclude: "keep*", exclude_mode: "glob" }, { include: "untouched" },
    ] });
    value = selection;
    return <TableCatalogContext.Provider value={{ tables, preview }}>
      <TableSelectionEditor value={selection} onChange={setSelection} />
    </TableCatalogContext.Provider>;
  }
  const view = render(<Form />);
  const input = view.getByRole("combobox", { name: "Include rule 1" });
  const browse = view.getByRole("button", { name: "Browse tables for Include rule 1" });
  const regex = view.getByRole("button", { name: "include regex rule 1" });
  expect(browse.compareDocumentPosition(regex) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
  browse.focus();
  fireEvent.click(browse);
  const dialog = view.getByRole("dialog");
  expect(document.activeElement).toBe(within(dialog).getByRole("textbox", { name: "Search tables" }));
  fireEvent.click(within(dialog).getByRole("button", { name: "Search tables regex" }));
  fireEvent.click(within(dialog).getByRole("button", { name: `Use ${qualifiedName(table)} in Include` }));
  expect(view.queryByRole("dialog")).toBeNull();
  expect((input as HTMLInputElement).value).toBe(exactPattern(table, mode));
  expect(value!).toEqual({ type: "selected", rules: [
    { include: exactPattern(table, mode), include_mode: mode, exclude: "keep*", exclude_mode: "glob" }, { include: "untouched" },
  ] });
  expect(document.activeElement).toBe(browse);
  expect(view.queryByRole("listbox")).toBeNull();
});

it("has no exact-name result row and confirms it inside the input without moving following controls", async () => {
  const selected = [{ namespace: "db", name: "events" }];
  let finish!: (value: SelectionPreview) => void;
  const preview = vi.fn(() => new Promise<SelectionPreview>(resolve => { finish = resolve; }));
  const view = render(<TableCatalogContext.Provider value={{ tables: selected, preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "db.events" }] }} onChange={vi.fn()} />
  </TableCatalogContext.Provider>);
  const row = view.getByLabelText("Table rule 1");
  const following = view.getByRole("button", { name: "Add tables" });
  const input = view.getByRole("combobox", { name: "Include rule 1" });
  const slot = row.querySelector(".table-pattern-confirmation");
  expect(slot).toBeTruthy();
  expect(row.querySelector(".table-rule-result")?.textContent).toBe("");
  await waitFor(() => expect(preview).toHaveBeenCalled());
  await act(async () => finish({ cards: [{ selected, excluded: [] }], issues: [] }));
  expect(row.querySelector(".table-pattern-confirmation")).toBe(slot);
  expect(within(row).getByLabelText("Table found")).toBeTruthy();
  expect(within(row).queryByRole("button", { name: /Matched tables/ })).toBeNull();
  expect(view.getByRole("combobox", { name: "Include rule 1" })).toBe(input);
  expect(view.getByRole("button", { name: "Add tables" })).toBe(following);
});

it("uses the shared frameless Copy with immediate deduplicated clipboard feedback in matched rows", async () => {
  let finish!: () => void;
  const writeText = vi.fn(() => new Promise<void>(resolve => { finish = resolve; }));
  vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: tables, excluded: [] }], issues: [] });
  const view = render(<TableCatalogContext.Provider value={{ tables, preview }}>
    <TableSelectionEditor value={{ type: "selected", rules: [{ include: "*" }] }} onChange={vi.fn()} />
  </TableCatalogContext.Provider>);
  const toggle = view.getByRole("button", { name: "Matched tables for rule 1" }) as HTMLButtonElement;
  await waitFor(() => expect(toggle.disabled).toBe(false));
  fireEvent.click(toggle);
  const region = view.getByRole("region", { name: "Matches for rule 1" });
  const copy = within(region).getByRole("button", { name: `Copy ${qualifiedName(table)}` });
  expect(copy.classList.contains("copy-action")).toBe(true);
  expect(copy.classList.contains("copy-action-framed")).toBe(false);
  fireEvent.click(copy);
  fireEvent.click(copy);
  expect(copy.getAttribute("aria-busy")).toBe("true");
  expect(writeText).toHaveBeenCalledExactlyOnceWith(qualifiedName(table));
  await act(async () => finish());
  expect(copy.querySelector(".copy-icon-check")).toBeTruthy();
  expect(view.getByRole("tooltip", { name: "Copied" }).textContent).toBe("Copied");
  expect(view.getByRole("region", { name: "Matches for rule 1" })).toBe(region);
});
