// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, expect, it, vi } from "vitest";
import { TablePatternInput } from "../src/features/tableSelection/TablePatternInput";
import { completionPattern, literalPatternPrefix } from "../src/features/tableSelection/model";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import type { SelectionPreview } from "../src/generated/apiContract";
import { render } from "./support/render";

afterEach(cleanup);

it("uses prefix completions for plain glob input and preserves authored patterns", () => {
  expect(completionPattern("schema", "glob")).toBe("schema*");
  expect(completionPattern("schema*", "glob")).toBe("schema*");
  expect(completionPattern("schema.?", "glob")).toBe("schema.?");
  expect(completionPattern("schema\\*", "glob")).toBe("schema\\**");
  expect(completionPattern("schema.*", "regex")).toBe("schema.*");
  expect(completionPattern("schema", "regex")).toBe("schema");
  expect(literalPatternPrefix("schema\\.reports*", "glob")).toBe("schema.reports");
});

it("shows anchored server suggestions without native datalist and selects exact names by keyboard", async () => {
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
  fireEvent.keyDown(input, { key: "ArrowDown" });
  fireEvent.keyDown(input, { key: "Enter" });
  expect((input as HTMLInputElement).value).toBe("schema\\.reports");
  expect(input.getAttribute("aria-expanded")).toBe("false");
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
  fireEvent.focus(view.getByRole("combobox"));
  await waitFor(() => expect(preview).toHaveBeenCalledTimes(1));
  view.rerender(component("new"));
  await act(async () => finish[0]!({ cards: [{ selected: tables, excluded: [] }], issues: [] }));
  expect(view.queryByRole("option")).toBeNull();
});
