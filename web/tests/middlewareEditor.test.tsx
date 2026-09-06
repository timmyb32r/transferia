// @vitest-environment jsdom

import { cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MiddlewareEditor } from "../src/features/middleware/MiddlewareEditor";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import type { JsonValue } from "../src/types";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const step = {
  tables: { include: "public.reports_*", exclude: "public.reports_test*", include_mode: "glob", exclude_mode: "glob" },
  datafusion: { sql: "SELECT id FROM input" },
};

function Editor({ value = [step], disabled = false }: { value?: JsonValue; disabled?: boolean }) {
  const [entries, setEntries] = useState(value);
  return <MiddlewareEditor value={entries} disabled={disabled} onChange={setEntries} />;
}

describe("ordered transform strips", () => {
  it("shows both Include and Exclude in the collapsed strip", () => {
    const view = render(<Editor />);
    const scope = view.container.querySelector(".middleware-scope-summary")!;
    expect(scope.textContent).toContain("Include: public.reports_*");
    expect(scope.textContent).toContain("Exclude: public.reports_test*");
    expect(view.queryByRole("textbox")).toBeNull();
  });
  it("omits the Include prefix when there is no Exclude", () => {
    const view = render(<Editor value={[{ ...step, tables: { include: "public.reports_*" } }]} />);
    expect(view.container.querySelector(".middleware-scope-summary")?.textContent).toBe("public.reports_*");
  });
  it("shows available tables in a dialog and shared matched tables below the scope", async () => {
    const selected = { namespace: "public", name: "reports_daily" };
    const excluded = { namespace: "public", name: "reports_test" };
    const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [selected], excluded: [excluded] }], issues: [] });
    const view = render(<TableCatalogContext.Provider value={{ tables: [selected, excluded], preview }}><Editor /></TableCatalogContext.Provider>);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    const available = view.getByRole("button", { name: "Available tables for transform 1" });
    const matched = view.getByRole("button", { name: "Matched tables for transform 1" });
    expect(available.textContent).toBe("Available tables (2)");
    expect(available.getAttribute("aria-haspopup")).toBe("dialog");
    await waitFor(() => expect(matched.textContent).toContain("1"));
    expect(preview).toHaveBeenCalledWith({ catalog: [selected, excluded], selection: { type: "selected", rules: [step.tables] } }, expect.any(AbortSignal));
    fireEvent.click(available);
    const dialog = view.getByRole("dialog", { name: "Available tables" });
    expect(within(dialog).getByRole("textbox", { name: "Search tables" })).toBeTruthy();
    expect(within(dialog).getByText("public.reports_test")).toBeTruthy();
    fireEvent.click(within(dialog).getByRole("button", { name: "Close available tables" }));
    fireEvent.click(matched);
    const list = view.getByRole("region", { name: "Matched tables for transform 1" });
    expect(list.classList.contains("table-rule-matches")).toBe(true);
    expect(within(list).getByText("public.reports_daily")).toBeTruthy();
    expect(within(list).queryByText("public.reports_test")).toBeNull();
    expect(view.getAllByRole("button", { name: "Show all" })).toHaveLength(1);
  });

  it.each(["Include", "Exclude"])("reuses source suggestions in %s for existing and newly added steps", async label => {
    const table = { namespace: "public", name: "reports_daily" };
    const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [table], excluded: [] }], issues: [] });
    const view = render(<TableCatalogContext.Provider value={{ tables: [table], preview }}><Editor /></TableCatalogContext.Provider>);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    for (const index of [0, 1]) {
      if (index === 1) fireEvent.click(view.getByRole("button", { name: "Add transform" }));
      const strip = view.getAllByRole("article")[index]!;
      const input = within(strip).getByRole("combobox", { name: label });
      fireEvent.input(input, { target: { value: "public.rep" } });
      const suggestion = await within(strip).findByRole("option", { name: "public.reports_daily" });
      fireEvent.click(suggestion);
      expect((input as HTMLInputElement).value).toBe("public.reports_daily");
    }
  });

  it("shows zero matched transforms as a skipped step, not a failing source rule", async () => {
    const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [], excluded: [] }], issues: [{ kind: "empty_match", card: 0 }] });
    const view = render(<TableCatalogContext.Provider value={{ tables: [], preview }}><Editor /></TableCatalogContext.Provider>);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    await waitFor(() => expect(view.getByRole("button", { name: "Matched tables for transform 1" }).textContent).toContain("0"));
    expect(view.queryByText(/selects no tables|Rule 1/)).toBeNull();
  });

  it("keeps settings and preview collapsed until explicitly opened", () => {
    const view = render(<Editor />);
    const toggle = view.getByRole("button", { name: "Expand transform 1" });
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(view.queryByDisplayValue("SELECT id FROM input")).toBeNull();
    fireEvent.click(toggle);
    expect(view.getByDisplayValue("SELECT id FROM input")).toBeTruthy();
    expect(view.getByRole("button", { name: "Preview transform 1" }).getAttribute("aria-expanded")).toBe("false");
    expect(view.queryByRole("button", { name: "Run preview" })).toBeNull();
  });

  it("adds one SQL step scoped to all tables", () => {
    const onChange = vi.fn();
    const view = render(<MiddlewareEditor value={[]} disabled={false} onChange={onChange} />);
    fireEvent.click(view.getByRole("button", { name: "Add transform" }));
    expect(onChange).toHaveBeenCalledWith([{
      tables: { include: "*", include_mode: "glob", exclude_mode: "glob" },
      datafusion: { sql: "SELECT * FROM input" },
    }]);
  });

  it("clones the complete step without opening either strip", () => {
    const onChange = vi.fn();
    const view = render(<MiddlewareEditor value={[step]} disabled={false} onChange={onChange} />);
    fireEvent.click(view.getByRole("button", { name: "Clone transform 1" }));
    const next = onChange.mock.calls[0]?.[0] as JsonValue[];
    expect(next).toEqual([step, step]);
    expect(next[1]).not.toBe(step);
    expect(view.queryByDisplayValue("SELECT id FROM input")).toBeNull();
  });

  it("edits Include and Exclude independently without changing SQL", () => {
    const onChange = vi.fn();
    const view = render(<MiddlewareEditor value={[step]} disabled={false} onChange={onChange} />);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    fireEvent.input(view.getByRole("textbox", { name: "Include" }), { target: { value: "public.events*" } });
    expect(onChange).toHaveBeenLastCalledWith([{ ...step, tables: { ...step.tables, include: "public.events*" } }]);
    fireEvent.click(view.getByRole("button", { name: "Exclude regex" }));
    expect(onChange).toHaveBeenLastCalledWith([{ ...step, tables: { ...step.tables, exclude_mode: "regex" } }]);
  });

  it("keeps open state attached to the original step after reordering", () => {
    const view = render(<Editor value={[step, { tables: { include: "*" }, filter: { field: "country", value: "DE" } }]} />);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    fireEvent.dragStart(view.getByRole("button", { name: "Reorder transform 1" }));
    fireEvent.drop(view.getAllByRole("article")[1]!);
    const strips = view.getAllByRole("article");
    expect(within(strips[0]!).queryByRole("textbox")).toBeNull();
    expect(within(strips[1]!).getByDisplayValue("SELECT id FROM input")).toBeTruthy();
  });

  it("uses the drag handle instead of duplicate up/down buttons", () => {
    const view = render(<Editor />);
    expect(view.getByRole("button", { name: "Reorder transform 1" }).getAttribute("draggable")).toBe("true");
    expect(view.queryByRole("button", { name: /Move transform/ })).toBeNull();
    expect(view.queryByRole("button", { name: /settings for transform/ })).toBeNull();
    expect(view.getByRole("button", { name: "Reorder transform 1" }).title).not.toContain("Alt");
  });

  it("requires confirmation before deleting a step", () => {
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(false);
    const view = render(<Editor />);
    fireEvent.click(view.getByRole("button", { name: "Delete transform 1" }));
    expect(confirm).toHaveBeenCalledOnce();
    expect(view.getAllByRole("article")).toHaveLength(1);
    confirm.mockReturnValue(true);
    fireEvent.click(view.getByRole("button", { name: "Delete transform 1" }));
    expect(view.queryByRole("article")).toBeNull();
  });

  it("allows inspecting a read-only step but no configuration mutations", () => {
    const onChange = vi.fn();
    const view = render(<MiddlewareEditor value={[step]} disabled onChange={onChange} />);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    expect((view.getByDisplayValue("SELECT id FROM input") as HTMLTextAreaElement).disabled).toBe(true);
    for (const name of ["Clone transform 1", "Delete transform 1", "Add transform"]) {
      const button = view.getByRole("button", { name }) as HTMLButtonElement;
      expect(button.disabled).toBe(true);
      fireEvent.click(button);
    }
    expect(onChange).not.toHaveBeenCalled();
  });
});
