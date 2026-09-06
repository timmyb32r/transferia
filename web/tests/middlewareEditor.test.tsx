// @vitest-environment jsdom

import { cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MiddlewareEditor } from "../src/features/middleware/MiddlewareEditor";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import { nextRequiredTarget, REQUIRED_CONTROL_SELECTOR } from "../src/ui/requiredGuidance";
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
  it("shares the source rule controls, including magnifier, optional Exclude and exact Use", () => {
    const table = { namespace: "analytics", name: "reports" };
    const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [table], excluded: [] }], issues: [] });
    const view = render(<TableCatalogContext.Provider value={{ tables: [table], preview }}><Editor value={[]} /></TableCatalogContext.Provider>);
    fireEvent.click(view.getByRole("button", { name: "Add transform" }));
    expect(view.queryByLabelText("Exclude transform 1")).toBeNull();
    const browse = view.getByRole("button", { name: "Browse tables for Include transform 1" });
    fireEvent.click(browse);
    fireEvent.click(view.getByRole("button", { name: "Use analytics.reports in Include" }));
    expect((view.getByLabelText("Include transform 1") as HTMLInputElement).value).toBe("analytics.reports");
    expect(view.queryByRole("dialog")).toBeNull();
    fireEvent.click(view.getByRole("button", { name: "Add Exclude for transform 1" }));
    const exclude = view.getByLabelText("Exclude transform 1");
    expect(document.activeElement).toBe(exclude);
    expect(exclude.closest(".table-rule-patterns")).toBe(view.getByLabelText("Include transform 1").closest(".table-rule-patterns"));
  });
  it.each([
    ["glob", "reports_daily", "analytics.reports_daily"],
    ["regex", "reports_daily", String.raw`analytics\.reports_daily`],
    ["glob", "reports_*+(1)", String.raw`analytics.reports_\*+(1)`],
    ["regex", "reports_*+(1)", String.raw`analytics\.reports_\*\+\(1\)`],
  ])("uses an exact table in the current Include's %s mode: %s", (mode, name, expected) => {
    const table = { namespace: "analytics", name: name! };
    const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [table], excluded: [] }], issues: [] });
    const view = render(<TableCatalogContext.Provider value={{ tables: [table], preview }}>
      <Editor value={[step, { ...step, tables: { ...step.tables, include_mode: mode! } }]} />
    </TableCatalogContext.Provider>);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 2" }));
    const include = view.getByRole("combobox", { name: "Include transform 2" }) as HTMLInputElement;
    const available = view.getByRole("button", { name: "Available tables for transform 2" });
    available.focus();
    fireEvent.click(available);
    // Search mode is deliberately the opposite of Include mode.
    if (mode === "glob") fireEvent.click(view.getByRole("button", { name: "Search tables regex" }));
    fireEvent.click(view.getByRole("button", { name: `Use analytics.${name} in Include` }));
    expect(view.queryByRole("dialog", { name: "Available tables" })).toBeNull();
    expect(include.value).toBe(expected);
    expect(view.getByRole("combobox", { name: "Include transform 2" })).toBe(include);
    expect(document.activeElement).toBe(available);
    expect(view.getByRole("button", { name: "Include transform 2 regex" }).getAttribute("aria-pressed")).toBe(String(mode === "regex"));
    expect((view.getByRole("combobox", { name: "Exclude transform 2" }) as HTMLInputElement).value).toBe(step.tables.exclude);
    expect(view.getByDisplayValue(step.datafusion.sql)).toBeTruthy();
    expect(view.container.querySelector(".middleware-scope-summary")?.textContent).toContain(step.tables.include);
  });

  it("uses a table in a newly added transform", () => {
    const table = { namespace: "system", name: "query_log" };
    const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [table], excluded: [] }], issues: [] });
    const view = render(<TableCatalogContext.Provider value={{ tables: [table], preview }}><Editor value={[]} /></TableCatalogContext.Provider>);
    fireEvent.click(view.getByRole("button", { name: "Add transform" }));
    fireEvent.click(view.getByRole("button", { name: "Available tables for transform 1" }));
    fireEvent.click(view.getByRole("button", { name: "Use system.query_log in Include" }));
    expect((view.getByRole("combobox", { name: "Include transform 1" }) as HTMLInputElement).value).toBe("system.query_log");
    expect(view.queryByRole("dialog")).toBeNull();
  });

  it("allows browsing and copying but not Use in a read-only transform", () => {
    const table = { namespace: "system", name: "query_log" };
    const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [table], excluded: [] }], issues: [] });
    const onChange = vi.fn();
    const view = render(<TableCatalogContext.Provider value={{ tables: [table], preview }}>
      <MiddlewareEditor value={[step]} disabled onChange={onChange} />
    </TableCatalogContext.Provider>);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    fireEvent.click(view.getByRole("button", { name: "Available tables for transform 1" }));
    const use = view.getByRole("button", { name: "Use system.query_log in Include" }) as HTMLButtonElement;
    expect(use.disabled).toBe(true);
    expect((view.getByRole("button", { name: "Copy system.query_log" }) as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(use);
    expect(onChange).not.toHaveBeenCalled();
    expect(view.getByRole("dialog", { name: "Available tables" })).toBeTruthy();
  });

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
    expect(view.queryByRole("button", { name: "Show all" })).toBeNull();
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

  it("adds one unselected step scoped to all tables without injecting SQL", () => {
    const onChange = vi.fn();
    const view = render(<MiddlewareEditor value={[]} disabled={false} onChange={onChange} />);
    fireEvent.click(view.getByRole("button", { name: "Add transform" }));
    expect(onChange).toHaveBeenCalledWith([{
      tables: { include: "*", include_mode: "glob", exclude_mode: "glob" },
    }]);
  });

  it("opens a new step with Not selected and editable scope, not an unsupported-YAML error", () => {
    const view = render(<Editor value={[]} />);
    fireEvent.click(view.getByRole("button", { name: "Add transform" }));
    expect(view.getByRole("button", { name: "Transformation" }).textContent).toBe("Not selected");
    expect(view.container.querySelector(".middleware-strip-title")?.textContent).toBe("Not selected");
    expect(view.queryByRole("alert")).toBeNull();
    expect(view.queryByLabelText("SQL over table input")).toBeNull();
    expect(view.queryByLabelText("Column")).toBeNull();
    expect(view.getByRole("textbox", { name: "Include transform 1" })).toBeTruthy();
    expect((view.getByRole("button", { name: "Preview transform 1" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("guides required-field navigation to the draft picker or its collapsed strip", () => {
    const view = render(<Editor value={[{ tables: step.tables }]} />);
    const root = view.container as HTMLElement;
    const toggle = view.getByRole("button", { name: "Expand transform 1" });
    expect(nextRequiredTarget(root)?.querySelector(REQUIRED_CONTROL_SELECTOR)).toBe(toggle);
    fireEvent.click(toggle);
    const selector = view.getByRole("button", { name: "Transformation" });
    expect(nextRequiredTarget(root)?.querySelector(REQUIRED_CONTROL_SELECTOR)).toBe(selector);
    fireEvent.click(selector);
    fireEvent.click(view.getByRole("option", { name: "SQL", exact: true }));
    expect(nextRequiredTarget(root)).toBeUndefined();
  });

  it.each([
    ["SQL", "datafusion", { sql: "SELECT * FROM input" }],
    ["String filter", "filter", { field: "", value: "" }],
  ] as const)("creates %s only after an explicit selection, preserving the scope", (label, kind, config) => {
    const onChange = vi.fn();
    const view = render(<MiddlewareEditor value={[{ tables: step.tables }]} disabled={false} onChange={onChange} />);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    fireEvent.click(view.getByRole("button", { name: "Transformation" }));
    expect(view.getAllByRole("option")[0]?.textContent).toBe("Not selected");
    fireEvent.click(view.getByRole("option", { name: label, exact: true }));
    expect(onChange).toHaveBeenCalledExactlyOnceWith([{ tables: step.tables, [kind]: config }]);
  });

  it("can return to Not selected without losing scope or moving the selector's DOM node", () => {
    const view = render(<Editor />);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    const selector = view.getByRole("button", { name: "Transformation" });
    const include = view.getByRole("textbox", { name: "Include transform 1" });
    fireEvent.click(selector);
    fireEvent.click(view.getByRole("option", { name: "Not selected" }));
    expect(view.getByRole("button", { name: "Transformation" })).toBe(selector);
    expect(selector.textContent).toBe("Not selected");
    expect(view.getByRole("textbox", { name: "Include transform 1" })).toBe(include);
    expect((include as HTMLInputElement).value).toBe(step.tables.include);
    expect((view.getByRole("textbox", { name: "Exclude transform 1" }) as HTMLInputElement).value).toBe(step.tables.exclude);
    expect(view.queryByDisplayValue(step.datafusion.sql)).toBeNull();
  });

  it("does not reinterpret malformed or unsupported configurations as an empty selection", () => {
    const view = render(<Editor value={[{ tables: step.tables, unknown: {} }]} />);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    expect(view.getByRole("alert").textContent).toContain("Open YAML");
    expect(view.queryByRole("button", { name: "Transformation" })).toBeNull();
  });

  it("clones the complete step without opening either strip", () => {
    const onChange = vi.fn();
    const view = render(<MiddlewareEditor value={[step]} disabled={false} onChange={onChange} />);
    const clone = view.getByRole("button", { name: "Clone transform 1" });
    expect(clone.classList.contains("copy-action")).toBe(true);
    expect(clone.classList.contains("secondary-button")).toBe(false);
    expect(clone.querySelector(".copy-icon")).toBeTruthy();
    fireEvent.click(clone);
    const next = onChange.mock.calls[0]?.[0] as JsonValue[];
    expect(next).toEqual([step, step]);
    expect(next[1]).not.toBe(step);
    expect(view.queryByDisplayValue("SELECT id FROM input")).toBeNull();
  });

  it("edits Include and Exclude independently without changing SQL", () => {
    const onChange = vi.fn();
    const view = render(<MiddlewareEditor value={[step]} disabled={false} onChange={onChange} />);
    fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
    fireEvent.input(view.getByRole("textbox", { name: "Include transform 1" }), { target: { value: "public.events*" } });
    expect(onChange).toHaveBeenLastCalledWith([{ ...step, tables: { ...step.tables, include: "public.events*" } }]);
    fireEvent.click(view.getByRole("button", { name: "Exclude transform 1 regex" }));
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

  it("keeps drag and cancelled deletion independent of the full-height disclosure", () => {
    vi.spyOn(window, "confirm").mockReturnValue(false);
    const view = render(<Editor />);
    const toggle = view.getByRole("button", { name: "Expand transform 1" });
    const drag = view.getByRole("button", { name: "Reorder transform 1" });
    const remove = view.getByRole("button", { name: "Delete transform 1" });
    for (const expanded of [false, true]) {
      fireEvent.click(drag);
      fireEvent.click(remove);
      expect(toggle.getAttribute("aria-expanded")).toBe(String(expanded));
      expect(view.getByRole("button", { name: "Reorder transform 1" })).toBe(drag);
      expect(view.getByRole("button", { name: "Delete transform 1" })).toBe(remove);
      fireEvent.click(toggle);
      expect(toggle.getAttribute("aria-expanded")).toBe(String(!expanded));
    }
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
