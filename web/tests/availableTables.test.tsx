// @vitest-environment jsdom
import { cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { AvailableTablesDialog } from "../src/features/tableSelection/AvailableTablesDialog";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });
const tables = [{ namespace: "db", name: "reports" }, { namespace: "db", name: "other" }];

it("uses the selected table identity and closes immediately without a clipboard write", () => {
  const onUse = vi.fn(), onClose = vi.fn(), writeText = vi.fn();
  vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
  const view = render(<AvailableTablesDialog catalog={{ tables, preview: vi.fn() }} onUse={onUse} onClose={onClose} />);
  fireEvent.click(view.getByRole("button", { name: "Use db.other in Include" }));
  expect(onUse).toHaveBeenCalledExactlyOnceWith(tables[1]);
  expect(onClose).toHaveBeenCalledOnce();
  expect(writeText).not.toHaveBeenCalled();
  vi.unstubAllGlobals();
});

it("keeps Use unavailable in a read-only table browser", () => {
  const onClose = vi.fn();
  const view = render(<AvailableTablesDialog catalog={{ tables, preview: vi.fn() }} showUse onClose={onClose} />);
  const use = view.getByRole("button", { name: "Use db.reports in Include" }) as HTMLButtonElement;
  expect(use.disabled).toBe(true);
  fireEvent.click(use);
  expect(onClose).not.toHaveBeenCalled();
  expect((view.getByRole("button", { name: "Copy db.reports" }) as HTMLButtonElement).disabled).toBe(false);
});

it("includes the last Use action in the modal keyboard focus trap", () => {
  const view = render(<AvailableTablesDialog catalog={{ tables, preview: vi.fn() }} onUse={vi.fn()} onClose={vi.fn()} />);
  const last = view.getByRole("button", { name: "Use db.other in Include" });
  last.focus();
  fireEvent.keyDown(last, { key: "Tab" });
  expect(document.activeElement).toBe(view.getByRole("button", { name: "Close available tables" }));
});

it("omits Include actions when browsing a source catalog", () => {
  const view = render(<AvailableTablesDialog catalog={{ tables, preview: vi.fn() }} onClose={vi.fn()} />);
  expect(view.queryByRole("button", { name: /Use .* in Include/ })).toBeNull();
  expect(view.getAllByRole("button", { name: /^Copy / })).toHaveLength(2);
});

it("keeps search focus inside the modal after Enter and closes on Escape", () => {
  const onClose = vi.fn();
  const view = render(<AvailableTablesDialog catalog={{ tables, preview: vi.fn() }} onClose={onClose} />);
  const input = view.getByRole("textbox", { name: "Search tables" });
  expect(document.activeElement).toBe(input);
  fireEvent.keyDown(input, { key: "Enter" });
  expect(document.activeElement).toBe(input);
  fireEvent.keyDown(input, { key: "Escape" });
  expect(onClose).toHaveBeenCalledOnce();
});

it("searches available tables through the shared glob/regex matcher in a stable viewport", async () => {
  const preview = vi.fn().mockResolvedValue({ cards: [{ selected: [tables[0]], excluded: [] }], issues: [] });
  const view = render(<AvailableTablesDialog catalog={{ tables, preview }} onClose={() => {}} />);
  const list = view.getByRole("region", { name: "Available table names" });
  fireEvent.input(view.getByRole("textbox", { name: "Search tables" }), { target: { value: "db.rep" } });
  await waitFor(() => expect(preview).toHaveBeenCalledWith({ catalog: tables, selection: { type: "selected", rules: [{ include: "db.rep*", include_mode: "glob" }] } }, expect.any(AbortSignal)));
  await waitFor(() => expect(within(list).queryByText("db.other")).toBeNull());
  expect(view.getByRole("region", { name: "Available table names" })).toBe(list);
  fireEvent.click(view.getByRole("button", { name: "Search tables regex" }));
  await waitFor(() => expect(preview).toHaveBeenLastCalledWith({ catalog: tables, selection: { type: "selected", rules: [{ include: "db.rep", include_mode: "regex" }] } }, expect.any(AbortSignal)));
});

it("traps Tab from the last copy button and lets Escape close the dialog", () => {
  const onClose = vi.fn();
  const view = render(<AvailableTablesDialog catalog={{ tables, preview: vi.fn() }} onClose={onClose} />);
  const last = view.getByRole("button", { name: "Copy db.other" });
  last.focus();
  fireEvent.keyDown(last, { key: "Tab" });
  expect(document.activeElement).toBe(view.getByRole("button", { name: "Close available tables" }));
  fireEvent.keyDown(last, { key: "Escape" });
  expect(onClose).toHaveBeenCalledOnce();
});

it("copies exact table names with immediate pending feedback and no duplicate clipboard write", async () => {
  let finish!: () => void;
  const writeText = vi.fn().mockImplementation(() => new Promise<void>(resolve => { finish = resolve; }));
  vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
  const view = render(<AvailableTablesDialog catalog={{ tables, preview: vi.fn() }} onClose={() => {}} />);
  const copy = view.getByRole("button", { name: "Copy db.reports" });
  expect(copy.classList.contains("copy-action")).toBe(true);
  expect(copy.classList.contains("copy-action-framed")).toBe(true);
  expect(copy.classList.contains("secondary-button")).toBe(false);
  fireEvent.click(copy);
  fireEvent.click(copy);
  expect(copy.getAttribute("aria-busy")).toBe("true");
  fireEvent.click(view.getByRole("button", { name: "Copy db.other" }));
  expect(writeText).toHaveBeenCalledExactlyOnceWith("db.reports");
  finish();
  await waitFor(() => expect(copy.getAttribute("aria-busy")).toBe("false"));
  expect(view.getByRole("status").textContent).toContain("Copied db.reports");
  expect(copy.querySelector(".copy-icon-check")).toBeTruthy();
  expect(view.getByRole("button", { name: "Copy db.reports" })).toBe(copy);
  vi.unstubAllGlobals();
});
