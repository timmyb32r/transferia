// @vitest-environment jsdom
import { cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { AvailableTablesDialog } from "../src/features/middleware/AvailableTablesDialog";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });
const tables = [{ namespace: "db", name: "reports" }, { namespace: "db", name: "other" }];

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

it("copies exact table names with immediate pending feedback and no duplicate clipboard write", async () => {
  let finish!: () => void;
  const writeText = vi.fn().mockImplementation(() => new Promise<void>(resolve => { finish = resolve; }));
  vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
  const view = render(<AvailableTablesDialog catalog={{ tables, preview: vi.fn() }} onClose={() => {}} />);
  const copy = view.getByRole("button", { name: "Copy db.reports" });
  fireEvent.click(copy);
  fireEvent.click(copy);
  expect(copy.getAttribute("aria-busy")).toBe("true");
  expect(writeText).toHaveBeenCalledExactlyOnceWith("db.reports");
  finish();
  await waitFor(() => expect(copy.getAttribute("aria-busy")).toBe("false"));
  expect(view.getByRole("status").textContent).toContain("Copied db.reports");
  expect(view.getByRole("button", { name: "Copy db.reports" })).toBe(copy);
  vi.unstubAllGlobals();
});
