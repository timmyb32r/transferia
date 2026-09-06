// @vitest-environment jsdom

import { cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { ApplicationServicesProvider } from "../src/bootstrap/ApplicationServicesProvider";
import { TransformPreview } from "../src/features/middleware/TransformPreview";
import { httpControlPlane } from "../src/infrastructure/controlPlane/httpControlPlane";
import { render } from "./support/render";
import { TableCatalogContext } from "../src/schema/tableCatalog";

afterEach(cleanup);

const source = { connector: "postgres", config: { database: "db", tables: { type: "all" } } };
const table = { namespace: "public", name: "reports" };
const entries = [{ tables: { include: "*" }, filter: { field: "status", value: "ready" } }];
const columns = [{ name: "amount", arrow_type: "Decimal128(30, 2)", nullable: false, metadata: {} }];
const response = {
  before: { table, columns, rows: [{ amount: "12345678901234567890.12" }] },
  after: { table, columns, rows: [] },
  applied: true,
};

it("renders an unavailable preview without crashing before a supported source is selected", () => {
  const view = render(<TransformPreview entries={entries} index={0} source={undefined} />);
  expect((view.getByRole("button", { name: "Run preview" }) as HTMLButtonElement).disabled).toBe(true);
});

function setup() {
  const checkConnection = vi.fn().mockResolvedValue({ status: "verified", options: {}, tables: [table] });
  const previewTransforms = vi.fn().mockResolvedValue(response);
  const controlPlane = { ...httpControlPlane, checkConnection, previewTransforms };
  const component = (steps = entries) => <ApplicationServicesProvider services={{ controlPlane }}>
    <TransformPreview entries={steps} index={0} source={source} />
  </ApplicationServicesProvider>;
  return { checkConnection, previewTransforms, component, view: render(component()) };
}

async function chooseTable(view: ReturnType<typeof setup>["view"]) {
  fireEvent.click(view.getByRole("button", { name: "Load tables" }));
  await waitFor(() => expect((view.getByRole("button", { name: "Sample table" }) as HTMLButtonElement).disabled).toBe(false));
  fireEvent.click(view.getByRole("button", { name: "Sample table" }));
  fireEvent.click(await view.findByRole("option", { name: "public.reports" }));
}

it("does not connect or sample until explicitly requested", () => {
  const { checkConnection, previewTransforms } = setup();
  expect(checkConnection).not.toHaveBeenCalled();
  expect(previewTransforms).not.toHaveBeenCalled();
});

it("uses an explicitly refreshed catalog until the shared verified source selection changes", async () => {
  const other = { namespace: "public", name: "new_reports" };
  const checkConnection = vi.fn().mockResolvedValue({ status: "verified", options: {}, tables: [other] });
  const api = { ...httpControlPlane, checkConnection };
  const initial = [table];
  const component = (tables: typeof initial) => <ApplicationServicesProvider services={{ controlPlane: api }}>
    <TableCatalogContext.Provider value={{ tables, preview: api.previewTables }}>
      <TransformPreview entries={entries} index={0} source={source} />
    </TableCatalogContext.Provider>
  </ApplicationServicesProvider>;
  const view = render(component(initial));
  fireEvent.click(view.getByRole("button", { name: "Load tables" }));
  await waitFor(() => expect(view.getByRole("button", { name: "Load tables" }).getAttribute("aria-busy")).toBe("false"));
  fireEvent.click(view.getByRole("button", { name: "Sample table" }));
  expect(view.getByRole("option", { name: "public.new_reports" })).toBeTruthy();
  expect(view.queryByRole("option", { name: "public.reports" })).toBeNull();
  fireEvent.click(view.getByRole("option", { name: "public.new_reports" }));
  view.rerender(component([table]));
  expect((view.getByRole("button", { name: "Run preview" }) as HTMLButtonElement).disabled).toBe(true);
});

it("loads actual source tables and runs the prefix against a bounded typed sample", async () => {
  const { view, checkConnection, previewTransforms } = setup();
  await chooseTable(view);
  fireEvent.click(view.getByRole("button", { name: "Run preview" }));
  await waitFor(() => expect(previewTransforms).toHaveBeenCalledOnce());
  expect(checkConnection).toHaveBeenCalledWith({ ...source, role: "source" }, expect.any(AbortSignal));
  expect(previewTransforms).toHaveBeenCalledWith({
    source, table, row_limit: 20, middlewares: entries, through_step: 0,
    max_sample_bytes: 16 * 1024 * 1024, memory_limit_bytes: 256 * 1024 * 1024, timeout_ms: 30000,
  }, expect.any(AbortSignal));
  await waitFor(() => expect(view.getByText("0 rows")).toBeTruthy());
  expect(view.getByText("amount")).toBeTruthy();
  expect(view.getByText("Decimal128(30, 2)")).toBeTruthy();
  fireEvent.click(view.getByRole("tab", { name: "Before step" }));
  expect(view.getByText("12345678901234567890.12")).toBeTruthy();
});

it("marks pending immediately and deduplicates preview activation", async () => {
  const { view, previewTransforms } = setup();
  await chooseTable(view);
  let finish!: (result: typeof response) => void;
  previewTransforms.mockImplementation(() => new Promise(resolve => { finish = resolve; }));
  const run = view.getByRole("button", { name: "Run preview" });
  fireEvent.click(run);
  fireEvent.click(run);
  expect(run.getAttribute("aria-busy")).toBe("true");
  expect(previewTransforms).toHaveBeenCalledOnce();
  const status = view.getByRole("status"), output = view.getByRole("tabpanel");
  finish(response);
  await waitFor(() => expect(run.getAttribute("aria-busy")).toBe("false"));
  expect(view.getByRole("status")).toBe(status);
  expect(view.getByRole("tabpanel")).toBe(output);
});

it("invalidates an in-flight result after a transform edit", async () => {
  const { view, component, previewTransforms } = setup();
  await chooseTable(view);
  let finish!: (result: typeof response) => void;
  previewTransforms.mockImplementation(() => new Promise(resolve => { finish = resolve; }));
  fireEvent.click(view.getByRole("button", { name: "Run preview" }));
  const signal = previewTransforms.mock.calls[0]?.[1] as AbortSignal;
  view.rerender(component([{ tables: { include: "*" }, filter: { field: "status", value: "new" } }]));
  await waitFor(() => expect(signal.aborted).toBe(true));
  finish(response);
  await waitFor(() => expect(view.getByRole("button", { name: "Run preview" }).getAttribute("aria-busy")).toBe("false"));
  expect(view.queryByText("amount")).toBeNull();
});

it("shows source failures in the existing status slot", async () => {
  const { view, checkConnection } = setup();
  checkConnection.mockRejectedValue(new Error("Source permission denied"));
  const status = view.getByRole("status");
  fireEvent.click(view.getByRole("button", { name: "Load tables" }));
  await waitFor(() => expect(status.textContent).toContain("Source permission denied"));
  expect(view.getByRole("status")).toBe(status);
});

it("does not substitute a default for an invalid sample row limit", async () => {
  const { view, previewTransforms } = setup();
  await chooseTable(view);
  fireEvent.input(view.getByRole("spinbutton", { name: "Sample rows" }), { target: { value: "0" } });
  fireEvent.click(view.getByRole("button", { name: "Run preview" }));
  expect(previewTransforms).not.toHaveBeenCalled();
  expect(view.getByRole("status").textContent).toContain("positive integer");
});

it("exposes preview budgets and rejects invalid limits without source reads", async () => {
  const { view, previewTransforms } = setup();
  await chooseTable(view);
  fireEvent.click(view.getByRole("button", { name: "Preview limits" }));
  const memory = view.getByRole("spinbutton", { name: "SQL memory (MiB)" });
  expect((memory as HTMLInputElement).value).toBe("256");
  fireEvent.input(memory, { target: { value: "0" } });
  fireEvent.click(view.getByRole("button", { name: "Run preview" }));
  expect(previewTransforms).not.toHaveBeenCalled();
  expect(view.getByRole("status").textContent).toContain("positive integers");
});
