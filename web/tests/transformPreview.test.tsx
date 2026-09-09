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
  const view = render(<TransformPreview entries={entries} index={0} source={undefined} matchedTables={undefined} />);
  expect((view.getByRole("button", { name: "Run preview" }) as HTMLButtonElement).disabled).toBe(true);
});

function setup() {
  const checkConnection = vi.fn().mockResolvedValue({ status: "verified", options: {}, tables: [table] });
  const previewTransforms = vi.fn().mockResolvedValue(response);
  const controlPlane = { ...httpControlPlane, checkConnection, previewTransforms };
  const component = (steps = entries) => <ApplicationServicesProvider services={{ controlPlane }}>
    <TableCatalogContext.Provider value={{ tables: [table], preview: controlPlane.previewTables }}>
    <TransformPreview entries={steps} index={0} source={source} matchedTables={[table]} />
    </TableCatalogContext.Provider>
  </ApplicationServicesProvider>;
  return { checkConnection, previewTransforms, component, view: render(component()) };
}

async function chooseTable(view: ReturnType<typeof setup>["view"]) {
  await waitFor(() => expect((view.getByRole("button", { name: "Sample table" }) as HTMLButtonElement).disabled).toBe(false));
  fireEvent.click(view.getByRole("button", { name: "Sample table" }));
  fireEvent.click(await view.findByRole("option", { name: "public.reports" }));
}

it("does not connect or sample until explicitly requested", () => {
  const { checkConnection, previewTransforms } = setup();
  expect(checkConnection).not.toHaveBeenCalled();
  expect(previewTransforms).not.toHaveBeenCalled();
});

it("shows renamed choices but samples the physical source table", async () => {
  const renamed = { ...table, name: "archive" };
  const previewTransforms = vi.fn().mockResolvedValue(response);
  const view = render(<ApplicationServicesProvider services={{ controlPlane: { ...httpControlPlane, previewTransforms } }}>
    <TransformPreview entries={entries} index={0} source={source} matchedTables={[renamed]}
      lineage={[{ source: table, current: renamed }]} />
  </ApplicationServicesProvider>);
  fireEvent.click(view.getByRole("button", { name: "Sample table" }));
  expect(view.queryByRole("option", { name: "public.reports" })).toBeNull();
  fireEvent.click(view.getByRole("option", { name: "public.archive" }));
  fireEvent.click(view.getByRole("button", { name: "Run preview" }));
  await waitFor(() => expect(previewTransforms).toHaveBeenCalledOnce());
  expect(previewTransforms.mock.calls[0]?.[0]).toEqual(expect.objectContaining({ table }));
});

it("does not sample when a current name has ambiguous source lineage", () => {
  const renamed = { ...table, name: "archive" };
  const previewTransforms = vi.fn();
  const view = render(<ApplicationServicesProvider services={{ controlPlane: { ...httpControlPlane, previewTransforms } }}>
    <TransformPreview entries={entries} index={0} source={source} matchedTables={[renamed]}
      lineage={[{ source: table, current: renamed }, { source: { ...table, name: "other" }, current: renamed }]} />
  </ApplicationServicesProvider>);
  const run = view.getByRole("button", { name: "Run preview" }) as HTMLButtonElement;
  expect(run.disabled).toBe(true);
  fireEvent.click(run);
  expect(previewTransforms).not.toHaveBeenCalled();
});

it("defaults to the first All matched tables option and samples each table separately", async () => {
  const other = { namespace: "public", name: "other" };
  const previewTransforms = vi.fn().mockImplementation(async request => ({
    ...response, before: { ...response.before, table: request.table }, after: { ...response.after, table: request.table },
  }));
  const view = render(<ApplicationServicesProvider services={{ controlPlane: { ...httpControlPlane, previewTransforms } }}>
    <TransformPreview entries={entries} index={0} source={source} matchedTables={[table, other]} />
  </ApplicationServicesProvider>);
  const picker = view.getByRole("button", { name: "Sample table" });
  expect(picker.textContent).toContain("All matched tables");
  expect(previewTransforms).not.toHaveBeenCalled();
  fireEvent.click(picker);
  expect(view.getAllByRole("option")[0]?.textContent).toContain("All matched tables");
  fireEvent.click(view.getByRole("option", { name: "All matched tables" }));
  const run = view.getByRole("button", { name: "Run preview" });
  const output = view.getByRole("tabpanel");
  fireEvent.click(run);
  fireEvent.click(run);
  expect(run.getAttribute("aria-busy")).toBe("true");
  await waitFor(() => expect(run.getAttribute("aria-busy")).toBe("false"));
  expect(previewTransforms).toHaveBeenCalledTimes(2);
  for (const [index, candidate] of [table, other].entries()) {
    expect(previewTransforms.mock.calls[index]?.[0]).toEqual(expect.objectContaining({ table: candidate, row_limit: 20 }));
  }
  expect(view.getByRole("heading", { name: "public.reports" })).toBeTruthy();
  expect(view.getByRole("heading", { name: "public.other" })).toBeTruthy();
  expect(view.getByRole("button", { name: "Sample table" })).toBe(picker);
  expect(view.getByRole("tabpanel")).toBe(output);
});

it("stops all-table sampling on a failure and identifies the failed table", async () => {
  const previewTransforms = vi.fn().mockRejectedValue(new Error("Read failed"));
  const view = render(<ApplicationServicesProvider services={{ controlPlane: { ...httpControlPlane, previewTransforms } }}>
    <TransformPreview entries={entries} index={0} source={source} matchedTables={[table, { namespace: "public", name: "other" }]} />
  </ApplicationServicesProvider>);
  fireEvent.click(view.getByRole("button", { name: "Run preview" }));
  await waitFor(() => expect(view.getByRole("status").textContent).toContain("public.reports: Read failed"));
  expect(previewTransforms).toHaveBeenCalledTimes(1);
  expect(view.queryByText("amount")).toBeNull();
});

it("uses only matched tables and invalidates selection when matches change", async () => {
  const other = { namespace: "public", name: "new_reports" };
  const checkConnection = vi.fn().mockResolvedValue({ status: "verified", options: {}, tables: [other] });
  const api = { ...httpControlPlane, checkConnection };
  const initial = [table];
  const component = (tables: typeof initial) => <ApplicationServicesProvider services={{ controlPlane: api }}>
    <TableCatalogContext.Provider value={{ tables: [table, other], preview: api.previewTables }}>
      <TransformPreview entries={entries} index={0} source={source} matchedTables={tables} />
    </TableCatalogContext.Provider>
  </ApplicationServicesProvider>;
  const view = render(component(initial));
  fireEvent.click(view.getByRole("button", { name: "Sample table" }));
  expect(view.getByRole("option", { name: "public.reports" })).toBeTruthy();
  expect(view.queryByRole("option", { name: "public.new_reports" })).toBeNull();
  fireEvent.click(view.getByRole("option", { name: "public.reports" }));
  view.rerender(component([other]));
  expect((view.getByRole("button", { name: "Run preview" }) as HTMLButtonElement).disabled).toBe(true);
  expect(checkConnection).not.toHaveBeenCalled();
});

it.each([undefined, []])("never falls back to all tables while matches are unavailable or empty (%s)", matchedTables => {
  const view = render(<TableCatalogContext.Provider value={{ tables: [table], preview: httpControlPlane.previewTables }}>
    <TransformPreview entries={entries} index={0} source={source} matchedTables={matchedTables} />
  </TableCatalogContext.Provider>);
  expect((view.getByRole("button", { name: "Sample table" }) as HTMLButtonElement).disabled).toBe(true);
  expect((view.getByRole("button", { name: "Run preview" }) as HTMLButtonElement).disabled).toBe(true);
});

it("loads actual source tables and runs the prefix against a bounded typed sample", async () => {
  const { view, checkConnection, previewTransforms } = setup();
  await chooseTable(view);
  fireEvent.click(view.getByRole("button", { name: "Run preview" }));
  await waitFor(() => expect(previewTransforms).toHaveBeenCalledOnce());
  expect(checkConnection).not.toHaveBeenCalled();
  expect(previewTransforms).toHaveBeenCalledWith({
    source, table, metadata_id: null, row_limit: 20, middlewares: entries, through_step: 0,
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

it("cancels an in-flight sample when its table stops matching without remounting controls", async () => {
  const previewTransforms = vi.fn();
  let finish!: (result: typeof response) => void;
  previewTransforms.mockImplementation(() => new Promise(resolve => { finish = resolve; }));
  const component = (matchedTables: typeof table[] | undefined) =>
    <ApplicationServicesProvider services={{ controlPlane: { ...httpControlPlane, previewTransforms } }}>
      <TransformPreview entries={entries} index={0} source={source} matchedTables={matchedTables} />
    </ApplicationServicesProvider>;
  const view = render(component([table]));
  await chooseTable(view);
  const picker = view.getByRole("button", { name: "Sample table" });
  const run = view.getByRole("button", { name: "Run preview" }) as HTMLButtonElement;
  const status = view.getByRole("status");
  const output = view.getByRole("tabpanel");
  fireEvent.click(run);
  const signal = previewTransforms.mock.calls[0]?.[1] as AbortSignal;
  view.rerender(component(undefined));
  expect(run.disabled).toBe(true);
  await waitFor(() => expect(signal.aborted).toBe(true));
  finish(response);
  view.rerender(component([]));
  expect(view.getByRole("button", { name: "Sample table" })).toBe(picker);
  expect(view.getByRole("button", { name: "Run preview" })).toBe(run);
  expect(view.getByRole("status")).toBe(status);
  expect(view.getByRole("tabpanel")).toBe(output);
  expect(view.queryByText("amount")).toBeNull();
  fireEvent.click(run);
  expect(previewTransforms).toHaveBeenCalledTimes(1);
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
  const { view, previewTransforms } = setup();
  await chooseTable(view);
  previewTransforms.mockRejectedValue(new Error("Source permission denied"));
  const status = view.getByRole("status");
  fireEvent.click(view.getByRole("button", { name: "Run preview" }));
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

it("hides preview limit controls while retaining execution budgets", async () => {
  const { view, previewTransforms } = setup();
  await chooseTable(view);
  expect(view.queryByRole("button", { name: "Preview limits" })).toBeNull();
  for (const name of ["Source sample (MiB)", "SQL memory (MiB)", "Timeout (seconds)"]) {
    expect(view.queryByRole("spinbutton", { name })).toBeNull();
  }
  fireEvent.click(view.getByRole("button", { name: "Run preview" }));
  await waitFor(() => expect(previewTransforms).toHaveBeenCalledOnce());
  expect(previewTransforms).toHaveBeenCalledWith(expect.objectContaining({
    max_sample_bytes: 16 * 1024 * 1024, memory_limit_bytes: 256 * 1024 * 1024, timeout_ms: 30000,
  }), expect.any(AbortSignal));
});
