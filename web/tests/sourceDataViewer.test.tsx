// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { SourceDataViewer } from "../src/delivery/SourceDataViewer";
import { SourceMetadataContext, type SourceMetadata } from "../src/delivery/sourceMetadata";
import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";
import type { SourcePreviewResult, TransformPreviewFrame } from "../src/generated/apiContract";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });
const table = { namespace: "public", name: "events" };
const source = { connector: "postgres", config: { database: "db", tables: { type: "all" }, hide_system_tables: false } };
const frame: TransformPreviewFrame = { table, columns: [{ name: "id", arrow_type: "Int64", nullable: false, metadata: {} }], rows: [{ id: "9007199254740993" }] };
const metadata = { metadata: { id: "cache", loaded: [] }, discovery: { state: "success", tables: [table] } } as unknown as SourceMetadata;

it("automatically loads raw source rows on opening, preserves exact strings and suppresses duplicate activation", async () => {
  let finish!: (value: SourcePreviewResult) => void;
  const sample = vi.spyOn(api, "previewSource").mockImplementation(() => new Promise(resolve => { finish = resolve; }));
  const transforms = vi.spyOn(api, "previewTransforms");
  const view = render(<SourceMetadataContext.Provider value={metadata}><SourceDataViewer source={source} onClose={() => {}} /></SourceMetadataContext.Provider>);
  const button = view.getByRole("button", { name: "Load sample" });
  await waitFor(() => expect(sample).toHaveBeenCalledOnce());
  fireEvent.click(button); fireEvent.click(button);
  expect(button.getAttribute("aria-busy")).toBe("true");
  expect(sample).toHaveBeenCalledTimes(1);
  expect(sample.mock.calls[0]![0]).toEqual({ metadata_id: "cache", source, table, row_limit: 20, max_sample_bytes: 16_777_216, timeout_ms: 30_000 });
  expect(view.queryByLabelText("Max sample MiB")).toBeNull();
  expect(view.queryByLabelText("Timeout seconds")).toBeNull();
  await act(async () => finish({ frames: [frame] }));
  expect(view.getByText("9007199254740993")).toBeTruthy();
  expect(transforms).not.toHaveBeenCalled();
});

it("aborts the read when the viewer unmounts and never renders a late response", async () => {
  let signal: AbortSignal | undefined;
  vi.spyOn(api, "previewSource").mockImplementation((_request, next) => { signal = next; return new Promise(() => {}); });
  const view = render(<SourceMetadataContext.Provider value={metadata}><SourceDataViewer source={source} onClose={() => {}} /></SourceMetadataContext.Provider>);
  await waitFor(() => expect(signal).toBeDefined());
  view.unmount();
  expect(signal?.aborted).toBe(true);
});

it.each(["kafka", "logbroker", "s3"])("automatically shows %s configured-parser output instead of invoking Scan", async connector => {
  const preview = vi.spyOn(api, "previewSource").mockResolvedValue({ frames: [frame] });
  const scan = vi.spyOn(api, "previewMessage");
  const parsed = { connector, config: { parser: { authored: "parser settings" } } };
  const state = { discovery: { state: "idle" } } as unknown as SourceMetadata;
  const view = render(<SourceMetadataContext.Provider value={state}><SourceDataViewer
    source={parsed} mode="parsed" onClose={() => {}} /></SourceMetadataContext.Provider>);
  await view.findByText("9007199254740993");
  expect(preview).toHaveBeenCalledOnce();
  expect(preview.mock.calls[0]![0]).toEqual({ source: parsed, metadata_id: null, table: null,
    row_limit: 20, max_sample_bytes: 16_777_216, timeout_ms: 30_000 });
  expect(scan).not.toHaveBeenCalled();
  expect(view.queryByRole("button", { name: "Use parser" })).toBeNull();
  expect(view.queryByLabelText("Max sample MiB")).toBeNull();
  expect(view.queryByLabelText("Timeout seconds")).toBeNull();
  view.rerender(<SourceMetadataContext.Provider value={{ ...state }}><SourceDataViewer
    source={{ ...parsed, config: { ...parsed.config } }} mode="parsed" onClose={() => {}} /></SourceMetadataContext.Provider>);
  await act(async () => {});
  expect(preview).toHaveBeenCalledOnce();
});

it("shows a failed table selection lookup instead of staying pending", async () => {
  const sample = vi.spyOn(api, "previewSource");
  vi.spyOn(api, "previewTables").mockRejectedValue(new Error("Selection lookup failed"));
  const filtered = { ...source, config: { ...source.config, tables: { type: "selected", tables: [] } } };
  const view = render(<SourceMetadataContext.Provider value={metadata}><SourceDataViewer source={filtered} onClose={() => {}} /></SourceMetadataContext.Provider>);
  await waitFor(() => expect(view.getByRole("status").textContent).toBe("Selection lookup failed"));
  expect(view.getByRole("button", { name: "Load sample" }).hasAttribute("disabled")).toBe(true);
  expect(sample).not.toHaveBeenCalled();
});

it("reports an empty selection without offering an unselected table", async () => {
  const sample = vi.spyOn(api, "previewSource");
  const state = { ...metadata, discovery: { state: "success", tables: [] } } as unknown as SourceMetadata;
  const view = render(<SourceMetadataContext.Provider value={state}><SourceDataViewer source={source} onClose={() => {}} /></SourceMetadataContext.Provider>);
  await waitFor(() => expect(view.getByRole("status").textContent).toContain("No tables match"));
  expect(view.getByRole("button", { name: "Load sample" }).hasAttribute("disabled")).toBe(true);
  expect(sample).not.toHaveBeenCalled();
});

it("Escape dismisses the table dropdown before dismissing the viewer", async () => {
  vi.spyOn(api, "previewSource").mockResolvedValue({ frames: [frame] });
  const close = vi.fn();
  const view = render(<SourceMetadataContext.Provider value={metadata}><SourceDataViewer source={source} onClose={close} /></SourceMetadataContext.Provider>);
  const trigger = await view.findByRole("button", { name: /public\.events/ });
  fireEvent.click(trigger);
  expect(view.getByRole("listbox")).toBeTruthy();
  fireEvent.keyDown(view.getByRole("searchbox"), { key: "Escape" });
  expect(view.queryByRole("listbox")).toBeNull();
  expect(close).not.toHaveBeenCalled();
  fireEvent.keyDown(view.getByRole("dialog"), { key: "Escape" });
  expect(close).toHaveBeenCalledOnce();
});

it("waits for manual reload after editing rows without cancelling the new request", async () => {
  let signal: AbortSignal | undefined;
  const sample = vi.spyOn(api, "previewSource").mockImplementation((_request, next) => { signal = next; return new Promise(() => {}); })
    .mockResolvedValueOnce({ frames: [frame] });
  const view = render(<SourceMetadataContext.Provider value={metadata}><SourceDataViewer source={source} onClose={() => {}} /></SourceMetadataContext.Provider>);
  const button = view.getByRole("button", { name: "Load sample" });
  await view.findByText("9007199254740993");
  fireEvent.input(view.getByLabelText("Sample rows"), { target: { value: "42" } });
  await act(async () => {});
  expect(sample).toHaveBeenCalledOnce();
  fireEvent.click(button);
  await act(async () => {});
  expect(sample).toHaveBeenCalledTimes(2);
  expect(sample.mock.calls[1]![0].row_limit).toBe(42);
  expect(signal?.aborted).toBe(false);
  expect(button.getAttribute("aria-busy")).toBe("true");
});

it("invalidates the request and ignores late rows when the source changes", async () => {
  const pending: { signal: AbortSignal | undefined; finish: (value: SourcePreviewResult) => void }[] = [];
  const sample = vi.spyOn(api, "previewSource").mockImplementation((_request, signal) => new Promise(finish => { pending.push({ signal, finish }); }));
  const content = (database: string) => <SourceMetadataContext.Provider value={metadata}><SourceDataViewer
    source={{ ...source, config: { ...source.config, database } }} onClose={() => {}} /></SourceMetadataContext.Provider>;
  const view = render(content("db"));
  const button = view.getByRole("button", { name: "Load sample" });
  await waitFor(() => expect(sample).toHaveBeenCalledOnce());
  view.rerender(content("another"));
  expect(pending[0]!.signal?.aborted).toBe(true);
  await waitFor(() => expect(sample).toHaveBeenCalledTimes(2));
  await act(async () => pending[0]!.finish({ frames: [frame] }));
  expect(view.queryByText("9007199254740993")).toBeNull();
  expect(button.getAttribute("aria-busy")).toBe("true");
  expect(pending[1]!.signal?.aborted).toBe(false);
});

it("automatically samples a newly chosen table and discards the previous table's pending response", async () => {
  const other = { namespace: "public", name: "users" };
  const pending: { signal: AbortSignal | undefined; finish: (value: SourcePreviewResult) => void }[] = [];
  const sample = vi.spyOn(api, "previewSource").mockImplementation((_request, signal) => new Promise(finish => { pending.push({ signal, finish }); }));
  const state = { ...metadata, discovery: { state: "success", tables: [table, other] } } as unknown as SourceMetadata;
  const view = render(<SourceMetadataContext.Provider value={state}><SourceDataViewer source={source} onClose={() => {}} /></SourceMetadataContext.Provider>);
  await waitFor(() => expect(sample).toHaveBeenCalledOnce());
  fireEvent.click(view.getByRole("button", { name: "public.events" }));
  fireEvent.click(view.getByRole("option", { name: "public.users" }));
  expect(sample).toHaveBeenCalledTimes(2);
  expect(sample.mock.calls[1]![0].table).toEqual(other);
  expect(pending[0]!.signal?.aborted).toBe(true);
  expect(view.getByRole("button", { name: "Load sample" }).getAttribute("aria-busy")).toBe("true");
  await act(async () => pending[0]!.finish({ frames: [frame] }));
  expect(view.queryByText("9007199254740993")).toBeNull();
  await act(async () => pending[1]!.finish({ frames: [{ ...frame, table: other, rows: [{ id: "42" }] }] }));
  expect(view.getByText("42")).toBeTruthy();
  fireEvent.click(view.getByRole("button", { name: "public.users" }));
  fireEvent.click(view.getByRole("option", { name: "public.users" }));
  expect(sample).toHaveBeenCalledTimes(2);
  fireEvent.click(view.getByRole("button", { name: "public.users" }));
  fireEvent.click(view.getByRole("option", { name: "public.events" }));
  expect(sample).toHaveBeenCalledTimes(3);
  expect(sample.mock.calls[2]![0].table).toEqual(table);
});

it("does not retry an automatic failure until manually requested and reloads when reopened", async () => {
  const sample = vi.spyOn(api, "previewSource").mockRejectedValueOnce(new Error("Sample failed"))
    .mockResolvedValue({ frames: [frame] });
  const content = <SourceMetadataContext.Provider value={metadata}><SourceDataViewer source={source} onClose={() => {}} /></SourceMetadataContext.Provider>;
  const view = render(content);
  await waitFor(() => expect(view.getByRole("status").textContent).toBe("Sample failed"));
  await act(async () => {});
  expect(sample).toHaveBeenCalledOnce();
  fireEvent.click(view.getByRole("button", { name: "Load sample" }));
  await view.findByText("9007199254740993");
  expect(sample).toHaveBeenCalledTimes(2);
  view.unmount();
  const reopened = render(content);
  await reopened.findByText("9007199254740993");
  expect(sample).toHaveBeenCalledTimes(3);
});
