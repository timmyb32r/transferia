// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { useSourceMetadata, SourceMetadataContext } from "../src/delivery/sourceMetadata";
import { ConnectionCheck } from "../src/delivery/ConnectionCheck";
import { TransformSchemaLoader } from "../src/features/middleware/TransformSchemaLoader";
import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";
import type { MetadataConnection, MetadataStatus } from "../src/generated/apiContract";
import { render, renderHook } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });
const table = { namespace: "public", name: "events" };
const config = { host: "localhost", database: "db", username: "reader", tables: { type: "all" }, hide_system_tables: true };
const source = { connector: "postgres", config };
function status(id = "one", count = 1, loaded = [table], loading = false): MetadataStatus {
  return { id, catalog_count: count, loaded, loading, errors: [], validation: null };
}
function response(metadata = status()): MetadataConnection {
  return { connection: { status: "verified", options: {}, tables: [table], message: null }, metadata };
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(finish => { resolve = finish; });
  return { resolve, promise };
}
function hook() {
  return renderHook(({ value }) => useSourceMetadata({ connector: "postgres", config: value,
    mode: "batch", sessionKey: "editor", validating: false }), { initialProps: { value: config } });
}
function Harness() {
  const metadata = useSourceMetadata({ ...source, mode: "batch", sessionKey: "editor", validating: false });
  return <SourceMetadataContext.Provider value={metadata}>
    <ConnectionCheck check={metadata.check} required onCheck={() => { void metadata.checkConnection(); }} />
    <TransformSchemaLoader source={source} tables={[table]} disabled={false} />
    <button data-testid="following-control">Following action</button>
  </SourceMetadataContext.Provider>;
}

it("connects once and lets Validate join the already-pending source request", async () => {
  const pending = deferred<MetadataConnection>();
  const connect = vi.spyOn(api, "connectMetadata").mockReturnValue(pending.promise);
  vi.spyOn(api, "releaseMetadata").mockResolvedValue(status());
  const { result } = hook();
  let sourceRequest!: Promise<MetadataStatus | undefined>;
  let validationRequest!: Promise<MetadataStatus | undefined>;
  act(() => { sourceRequest = result.current.checkConnection(); validationRequest = result.current.checkConnection(); });
  expect(result.current.check.state).toBe("checking");
  expect(sourceRequest).toBe(validationRequest);
  expect(connect).toHaveBeenCalledOnce();
  await act(async () => { pending.resolve(response()); await sourceRequest; });
  expect(result.current.metadata?.id).toBe("one");
  expect(await validationRequest).toEqual(status());
});

it("retries after a failed Refresh without resending the deleted cache ID", async () => {
  const connect = vi.spyOn(api, "connectMetadata").mockResolvedValueOnce(response())
    .mockRejectedValueOnce(new Error("Connection refused")).mockResolvedValueOnce(response(status("two")));
  vi.spyOn(api, "releaseMetadata").mockResolvedValue(status());
  const { result } = hook();
  await act(async () => { await result.current.checkConnection(); });
  await act(async () => { await result.current.checkConnection(); });
  expect(result.current.check.state).toBe("error");
  await act(async () => { await result.current.checkConnection(); });
  expect(connect.mock.calls.map(call => call[0].replace_metadata_id)).toEqual([null, "one", null]);
  expect(result.current.metadata?.id).toBe("two");
});

it("keeps selection filters local but invalidates a changed connection and ignores its stale result", async () => {
  const pending = deferred<MetadataConnection>();
  vi.spyOn(api, "connectMetadata").mockResolvedValueOnce(response()).mockReturnValueOnce(pending.promise);
  const release = vi.spyOn(api, "releaseMetadata").mockResolvedValue(status());
  const { result, rerender } = hook();
  await act(async () => { await result.current.checkConnection(); });
  rerender({ value: { ...config, hide_system_tables: false } });
  expect(result.current.metadata?.id).toBe("one");
  expect(release).not.toHaveBeenCalled();
  let request!: Promise<MetadataStatus | undefined>;
  act(() => { request = result.current.checkConnection(); });
  rerender({ value: { ...config, username: "another" } });
  await act(async () => { pending.resolve(response(status("stale"))); await request; });
  expect(result.current.metadata).toBeUndefined();
  expect(release).toHaveBeenCalledWith("stale");
});

it("shows polling failure in the fixed source status and stops claiming loading", async () => {
  vi.spyOn(api, "connectMetadata").mockResolvedValue(response(status("one", 2, [], true)));
  vi.spyOn(api, "metadataStatus").mockRejectedValue(new Error("Cache not found"));
  vi.spyOn(api, "releaseMetadata").mockResolvedValue(status());
  const view = render(<Harness />);
  const slot = view.container.querySelector(".connection-check-result");
  const following = view.getByTestId("following-control");
  fireEvent.click(view.getByRole("button", { name: "Connect & load metadata" }));
  await waitFor(() => expect(slot?.textContent).toContain("Cache not found"));
  expect(slot?.textContent).toContain("retry");
  expect(view.container.querySelector(".connection-check-result")).toBe(slot);
  expect(view.getByTestId("following-control")).toBe(following);
});

it("loads only matched schemas on explicit activation, with a stable pending/status slot", async () => {
  const cached = status("large", 2400, []);
  const pending = deferred<MetadataStatus>();
  vi.spyOn(api, "connectMetadata").mockResolvedValue(response(cached));
  vi.spyOn(api, "metadataStatus").mockResolvedValue(cached);
  vi.spyOn(api, "releaseMetadata").mockResolvedValue(cached);
  const load = vi.spyOn(api, "loadMetadataSchemas").mockReturnValue(pending.promise);
  const view = render(<Harness />);
  fireEvent.click(view.getByRole("button", { name: "Connect & load metadata" }));
  const button = view.getByRole("button", { name: "Load schemas", hidden: true });
  const row = button.parentElement!;
  const controls = Array.from(row.children);
  const following = view.getByTestId("following-control");
  await waitFor(() => expect((button as HTMLButtonElement).style.visibility).toBe("visible"));
  expect(load).not.toHaveBeenCalled();
  fireEvent.click(button); fireEvent.click(button);
  expect(button.getAttribute("aria-busy")).toBe("true");
  expect(load).toHaveBeenCalledOnce();
  expect(load).toHaveBeenCalledWith("large", { source, tables: [table] }, expect.any(AbortSignal));
  await act(async () => { pending.resolve(status("large", 2400)); await pending.promise; });
  await waitFor(() => expect(button.getAttribute("aria-busy")).toBe("false"));
  expect(Array.from(row.children)).toEqual(controls);
  expect(view.getByTestId("following-control")).toBe(following);
});
