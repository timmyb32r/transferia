// @vitest-environment jsdom
import { cleanup, waitFor } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { renderHook } from "./support/render";
import { useTransformCatalog } from "../src/features/middleware/useTransformCatalog";
import { tableConnectionIdentity } from "../src/delivery/useEndpointActions";
import { httpControlPlane } from "../src/infrastructure/controlPlane/httpControlPlane";

afterEach(cleanup);
const table = { namespace: "analytics", name: "reports" };
const system = { namespace: "system", name: "tables" };
const source = { connector: "clickhouse", config: { host: "host", tables: { type: "all" }, hide_system_tables: true } };
const snapshot = { identity: tableConnectionIdentity(source.connector, source.config)!, tables: [table, system] };

it("has no catalog before a source is selected", () => {
  const hook = renderHook(() => useTransformCatalog(undefined, undefined, httpControlPlane));
  expect(hook.result.current).toBeUndefined();
});

it("shares only verified source-selected tables and filters system tables without reconnecting", async () => {
  const api = { ...httpControlPlane, previewTables: vi.fn() };
  const hook = renderHook(({ value }) => useTransformCatalog(value, snapshot, api), { initialProps: { value: source } });
  await waitFor(() => expect(hook.result.current?.tables).toEqual([table]));
  hook.rerender({ value: { ...source, config: { ...source.config, hide_system_tables: false } } });
  await waitFor(() => expect(hook.result.current?.tables).toEqual([table, system]));
  expect(api.previewTables).not.toHaveBeenCalled();
  hook.rerender({ value: { ...source, config: { ...source.config, host: "different" } } });
  expect(hook.result.current).toBeUndefined();
});

it("uses the backend selection matcher and discards a stale source selection result", async () => {
  let finish!: (value: unknown) => void;
  const previewTables = vi.fn().mockImplementation(() => new Promise(resolve => { finish = resolve; }));
  const api = { ...httpControlPlane, previewTables };
  const selected = { ...source, config: { ...source.config, tables: { type: "selected", rules: [{ include: "analytics.*", exclude: "analytics.test*" }] } } };
  const hook = renderHook(({ value }) => useTransformCatalog(value, snapshot, api), { initialProps: { value: selected } });
  await waitFor(() => expect(previewTables).toHaveBeenCalledOnce());
  expect(previewTables.mock.calls[0]?.[0]).toEqual({ catalog: [table], selection: selected.config.tables });
  const signal = previewTables.mock.calls[0]?.[1] as AbortSignal;
  hook.rerender({ value: { ...selected, config: { ...selected.config, host: "other" } } });
  await waitFor(() => expect(signal.aborted).toBe(true));
  finish({ cards: [{ selected: [table], excluded: [] }], issues: [] });
  await waitFor(() => expect(hook.result.current).toBeUndefined());
});
