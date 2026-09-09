// @vitest-environment jsdom
import { act, cleanup, fireEvent, renderHook, waitFor } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { MiddlewareEditor } from "../src/features/middleware/MiddlewareEditor";
import { useTransformMatches } from "../src/features/middleware/TransformTableScope";
import { TableCatalogContext } from "../src/schema/tableCatalog";
import type { TableSelectionPreviewResult } from "../src/generated/apiContract";
import { render } from "./support/render";

afterEach(cleanup);
const source = { namespace: "public", name: "raw_events" };
const current = { namespace: "public", name: "archive" };
const tables = [source];
const prefix = [{ rename_table: { mode: "exact", name: "public.archive" } }];
const response: TableSelectionPreviewResult = {
  cards: [{ selected: [current], excluded: [] }], issues: [], lineage: [{ source, current }],
};

it("matches projected names, loads original schemas and withdraws stale lineage immediately", async () => {
  const preview = vi.fn().mockResolvedValue(response);
  const catalog = { tables, preview };
  const hook = renderHook(({ preceding, include }) => useTransformMatches({ include }, true, preceding), {
    initialProps: { preceding: prefix, include: "public.archive" },
    wrapper: ({ children }) => <TableCatalogContext.Provider value={catalog}>{children}</TableCatalogContext.Provider>,
  });
  await waitFor(() => expect(hook.result.current?.tables).toEqual([current]));
  expect(hook.result.current?.sourceTables).toEqual([source]);
  expect(preview.mock.calls[0]?.[0]).toEqual({ catalog: tables, preceding_middlewares: prefix,
    selection: { type: "selected", rules: [{ include: "public.archive" }] } });
  const lineage = hook.result.current?.lineage;
  hook.rerender({ preceding: prefix, include: "public.*" });
  expect(hook.result.current?.tables).toBeUndefined();
  expect(hook.result.current?.lineage).toBe(lineage);
  await waitFor(() => expect(preview).toHaveBeenCalledTimes(2));
  await waitFor(() => expect(hook.result.current?.tables).toEqual([current]));
  expect(hook.result.current?.lineage).toBe(lineage);
  let finish!: (value: TableSelectionPreviewResult) => void;
  preview.mockImplementation(() => new Promise(resolve => { finish = resolve; }));
  hook.rerender({ preceding: [{ rename_table: { mode: "exact", name: "public.next" } }], include: "public.*" });
  expect(hook.result.current).toBeUndefined();
  await waitFor(() => expect(preview).toHaveBeenCalledTimes(3));
  const signal = preview.mock.calls[2]?.[1] as AbortSignal;
  hook.rerender({ preceding: prefix, include: "public.archive" });
  expect(signal.aborted).toBe(true);
  await act(async () => finish({ cards: [], issues: [], lineage: [] }));
  expect(hook.result.current?.lineage).toBe(lineage);
});

it("browses renamed tables with their cached original schema status", async () => {
  const preview = vi.fn().mockResolvedValue(response);
  const metadata = { id: "cached", catalog_count: 1, loaded: [source], errors: [], loading: false };
  const view = render(<TableCatalogContext.Provider value={{ tables, preview, metadata }}>
    <MiddlewareEditor value={[...prefix, { tables: { include: "public.archive" }, filter: { field: "id", value: "1" } }]}
      disabled={false} onChange={() => undefined} />
  </TableCatalogContext.Provider>);
  fireEvent.click(view.getByRole("button", { name: "Expand transform 2" }));
  const browse = view.getByRole("button", { name: "Available tables for transform 2" }) as HTMLButtonElement;
  expect(browse.disabled).toBe(true);
  await waitFor(() => expect(browse.disabled).toBe(false));
  fireEvent.click(browse);
  await waitFor(() => expect(view.getByLabelText("Schema Loaded for public.archive")).toBeTruthy());
  expect(view.queryByText("public.raw_events")).toBeNull();
  expect(view.getByRole("button", { name: "Available tables for transform 2" })).toBe(browse);
});
