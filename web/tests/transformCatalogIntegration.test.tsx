// @vitest-environment jsdom

import { cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { useCallback, useState } from "preact/hooks";
import { afterEach, expect, it, vi } from "vitest";

import catalog from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { EndpointCard } from "../src/delivery/EndpointCard";
import { endpointValue } from "../src/delivery/editorConfig";
import { tableConnectionIdentity } from "../src/delivery/useEndpointActions";
import { useTransformCatalog, type VerifiedTableCatalog } from "../src/features/middleware/useTransformCatalog";
import type { ConnectionCheckResult, TableIdentity } from "../src/generated/apiContract";
import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";
import type { ConnectorDefinition, EndpointDefinition, JsonObject } from "../src/types";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

function Form({ connectorKey, onPublished }: {
  connectorKey: string;
  onPublished: (catalog: VerifiedTableCatalog | undefined) => void;
}) {
  const connector = catalog.connectors.find(item => item.key === connectorKey)! as unknown as ConnectorDefinition;
  const endpoint = connector.source! as EndpointDefinition;
  const [config, setConfig] = useState<JsonObject>({
    delivery_type: "batch",
    source: { [connectorKey]: { ...endpoint.initial, tables: { type: "all" }, hide_system_tables: true } },
  });
  const [checked, setChecked] = useState<VerifiedTableCatalog>();
  const publish = useCallback((value: VerifiedTableCatalog | undefined) => {
    onPublished(value);
    setChecked(value);
  }, [onPublished]);
  const sourceConfig = endpointValue(config, "source", connectorKey) as JsonObject;
  const transform = useTransformCatalog({ connector: connectorKey, config: sourceConfig }, checked, api);
  return <>
    <EndpointCard title="Source" role="source" selectedKey={connectorKey} connectors={[connector]}
      endpoint={endpoint} config={config} readOnly={false} showRequiredErrors={false}
      onChoose={() => undefined} onConfig={setConfig} onTableCatalog={publish} />
    <output data-testid="transform-catalog">{transform ? JSON.stringify(transform.tables) : "unavailable"}</output>
  </>;
}

it.each([
  ["postgres", "pg_catalog"],
  ["mysql", "performance_schema"],
  ["clickhouse", "system"],
])("publishes the raw verified %s catalog, filters locally, and invalidates credentials", async (connectorKey, namespace) => {
  const userTable: TableIdentity = { namespace: "analytics", name: "reports" };
  const systemTable: TableIdentity = { namespace, name: "tables" };
  const tables = [userTable, systemTable];
  const request = vi.spyOn(api, "checkConnection").mockResolvedValue({ status: "verified", options: {}, message: null, tables });
  vi.spyOn(api, "previewTables").mockImplementation(async ({ catalog: visible }) => ({
    cards: [{ selected: visible, excluded: [] }], issues: [],
  }));
  const published = vi.fn();
  const view = render(<Form connectorKey={connectorKey} onPublished={published} />);
  const probe = view.getByTestId("transform-catalog");
  expect(probe.textContent).toBe("unavailable");
  const check = view.getByRole("button", { name: /Connect & load metadata|Refresh metadata/ });
  fireEvent.click(check);
  expect(check.getAttribute("aria-busy")).toBe("true");
  fireEvent.click(check);
  expect(request).toHaveBeenCalledOnce();
  await waitFor(() => expect(probe.textContent).toBe(JSON.stringify([userTable])));
  const checkedConfig = request.mock.calls[0]![0].config;
  expect(published).toHaveBeenLastCalledWith({
    identity: tableConnectionIdentity(connectorKey, checkedConfig), tables,
  });
  expect(published.mock.calls.at(-1)![0].tables).toBe(tables);

  const publications = published.mock.calls.length;
  fireEvent.click(view.getByLabelText(/^Hide system tables/));
  await waitFor(() => expect(probe.textContent).toBe(JSON.stringify(tables)));
  expect(request).toHaveBeenCalledOnce();
  expect(published).toHaveBeenCalledTimes(publications);
  expect(view.getByTestId("transform-catalog")).toBe(probe);

  fireEvent.input(view.getByLabelText(/^Password/), { target: { value: "changed" } });
  expect(probe.textContent).toBe("unavailable");
  await waitFor(() => expect(published).toHaveBeenLastCalledWith(undefined));
  expect(request).toHaveBeenCalledOnce();
});

it("does not publish an old check response after credentials change", async () => {
  let finish!: (result: ConnectionCheckResult) => void;
  const request = vi.spyOn(api, "checkConnection").mockReturnValue(new Promise(resolve => { finish = resolve; }));
  const published = vi.fn();
  const view = render(<Form connectorKey="postgres" onPublished={published} />);
  fireEvent.click(view.getByRole("button", { name: /Connect & load metadata|Refresh metadata/ }));
  const signal = request.mock.calls[0]![1]!;
  fireEvent.input(view.getByLabelText(/^Password/), { target: { value: "changed" } });
  await waitFor(() => expect(signal.aborted).toBe(true));
  finish({ status: "verified", options: {}, message: null, tables: [{ namespace: "private", name: "old_catalog" }] });
  await waitFor(() => expect(view.getByText("Required to unlock tables and transforms")).toBeTruthy());
  expect(view.getByTestId("transform-catalog").textContent).toBe("unavailable");
  expect(published.mock.calls.every(([value]) => value === undefined)).toBe(true);
});

it("withdraws the old catalog while rechecking and publishes the new snapshot", async () => {
  const original = [{ namespace: "analytics", name: "old_table" }];
  const refreshed = [{ namespace: "analytics", name: "new_table" }];
  let finish!: (result: ConnectionCheckResult) => void;
  const request = vi.spyOn(api, "checkConnection")
    .mockResolvedValueOnce({ status: "verified", options: {}, message: null, tables: original })
    .mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  vi.spyOn(api, "previewTables").mockImplementation(async ({ catalog: visible }) => ({
    cards: [{ selected: visible, excluded: [] }], issues: [],
  }));
  const published = vi.fn();
  const view = render(<Form connectorKey="postgres" onPublished={published} />);
  const check = view.getByRole("button", { name: /Connect & load metadata|Refresh metadata/ });
  const probe = view.getByTestId("transform-catalog");
  fireEvent.click(check);
  await waitFor(() => expect(probe.textContent).toBe(JSON.stringify(original)));
  fireEvent.click(check);
  expect(check.getAttribute("aria-busy")).toBe("true");
  await waitFor(() => expect(probe.textContent).toBe("unavailable"));
  expect(published).toHaveBeenLastCalledWith(undefined);
  fireEvent.click(check);
  expect(request).toHaveBeenCalledTimes(2);
  finish({ status: "verified", options: {}, message: null, tables: refreshed });
  await waitFor(() => expect(probe.textContent).toBe(JSON.stringify(refreshed)));
  expect(published.mock.calls.at(-1)![0].tables).toBe(refreshed);
  expect(view.getByTestId("transform-catalog")).toBe(probe);
});

it.each([
  { status: "network_reachable", options: {}, message: null, tables: [] },
  { status: "verified", options: {}, message: null },
] satisfies ConnectionCheckResult[])("never publishes incomplete verification: $status", async result => {
  vi.spyOn(api, "checkConnection").mockResolvedValue(result);
  const published = vi.fn();
  const view = render(<Form connectorKey="postgres" onPublished={published} />);
  const check = view.getByRole("button", { name: /Connect & load metadata|Refresh metadata/ });
  fireEvent.click(check);
  await waitFor(() => expect(check.getAttribute("aria-busy")).toBe("false"));
  expect(view.getByTestId("transform-catalog").textContent).toBe("unavailable");
  expect(published.mock.calls.every(([value]) => value === undefined)).toBe(true);
});
