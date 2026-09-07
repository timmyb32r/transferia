// @vitest-environment jsdom

import { cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, expect, it, vi } from "vitest";

import catalog from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { EndpointCard } from "../src/delivery/EndpointCard";
import type { ConnectionCheckResult } from "../src/generated/apiContract";
import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";
import type { ConnectorDefinition, EndpointDefinition, JsonObject } from "../src/types";
import { render } from "./support/render";
import { mockTableDiscovery } from "./support/metadata";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

it("does not narrow a standalone Tables fallback twice", () => {
  const view = render(<Form />);
  expect(view.container.querySelector(".endpoint-card-source > .island-form .source-tables-card > .island-form-wide")).not.toBeNull();
});

function Form({ connectorKey = "postgres", readOnly = false, role = "source", deliveryType = "batch", fullWidth = false, initialConfig, onConfig }: {
  connectorKey?: string; readOnly?: boolean; role?: "source" | "sink"; deliveryType?: string; fullWidth?: boolean;
  initialConfig?: JsonObject; onConfig?: (config: JsonObject) => void;
}) {
  const connector = catalog.connectors.find(item => item.key === connectorKey)! as unknown as ConnectorDefinition;
  const endpoint = connector[role]! as EndpointDefinition;
  const [tablesHost, setTablesHost] = useState<HTMLElement | null>(null);
  const [config, setConfig] = useState<JsonObject>(initialConfig ?? {
    delivery_type: deliveryType,
    [role]: { [connectorKey]: endpoint.initial },
  });
  return <><EndpointCard title={role === "source" ? "Source" : "Destination"} role={role}
    selectedKey={connectorKey} connectors={[connector]} endpoint={endpoint} config={config}
    readOnly={readOnly} showRequiredErrors={false} onChoose={() => undefined}
    onConfig={next => { setConfig(next); onConfig?.(next); }} tablesHost={fullWidth ? tablesHost : undefined} />
    {fullWidth && <><button>Destination settings</button><section class="source-tables-card" aria-label="Source tables" ref={setTablesHost} /></>}
  </>;
}

it.each([false, true])("labels the S3 destination prefix as Path without changing its configuration (readOnly=%s)", readOnly => {
  const sink = catalog.connectors.find(item => item.key === "s3")!.sink!.initial;
  const config: JsonObject = { delivery_type: "batch", sink: { s3: { ...sink, path_prefix: "Reports/2026" } } };
  const onConfig = vi.fn();
  const view = render(<Form connectorKey="s3" role="sink" readOnly={readOnly} initialConfig={config} onConfig={onConfig} />);
  const path = view.getByLabelText(/^Path(?:\s|$)/) as HTMLInputElement;
  expect(path.value).toBe("Reports/2026");
  expect(path.disabled).toBe(readOnly);
  expect(view.queryByLabelText(/^Path prefix/)).toBeNull();
  expect(onConfig).not.toHaveBeenCalled();
  if (!readOnly) {
    fireEvent.input(path, { target: { value: "Reports/New Path" } });
    expect(onConfig.mock.lastCall?.[0]).toEqual({ ...config, sink: { s3: { ...sink, path_prefix: "Reports/New Path" } } });
  }
});

it("moves S3 path, table name and parser with Scan into Tables, leaving connection controls in Source", () => {
  const view = render(<Form connectorKey="s3" fullWidth />);
  const host = view.getByRole("region", { name: "Source tables" });
  const group = within(host).getByRole("group", { name: "Table settings" }) as HTMLFieldSetElement;
  expect(group.disabled).toBe(false);
  expect(within(group).getByLabelText(/^Path prefix/)).toBeTruthy();
  expect(within(group).getByLabelText(/^Table name/)).toBeTruthy();
  expect(within(group).getByLabelText(/^Parser/).textContent).toContain("Parquet parser");
  expect(within(group).getByRole("button", { name: "Preview one message" })).toBeTruthy();
  const source = view.container.querySelector(".endpoint-card-source")!;
  expect(source.contains(group)).toBe(false);
  expect(source.contains(view.getByLabelText(/^Bucket/))).toBe(true);
  expect(source.contains(view.getByRole("button", { name: "Check connection", exact: true }))).toBe(true);
  expect(view.getAllByLabelText(/^Path prefix/)).toHaveLength(1);
  expect(view.getAllByLabelText(/^Table name/)).toHaveLength(1);
  expect(view.getAllByLabelText(/^Parser/)).toHaveLength(1);
  expect(host.querySelector(":scope > .island-form:not(.island-form-wide)")).not.toBeNull();
  expect(view.queryByRole("button", { name: "Discover tables" })).toBeNull();
});

it("edits S3 fields in Tables without rewriting their values or the rest of the delivery", () => {
  const source = catalog.connectors.find(item => item.key === "s3")!.source!.initial;
  const config: JsonObject = { delivery_type: "batch", source: { s3: source }, sink: { discard: {} } };
  const onConfig = vi.fn();
  const view = render(<Form connectorKey="s3" fullWidth initialConfig={config} onConfig={onConfig} />);
  expect(onConfig).not.toHaveBeenCalled();
  const group = view.getByRole("group", { name: "Table settings" });
  const path = within(group).getByLabelText(/^Path prefix/);
  const name = within(group).getByLabelText(/^Table name/);
  fireEvent.input(path, { target: { value: "Exact Prefix/../2026/" } });
  fireEvent.input(name, { target: { value: "Exact.Table Name" } });
  expect(onConfig.mock.lastCall?.[0]).toEqual({ ...config, source: { s3: {
    ...source, path_prefix: "Exact Prefix/../2026/", table_name: "Exact.Table Name",
  } } });
  expect(within(group).getByLabelText(/^Path prefix/)).toBe(path);
  expect(within(group).getByLabelText(/^Table name/)).toBe(name);
});

it.each([
  ["iceberg", "table_names"], ["opensearch", "indices"], ["ydb", "tables"], ["ytsaurus", "tables"],
])("moves %s %s into Tables without requiring discovery or moving Check connection", (connectorKey, field) => {
  const view = render(<Form connectorKey={connectorKey} fullWidth />);
  const host = view.getByRole("region", { name: "Source tables" });
  const group = within(host).getByRole("group", { name: "Table settings" }) as HTMLFieldSetElement;
  const selectorQuery = connectorKey === "opensearch" || connectorKey === "ydb"
    ? ".compact-array-field" : `[data-field-name="${field}"]`;
  const selector = group.querySelector(selectorQuery)!;
  expect(selector).not.toBeNull();
  expect(group.disabled).toBe(false);
  const input = selector.querySelector("input")!;
  expect(input.disabled).toBe(false);
  expect(view.container.querySelector(".endpoint-card-source")?.contains(selector)).toBe(false);
  expect(group.querySelectorAll(selectorQuery)).toHaveLength(1);
  expect(selector.querySelector(".field-label")?.textContent).toContain(
    connectorKey === "iceberg" ? "Table names" : connectorKey === "opensearch" ? "Indices" : "Tables");
  const check = view.getByRole("button", { name: "Check connection", exact: true });
  expect(view.container.querySelector(".endpoint-card-source")?.contains(check)).toBe(true);
  expect(host.querySelector(":scope > .island-form:not(.island-form-wide)")).not.toBeNull();
  expect(within(host).getByRole("heading", { name: "Tables" })).toBeTruthy();
  expect(view.queryByRole("button", { name: "Discover tables" })).toBeNull();
  expect(view.queryByRole("button", { name: "Available tables in source" })).toBeNull();
  if (connectorKey === "iceberg")
    expect(view.container.querySelector(".endpoint-card-source [data-field-name='namespace']")).not.toBeNull();
});

it.each(["iceberg", "opensearch", "ydb", "ytsaurus", "s3"])("keeps relocated %s selection read-only when viewing a saved delivery", connectorKey => {
  const view = render(<Form connectorKey={connectorKey} fullWidth readOnly />);
  const group = view.getByRole("group", { name: "Table settings" }) as HTMLFieldSetElement;
  expect(group.disabled).toBe(true);
  const inputs = [...group.querySelectorAll("input")];
  expect(inputs.length).toBeGreaterThan(0);
  expect(inputs.every(input => input.disabled)).toBe(true);
  if (connectorKey === "s3") {
    expect((within(group).getByLabelText(/^Parser/) as HTMLButtonElement).disabled).toBe(true);
    expect((within(group).getByRole("button", { name: "Preview one message" }) as HTMLButtonElement).disabled).toBe(true);
  }
});

it.each([["opensearch", "auth"], ["ydb", "tables"], ["ytsaurus", "tables"], ["s3", "installation"]])("leaves %s destination settings in Destination", (connectorKey, field) => {
  const view = render(<Form connectorKey={connectorKey} role="sink" />);
  expect(view.queryByRole("group", { name: "Table settings" })).toBeNull();
  expect(view.container.querySelector(".source-tables-card")).toBeNull();
  const selector = view.container.querySelector(connectorKey === "ydb"
    ? ".endpoint-card-sink .compact-array-field" : `.endpoint-card-sink [data-field-name="${field}"]`);
  expect(selector).not.toBeNull();
  if (connectorKey === "ydb") expect(selector!.querySelector(".field-label")?.textContent).toContain("Tables");
});

it.each([
  ["iceberg", "table_names", ""], ["opensearch", "indices", "name"],
  ["ydb", "tables", "path"], ["ytsaurus", "tables", "path"],
])("edits relocated %s selection without changing connection or delivery configuration", (connectorKey, field, itemField) => {
  const endpoint = catalog.connectors.find(item => item.key === connectorKey)!.source!;
  const initial = { ...endpoint.initial, [field]: [itemField ? { [itemField]: "original" } : "original"] };
  const config: JsonObject = { delivery_type: "batch", source: { [connectorKey]: initial },
    sink: { discard: {} }, middlewares: [] };
  const onConfig = vi.fn();
  const view = render(<Form connectorKey={connectorKey} fullWidth initialConfig={config} onConfig={onConfig} />);
  expect(onConfig).not.toHaveBeenCalled();
  const group = view.getByRole("group", { name: "Table settings" });
  const input = group.querySelector("input")!;
  const selected = "//Exact Path/Table-01";
  fireEvent.input(input, { target: { value: selected } });
  expect(onConfig.mock.lastCall?.[0]).toEqual({ ...config, source: { [connectorKey]: {
    ...initial, [field]: [itemField ? { [itemField]: selected } : selected],
  } } });
  expect(view.getByRole("group", { name: "Table settings" })).toBe(group);
  expect(group.querySelector("input")).toBe(input);
});

it.each(["iceberg", "opensearch", "ydb", "ytsaurus", "s3"])("keeps %s Tables controls mounted and editable while connection checking is pending", async connectorKey => {
  let finish!: (result: ConnectionCheckResult) => void;
  const request = vi.spyOn(api, "checkConnection").mockReturnValue(new Promise(resolve => { finish = resolve; }));
  const discover = vi.spyOn(api, "connectMetadata");
  const view = render(<Form connectorKey={connectorKey} fullWidth />);
  const host = view.getByRole("region", { name: "Source tables" });
  const group = within(host).getByRole("group", { name: "Table settings" }) as HTMLFieldSetElement;
  const input = group.querySelector("input")!;
  const siblings = [...host.parentElement!.children];
  const controls = [...group.querySelectorAll("input, button")];
  const check = view.getByRole("button", { name: "Check connection", exact: true });
  fireEvent.click(check);
  expect(check.getAttribute("aria-busy")).toBe("true");
  expect(check.getAttribute("aria-disabled")).toBe("true");
  expect(group.disabled).toBe(false);
  expect(input.disabled).toBe(false);
  fireEvent.click(check);
  expect(request).toHaveBeenCalledOnce();
  finish({ status: "verified", options: {}, tables: [] });
  await view.findByText("Connection verified.");
  expect(view.getByRole("button", { name: "Check connection", exact: true })).toBe(check);
  expect(view.getByRole("region", { name: "Source tables" })).toBe(host);
  expect(within(host).getByRole("group", { name: "Table settings" })).toBe(group);
  expect([...host.parentElement!.children]).toEqual(siblings);
  expect([...group.querySelectorAll("input, button")]).toEqual(controls);
  expect(group.disabled).toBe(false);
  expect(input.disabled).toBe(false);
  expect(discover).not.toHaveBeenCalled();
});

it.each(["postgres", "mysql", "clickhouse"])("plain %s Check does not discover or unlock tables", async connectorKey => {
  const check = vi.spyOn(api, "checkConnection").mockResolvedValue({ status: "verified", options: {}, tables: [] });
  const discover = mockTableDiscovery().mockResolvedValue({ status: "verified", options: {}, tables: [] });
  const view = render(<Form connectorKey={connectorKey} fullWidth />);
  const tables = view.getByRole("region", { name: "Source tables" });
  const discoverButton = view.getByRole("button", { name: "Discover tables" });
  expect(tables.contains(discoverButton)).toBe(true);
  expect(discoverButton.closest("fieldset")).toBeNull();
  expect(view.container.querySelector(".endpoint-card-source")?.contains(discoverButton)).toBe(false);
  fireEvent.click(view.getByRole("button", { name: "Check connection", exact: true }));
  await view.findByText("Connection verified.");
  expect(check).toHaveBeenCalledOnce();
  expect(discover).not.toHaveBeenCalled();
  expect((view.getByLabelText("Include rule 1") as HTMLInputElement).disabled).toBe(true);
  fireEvent.click(discoverButton);
  await waitFor(() => expect((view.getByLabelText("Include rule 1") as HTMLInputElement).disabled).toBe(false));
  expect(check).toHaveBeenCalledOnce();
  expect(discover).toHaveBeenCalledOnce();
});

it.each(["postgres", "mysql", "clickhouse"])("keeps one full-width %s table group mounted outside Source while metadata unlocks it", async connectorKey => {
  mockTableDiscovery().mockResolvedValue({ status: "verified", options: {}, tables: [] });
  const view = render(<Form connectorKey={connectorKey} fullWidth deliveryType={connectorKey === "mysql" ? "stream" : "batch"} />);
  const host = view.getByRole("region", { name: "Source tables" });
  host.scrollIntoView = vi.fn();
  const group = view.getByRole("group", { name: "Table settings" });
  expect(host.contains(group)).toBe(true);
  expect(view.container.querySelector(".endpoint-card-source")?.contains(group)).toBe(false);
  const destination = view.getByRole("button", { name: "Destination settings" });
  expect(destination.compareDocumentPosition(host) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
  expect((within(group).getByRole("radio", { name: "Selected tables" }) as HTMLButtonElement).disabled).toBe(true);
  fireEvent.click(view.getByRole("button", { name: "Discover tables" }));
  await waitFor(() => expect((within(group).getByRole("radio", { name: "Selected tables" }) as HTMLButtonElement).disabled).toBe(false));
  expect(view.getByRole("group", { name: "Table settings" })).toBe(group);
  expect(view.getByRole("region", { name: "Source tables" })).toBe(host);
  expect(host.scrollIntoView).not.toHaveBeenCalled();
  if (connectorKey === "mysql") expect(within(group).getByText("New tables")).toBeTruthy();
});

it.each(["postgres", "mysql", "clickhouse"])("shows discovery and locks only dependent %s settings", connectorKey => {
  const view = render(<Form connectorKey={connectorKey} />);
  const group = view.getByRole("group", { name: "Table settings" });
  expect(view.queryByText("Required")).toBeNull();
  expect(view.getByText("Discover tables to unlock table selection and transforms.")).toBeTruthy();
  expect(view.container.querySelector(".connection-dependent-status")).toBeNull();
  expect(view.queryByRole("alert")).toBeNull();
  expect((within(group).getByLabelText(/^Hide system tables/) as HTMLInputElement).disabled).toBe(true);
  expect((within(group).getByRole("radio", { name: "Selected tables" }) as HTMLButtonElement).disabled).toBe(true);
  expect((within(group).getByLabelText("Include rule 1") as HTMLInputElement).disabled).toBe(true);
  expect((within(group).getByRole("button", { name: "Remove rule 1" }) as HTMLButtonElement).disabled).toBe(true);
  expect((within(group).getByRole("button", { name: "Browse tables for Include rule 1" }) as HTMLButtonElement).disabled).toBe(true);
  expect(view.getByRole("heading", { name: "Tables" })).toBeTruthy();
  expect(within(group).getByLabelText(/^Hide system tables/).closest(".table-selection-toolbar")).toBeTruthy();
  const available = view.getByRole("button", { name: "Available tables in source" }) as HTMLButtonElement;
  expect(available.disabled).toBe(true);
  expect(available.textContent).toContain("Available tables (—)");
  expect((view.getByLabelText(/^Password/) as HTMLInputElement).disabled).toBe(false);
  expect(group.contains(view.getByText("Advanced settings"))).toBe(false);
});

it.each(["postgres", "mysql", "clickhouse"])("browses the verified %s catalog with system-table filtering", async connectorKey => {
  const tables = [{ namespace: "reports", name: "daily" }, { namespace: "information_schema", name: "tables" }];
  mockTableDiscovery().mockResolvedValue({ status: "verified", options: {}, tables });
  const view = render(<Form connectorKey={connectorKey} />);
  fireEvent.click(view.getByRole("button", { name: /Discover tables|Refresh tables/ }));
  const available = view.getByRole("button", { name: "Available tables in source" }) as HTMLButtonElement;
  await waitFor(() => expect(available.disabled).toBe(false));
  expect(available.textContent).toContain("Available tables (1)");
  available.focus();
  fireEvent.click(available);
  let dialog = view.getByRole("dialog");
  expect(within(dialog).getByText("reports.daily")).toBeTruthy();
  expect(within(dialog).queryByText("information_schema.tables")).toBeNull();
  fireEvent.click(within(dialog).getByRole("button", { name: "Close available tables" }));
  expect(document.activeElement).toBe(available);
  fireEvent.click(view.getByLabelText(/^Hide system tables/));
  expect(available.textContent).toContain("Available tables (2)");
  fireEvent.click(available);
  dialog = view.getByRole("dialog");
  expect(within(dialog).getByText("information_schema.tables")).toBeTruthy();
  expect(within(dialog).queryByRole("button", { name: /Use .* in Include/ })).toBeNull();
  expect(api.connectMetadata).toHaveBeenCalledTimes(1);
});

it.each(["postgres", "mysql", "clickhouse"])("reports an empty %s catalog without an imaginary rule in All tables", async connectorKey => {
  mockTableDiscovery().mockResolvedValue({ status: "verified", options: {}, message: null, tables: [] });
  vi.spyOn(api, "previewTables").mockResolvedValue({
    cards: [{ selected: [], excluded: [] }], issues: [{ kind: "empty_match", card: 0 }],
  });
  const view = render(<Form connectorKey={connectorKey} />);
  const all = view.getByRole("radio", { name: "All tables" }) as HTMLButtonElement;
  fireEvent.click(view.getByRole("button", { name: /Discover tables|Refresh tables/ }));
  await waitFor(() => expect(all.disabled).toBe(false));
  fireEvent.click(all);
  const status = view.container.querySelector(".table-selection-status")!;
  const footer = view.container.querySelector(".table-selection-footer")!;
  const matched = within(footer as HTMLElement).getByRole("button");
  const controls = Array.from(footer.children);
  await waitFor(() => expect(status.textContent).toBe("No tables available for transfer."));
  expect(status.getAttribute("title")).toBe("No tables available for transfer.");
  expect(status.classList.contains("has-error")).toBe(true);
  expect(view.queryByText(/Rule \d+ selects no tables/)).toBeNull();
  expect(view.queryByLabelText("Include rule 1")).toBeNull();
  expect(view.getByRole("button", { name: "All matched tables 0" })).toBe(matched);
  expect(view.container.querySelector(".table-selection-status")).toBe(status);
  expect(Array.from(footer.children)).toEqual(controls);

  fireEvent.click(view.getByRole("radio", { name: "Selected tables" }));
  expect(status.textContent).not.toContain("No tables available for transfer.");
  fireEvent.input(view.getByLabelText("Include rule 1"), { target: { value: "db.*" } });
  await waitFor(() => expect(status.textContent).toBe("Rule 1 selects no tables."));
});

it("includes the MySQL new-table policy in the same locked group for stream deliveries", () => {
  const view = render(<Form connectorKey="mysql" deliveryType="stream" />);
  const group = view.getByRole("group", { name: "Table settings" });
  const policy = view.container.querySelector('[data-field-name="new_tables"]')!;
  expect(group.contains(policy)).toBe(true);
  expect((policy.querySelector("button") as HTMLButtonElement).disabled).toBe(true);
});

it.each(["postgres", "mysql", "clickhouse"])("unlocks %s only after verification without replacing controls or moving their slots", async connectorKey => {
  let finish!: (result: ConnectionCheckResult) => void;
  mockTableDiscovery().mockReturnValue(new Promise(resolve => { finish = resolve; }));
  const view = render(<Form connectorKey={connectorKey} fullWidth />);
  const group = view.getByRole("group", { name: "Table settings" });
  const check = view.getByRole("button", { name: /Discover tables|Refresh tables/ });
  const tables = view.getByRole("region", { name: "Source tables" });
  const column = tables.querySelector(":scope > .island-form")!;
  expect(column).not.toBeNull();
  expect(column.contains(group)).toBe(true);
  expect(column.contains(check)).toBe(true);
  expect(column.contains(view.getByRole("button", { name: "Available tables in source" }))).toBe(true);
  const input = within(group).getByLabelText("Include rule 1") as HTMLInputElement;
  const checkbox = within(group).getByLabelText(/^Hide system tables/) as HTMLInputElement;
  const status = view.container.querySelector(".table-discovery-result")!;
  const siblings = Array.from(group.parentElement!.parentElement!.children);
  const controls = Array.from(group.querySelectorAll("input, button"));
  check.focus();
  fireEvent.click(check);
  expect(check.getAttribute("aria-busy")).toBe("true");
  expect(check.getAttribute("aria-disabled")).toBe("true");
  expect(view.getByText("Discovering tables…")).toBeTruthy();
  expect(tables.querySelector(":scope > .island-form")).toBe(column);
  expect(input.disabled).toBe(true);
  fireEvent.click(check);
  expect(api.connectMetadata).toHaveBeenCalledTimes(1);
  // An empty catalog is successful discovery, not an unavailable catalog.
  finish({ status: "verified", options: {}, message: null, tables: [] });
  await waitFor(() => expect(input.disabled).toBe(false));
  expect(view.getByText("Tables discovered")).toBeTruthy();
  expect(tables.querySelector(":scope > .island-form")).toBe(column);
  expect(checkbox.disabled).toBe(false);
  expect(input.closest(".required-incomplete")).toBeTruthy();
  expect(document.activeElement).toBe(check);
  expect(view.getByRole("group", { name: "Table settings" })).toBe(group);
  expect(view.container.querySelector(".table-discovery-result")).toBe(status);
  expect(Array.from(group.parentElement!.parentElement!.children)).toEqual(siblings);
  expect(Array.from(group.querySelectorAll("input, button"))).toEqual(controls);

  fireEvent.click(checkbox);
  expect(input.disabled).toBe(false);
  expect(api.connectMetadata).toHaveBeenCalledTimes(1);
  fireEvent.input(view.getByLabelText(/^Password/), { target: { value: "changed" } });
  expect(input.disabled).toBe(true);
  expect(view.getByText("Discover tables to unlock table selection and transforms.")).toBeTruthy();
});

it.each([
  { status: "network_reachable", message: "Authentication was not checked.", options: {}, tables: [] },
  { status: "verified", message: null, options: {} },
] satisfies ConnectionCheckResult[])("keeps settings locked for incomplete discovery: $status / $message", async result => {
  mockTableDiscovery().mockResolvedValue(result);
  const view = render(<Form />);
  fireEvent.click(view.getByRole("button", { name: /Discover tables|Refresh tables/ }));
  await waitFor(() => expect(view.getByRole("button", { name: /Discover tables|Refresh tables/ }).getAttribute("aria-busy")).toBe("false"));
  expect((view.getByLabelText("Include rule 1") as HTMLInputElement).disabled).toBe(true);
  expect(view.queryByText("Tables discovered")).toBeNull();
  expect(view.getByText(result.message ?? "An authenticated table catalog is unavailable. Discover tables again.")).toBeTruthy();
});

it("keeps the same status region and locked settings after failed discovery", async () => {
  mockTableDiscovery().mockRejectedValue(new Error("Authentication failed. Enter a password."));
  const view = render(<Form />);
  const status = view.container.querySelector(".table-discovery-result");
  fireEvent.click(view.getByRole("button", { name: /Discover tables|Refresh tables/ }));
  expect(await view.findByRole("alert")).toBe(status);
  expect(status?.textContent).toContain("Authentication failed. Enter a password.");
  expect((view.getByLabelText("Include rule 1") as HTMLInputElement).disabled).toBe(true);
});

it("locks the existing table controls immediately during a refresh", async () => {
  const request = mockTableDiscovery()
    .mockResolvedValueOnce({ status: "verified", options: {}, message: null, tables: [] })
    .mockImplementationOnce(() => new Promise(() => undefined));
  const view = render(<Form />);
  const check = view.getByRole("button", { name: /Discover tables|Refresh tables/ });
  const input = view.getByLabelText("Include rule 1") as HTMLInputElement;
  fireEvent.click(check);
  await waitFor(() => expect(input.disabled).toBe(false));
  fireEvent.click(check);
  expect(input.disabled).toBe(true);
  expect(view.getByText("Discovering tables…")).toBeTruthy();
  expect(view.getByLabelText("Include rule 1")).toBe(input);
  expect(request).toHaveBeenCalledTimes(2);
});

it("does not unlock from stale discovery after the credentials change", async () => {
  let finish!: (result: ConnectionCheckResult) => void;
  mockTableDiscovery().mockReturnValue(new Promise(resolve => { finish = resolve; }));
  const view = render(<Form />);
  fireEvent.click(view.getByRole("button", { name: /Discover tables|Refresh tables/ }));
  fireEvent.input(view.getByLabelText(/^Password/), { target: { value: "changed" } });
  await waitFor(() => expect(view.getByText("Discover tables to unlock table selection and transforms.")).toBeTruthy());
  finish({ status: "verified", options: {}, message: null, tables: [] });
  await Promise.resolve();
  expect((view.getByLabelText("Include rule 1") as HTMLInputElement).disabled).toBe(true);
  expect(view.queryByText("Tables discovered")).toBeNull();
});

it("does not unlock editing in a read-only delivery after verification", async () => {
  mockTableDiscovery().mockResolvedValue({ status: "verified", options: {}, message: null, tables: [] });
  const view = render(<Form readOnly />);
  fireEvent.click(view.getByRole("button", { name: /Discover tables|Refresh tables/ }));
  await view.findByText("Tables discovered");
  expect((view.getByLabelText("Include rule 1") as HTMLInputElement).disabled).toBe(true);
});

it.each([{ connectorKey: "clickhouse", role: "sink" as const }, { connectorKey: "opensearch", role: "source" as const }])(
  "does not make ordinary checks mandatory for $connectorKey $role", props => {
    const view = render(<Form {...props} />);
    expect(view.queryByText("Required")).toBeNull();
    const group = view.queryByRole("group", { name: "Table settings" }) as HTMLFieldSetElement | null;
    if (group) expect(group.disabled).toBe(false);
    expect(view.queryByRole("button", { name: "Discover tables" })).toBeNull();
  },
);
