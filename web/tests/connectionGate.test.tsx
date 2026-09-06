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

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

function Form({ connectorKey = "postgres", readOnly = false, role = "source", deliveryType = "batch" }: {
  connectorKey?: string; readOnly?: boolean; role?: "source" | "sink"; deliveryType?: string;
}) {
  const connector = catalog.connectors.find(item => item.key === connectorKey)! as unknown as ConnectorDefinition;
  const endpoint = connector[role]! as EndpointDefinition;
  const [config, setConfig] = useState<JsonObject>({
    delivery_type: deliveryType,
    [role]: { [connectorKey]: endpoint.initial },
  });
  return <EndpointCard title={role === "source" ? "Source" : "Destination"} role={role}
    selectedKey={connectorKey} connectors={[connector]} endpoint={endpoint} config={config}
    readOnly={readOnly} showRequiredErrors={false} onChoose={() => undefined} onConfig={setConfig} />;
}

it.each(["postgres", "mysql", "clickhouse"])("shows the required check and locks only dependent %s settings", connectorKey => {
  const view = render(<Form connectorKey={connectorKey} />);
  const group = view.getByRole("group", { name: "Table settings" });
  expect(view.getByText("Required")).toBeTruthy();
  expect(view.getByText("Not checked")).toBeTruthy();
  expect(view.getByText("Complete a successful check to unlock table settings.")).toBeTruthy();
  expect(view.queryByRole("alert")).toBeNull();
  expect((within(group).getByLabelText(/^Hide system tables/) as HTMLInputElement).disabled).toBe(true);
  expect((within(group).getByRole("radio", { name: "Selected tables" }) as HTMLButtonElement).disabled).toBe(true);
  expect((within(group).getByRole("combobox", { name: "Include rule 1" }) as HTMLInputElement).disabled).toBe(true);
  expect((within(group).getByRole("button", { name: "Remove rule 1" }) as HTMLButtonElement).disabled).toBe(true);
  expect((view.getByLabelText(/^Password/) as HTMLInputElement).disabled).toBe(false);
  expect(group.contains(view.getByText("Advanced settings"))).toBe(false);
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
  vi.spyOn(api, "checkConnection").mockReturnValue(new Promise(resolve => { finish = resolve; }));
  const view = render(<Form connectorKey={connectorKey} />);
  const group = view.getByRole("group", { name: "Table settings" });
  const check = view.getByRole("button", { name: "Check connection" });
  const input = within(group).getByRole("combobox", { name: "Include rule 1" }) as HTMLInputElement;
  const checkbox = within(group).getByLabelText(/^Hide system tables/) as HTMLInputElement;
  const status = view.container.querySelector(".connection-check-result")!;
  const header = view.container.querySelector(".connection-dependent-status")!;
  const siblings = Array.from(group.parentElement!.parentElement!.children);
  const controls = Array.from(group.querySelectorAll("input, button"));
  check.focus();
  fireEvent.click(check);
  expect(check.getAttribute("aria-busy")).toBe("true");
  expect(check.getAttribute("aria-disabled")).toBe("true");
  expect(view.getByText("Checking connection…")).toBeTruthy();
  expect(input.disabled).toBe(true);
  fireEvent.click(check);
  expect(api.checkConnection).toHaveBeenCalledTimes(1);
  // An empty catalog is a successful check, not an unavailable catalog.
  finish({ status: "verified", options: {}, message: null, tables: [] });
  await waitFor(() => expect(input.disabled).toBe(false));
  expect(view.getByText("Connection verified")).toBeTruthy();
  expect(view.getByText("Table settings are ready.")).toBeTruthy();
  expect(checkbox.disabled).toBe(false);
  expect(document.activeElement).toBe(check);
  expect(view.getByRole("group", { name: "Table settings" })).toBe(group);
  expect(view.container.querySelector(".connection-check-result")).toBe(status);
  expect(view.container.querySelector(".connection-dependent-status")).toBe(header);
  expect(Array.from(group.parentElement!.parentElement!.children)).toEqual(siblings);
  expect(Array.from(group.querySelectorAll("input, button"))).toEqual(controls);

  fireEvent.click(checkbox);
  expect(input.disabled).toBe(false);
  expect(api.checkConnection).toHaveBeenCalledTimes(1);
  fireEvent.input(view.getByLabelText(/^Password/), { target: { value: "changed" } });
  expect(input.disabled).toBe(true);
  expect(view.getByText("Not checked")).toBeTruthy();
});

it.each([
  { status: "network_reachable", message: "Authentication was not checked.", options: {}, tables: [] },
  { status: "verified", message: null, options: {} },
] satisfies ConnectionCheckResult[])("keeps settings locked for an incomplete check: $status / $message", async result => {
  vi.spyOn(api, "checkConnection").mockResolvedValue(result);
  const view = render(<Form />);
  fireEvent.click(view.getByRole("button", { name: "Check connection" }));
  await waitFor(() => expect(view.getByRole("button", { name: "Check connection" }).getAttribute("aria-busy")).toBe("false"));
  expect((view.getByRole("combobox", { name: "Include rule 1" }) as HTMLInputElement).disabled).toBe(true);
  expect(view.queryByText("Table settings are ready.")).toBeNull();
  expect(view.getByText(result.message ?? "Connection verified, but the table list is unavailable. Check again.")).toBeTruthy();
});

it("keeps the same status region and locked settings after a failed check", async () => {
  vi.spyOn(api, "checkConnection").mockRejectedValue(new Error("Authentication failed. Enter a password."));
  const view = render(<Form />);
  const status = view.container.querySelector(".connection-check-result");
  fireEvent.click(view.getByRole("button", { name: "Check connection" }));
  expect(await view.findByRole("alert")).toBe(status);
  expect(status?.textContent).toContain("Authentication failed. Enter a password.");
  expect((view.getByRole("combobox", { name: "Include rule 1" }) as HTMLInputElement).disabled).toBe(true);
});

it("locks the existing table controls immediately during a recheck", async () => {
  const request = vi.spyOn(api, "checkConnection")
    .mockResolvedValueOnce({ status: "verified", options: {}, message: null, tables: [] })
    .mockImplementationOnce(() => new Promise(() => undefined));
  const view = render(<Form />);
  const check = view.getByRole("button", { name: "Check connection" });
  const input = view.getByRole("combobox", { name: "Include rule 1" }) as HTMLInputElement;
  fireEvent.click(check);
  await waitFor(() => expect(input.disabled).toBe(false));
  fireEvent.click(check);
  expect(input.disabled).toBe(true);
  expect(view.getByText("Checking connection…")).toBeTruthy();
  expect(view.getByRole("combobox", { name: "Include rule 1" })).toBe(input);
  expect(request).toHaveBeenCalledTimes(2);
});

it("does not unlock from a stale check after the credentials change", async () => {
  let finish!: (result: ConnectionCheckResult) => void;
  vi.spyOn(api, "checkConnection").mockReturnValue(new Promise(resolve => { finish = resolve; }));
  const view = render(<Form />);
  fireEvent.click(view.getByRole("button", { name: "Check connection" }));
  fireEvent.input(view.getByLabelText(/^Password/), { target: { value: "changed" } });
  await waitFor(() => expect(view.getByText("Not checked")).toBeTruthy());
  finish({ status: "verified", options: {}, message: null, tables: [] });
  await Promise.resolve();
  expect((view.getByRole("combobox", { name: "Include rule 1" }) as HTMLInputElement).disabled).toBe(true);
  expect(view.queryByText("Table settings are ready.")).toBeNull();
});

it("does not unlock editing in a read-only delivery after verification", async () => {
  vi.spyOn(api, "checkConnection").mockResolvedValue({ status: "verified", options: {}, message: null, tables: [] });
  const view = render(<Form readOnly />);
  fireEvent.click(view.getByRole("button", { name: "Check connection" }));
  await view.findByText("Table settings are ready.");
  expect((view.getByRole("combobox", { name: "Include rule 1" }) as HTMLInputElement).disabled).toBe(true);
});

it.each([{ connectorKey: "clickhouse", role: "sink" as const }, { connectorKey: "opensearch", role: "source" as const }])(
  "does not make ordinary checks mandatory for $connectorKey $role", props => {
    const view = render(<Form {...props} />);
    expect(view.queryByText("Required")).toBeNull();
    expect(view.queryByRole("group", { name: "Table settings" })).toBeNull();
  },
);
