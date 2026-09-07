// @vitest-environment jsdom
import { cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import catalogFixture from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { decodeApi } from "../src/api/contractDecoder";
import { DeliveryConfiguration } from "../src/delivery/DeliveryConfiguration";
import { selectedEndpoints, configurationReadiness } from "../src/delivery/editorConfig";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import { DELIVERY_TYPES } from "../src/recordSemantics";
import { compatibilityRoutes } from "../src/ui/CompatibilityMatrixDialog";
import { render } from "./support/render";
import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";
import type { DiscoveryResult } from "../src/types";

// Keep the production visibility gate, without mounting network-backed endpoint fields.
vi.mock("../src/delivery/EditorViews", () => ({
  EndpointCard: ({ role, showSettings }: { role: string; showSettings: boolean }) =>
    <section aria-label={role} data-settings-visible={String(showSettings)} />,
  CommonSettings: () => null,
}));
vi.mock("../src/features/variantDetails/VariantDetailsForms", () => ({
  ParserDetailsForm: () => null,
}));
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const catalog = decodeApi("catalog_response", catalogFixture, "catalog");

it.each(["iceberg", "opensearch", "ydb", "ytsaurus"])("allocates a separate Tables island for the %s source before any connection request", key => {
  const source = catalog.connectors.find(connector => connector.key === key)!.source!;
  const config = { delivery_type: "batch", source: { [key]: source.initial }, sink: { discard: {} } };
  const selection = selectedEndpoints(catalog, config, productionWidgetRegistry);
  const onConfig = vi.fn();
  const view = render(<DeliveryConfiguration catalog={catalog}
    editor={{ sessionId: key, editing: true, localRevision: 0, name: key, description: "", config,
      validation: { state: "draft" }, runtime: { state: "stopped" } }} selection={selection}
    readOnly={false} requiredErrorScope="none" onName={() => {}} onDescription={() => {}}
    onConfig={onConfig} onChooseEndpoint={() => {}} />);
  const tables = view.getByRole("region", { name: "Source tables" });
  expect(tables.parentElement?.classList.contains("route-composition")).toBe(true);
  expect(tables.previousElementSibling?.getAttribute("aria-label")).toBe("sink");
  expect(view.getAllByRole("region", { name: "Source tables" })).toHaveLength(1);
  expect(onConfig).not.toHaveBeenCalled();
});

it("connects Logbroker parser tables to transforms, matched tables and Use without database discovery", async () => {
  const table = { namespace: "", name: "events" };
  const preview = vi.spyOn(api, "previewTables").mockResolvedValue({ cards: [{ selected: [table], excluded: [] }], issues: [] });
  const discover = vi.spyOn(api, "discover");
  const metadata = vi.spyOn(api, "connectMetadata");
  const onConfig = vi.fn();
  const config = { delivery_type: "stream",
    source: { logbroker: { parser: { common: { table_name: "events" }, json_parser: { columns: [] } } } },
    sink: { clickhouse: {} }, middlewares: [{ tables: { include: "*" }, datafusion: { sql: "SELECT * FROM input" } }],
  };
  const discovered: DiscoveryResult = { source: "logbroker", sink: "clickhouse", pipeline_count: 1,
    performance_advice: [], sink_limits: { sink: "clickhouse", supported_arrow_types: [] }, datasets: [
      { name: "events", role: "Main", intermediate_columns: [], final_columns: [] },
      { name: "failed_messages", role: "DeadLetterQueue", intermediate_columns: [], final_columns: [] },
    ] };
  const configuration = (sourceDiscovery: DiscoveryResult | undefined) => <DeliveryConfiguration catalog={catalog}
    editor={{ sessionId: "lb-parser", editing: true, localRevision: 0, name: "LB", description: "", config,
      validation: { state: "draft" }, runtime: { state: "stopped" } }}
    selection={selectedEndpoints(catalog, config, productionWidgetRegistry)} sourceDiscovery={sourceDiscovery}
    readOnly={false} requiredErrorScope="none" onName={() => {}} onDescription={() => {}}
    onConfig={onConfig} onChooseEndpoint={() => {}} />;
  const view = render(configuration(undefined));
  const add = view.getByRole("button", { name: "Add transform" }) as HTMLButtonElement;
  expect(add.disabled).toBe(true);
  fireEvent.click(view.getByRole("button", { name: "Expand transform 1" }));
  expect(view.container.querySelector(".middleware-scope-status")?.textContent)
    .toBe("Complete the parser configuration and wait for its table schemas to load.");
  const include = view.getByRole("combobox", { name: "Include transform 1" });
  const available = view.getByRole("button", { name: "Available tables for transform 1" }) as HTMLButtonElement;
  const scopeStatus = view.container.querySelector(".middleware-scope-status");
  view.rerender(configuration(discovered));
  expect(view.getByRole("button", { name: "Add transform" })).toBe(add);
  expect(add.disabled).toBe(false);
  expect(available.disabled).toBe(false);
  expect(available.textContent).toContain("(1)");
  // Async catalog publication updates reserved controls/status, not the form structure.
  expect(view.getByRole("combobox", { name: "Include transform 1" })).toBe(include);
  expect(view.container.querySelector(".middleware-scope-status")).toBe(scopeStatus);
  expect(view.queryByText(/Use Discover tables in Tables first/)).toBeNull();
  await waitFor(() => expect(preview).toHaveBeenCalled());
  expect(preview.mock.calls[0]![0].catalog).toEqual([table]);
  fireEvent.click(view.getByRole("button", { name: "Matched tables for transform 1" }));
  await waitFor(() => expect(within(view.getByRole("region", { name: "Matched tables for transform 1" })).getByText("events")).toBeTruthy());
  fireEvent.click(available);
  const dialog = view.getByRole("dialog", { name: "Available tables" });
  expect(within(dialog).queryByText("failed_messages")).toBeNull();
  fireEvent.click(within(dialog).getByRole("button", { name: "Use events in Include" }));
  expect(view.queryByRole("dialog")).toBeNull();
  expect(onConfig.mock.lastCall?.[0].middlewares[0].tables.include).toBe("events");
  fireEvent.click(view.getByRole("button", { name: "Preview transform 1" }));
  expect((view.getByRole("button", { name: "Run preview" }) as HTMLButtonElement).disabled).toBe(true);
  expect(discover).not.toHaveBeenCalled();
  expect(metadata).not.toHaveBeenCalled();
  view.rerender(configuration(undefined));
  expect(add.disabled).toBe(true);
  expect(available.disabled).toBe(true);
  expect((view.getByRole("button", { name: "Clone transform 1" }) as HTMLButtonElement).disabled).toBe(true);
});
it("places one transforms island directly below source and destination", () => {
  const config = { delivery_type: "batch", source: { clickhouse: {} }, sink: { discard: {} }, middlewares: [] };
  const view = render(<DeliveryConfiguration catalog={catalog}
    editor={{ sessionId: "transforms-island", editing: true, localRevision: 0, name: "Test", description: "", config,
      validation: { state: "draft" }, runtime: { state: "stopped" } }}
    selection={selectedEndpoints(catalog, config, productionWidgetRegistry)} readOnly={false} requiredErrorScope="none"
    onName={() => {}} onDescription={() => {}} onConfig={() => {}} onChooseEndpoint={() => {}} />);
  const transforms = view.getAllByRole("region", { name: "Transforms" });
  expect(transforms).toHaveLength(1);
  const island = transforms[0]!.closest(".middleware-island");
  expect(island?.previousElementSibling?.classList.contains("route-composition")).toBe(true);
  expect(island?.nextElementSibling?.classList.contains("pipeline-section")).toBe(true);
});
it("shows destination-mode errors immediately even without a selected source", () => {
  const config = { delivery_type: "stream", sink: { ytsaurus: { tables: { type: "static_tables" } } } };
  const view = render(<DeliveryConfiguration catalog={catalog}
    editor={{ sessionId: "destination-mode", editing: true, localRevision: 0, name: "Test", description: "", config,
      validation: { state: "draft" }, runtime: { state: "stopped" } }}
    selection={selectedEndpoints(catalog, config, productionWidgetRegistry)} readOnly={false} requiredErrorScope="none"
    onName={() => {}} onDescription={() => {}} onConfig={() => {}} onChooseEndpoint={() => {}} />);
  expect(view.getByRole("status").textContent)
    .toContain("YTsaurus static tables can be used only in 'batch' delivery mode.");
  expect(view.getByText("Incompatible configuration")).toBeTruthy();
});

it.each(compatibilityRoutes(catalog).flatMap((route) => DELIVERY_TYPES.map((mode) => ({
  name: `${route.source.key} → ${route.sink.key} / ${mode}`, route, mode,
}))))("keeps settings visibility consistent with the matrix: $name", ({ route, mode }) => {
  const config = { delivery_type: mode,
    source: { [route.source.key]: route.source.source!.initial },
    sink: { [route.sink.key]: route.sink.sink!.initial },
  };
  const selection = selectedEndpoints(catalog, config, productionWidgetRegistry);
  const expected = route.supported.includes(mode);
  const configuration = (next: typeof selection) => <DeliveryConfiguration catalog={catalog}
    editor={{ sessionId: "route-test", editing: true, localRevision: 0, name: "Test", description: "", config,
      validation: { state: "draft" }, runtime: { state: "stopped" } }}
    selection={next} readOnly={false} requiredErrorScope="none"
    onName={() => {}} onDescription={() => {}} onConfig={() => {}} onChooseEndpoint={() => {}} />;
  const view = render(configuration(selection));
  const feedback = view.getByRole("status");
  const sourceSettings = view.getByRole("region", { name: "source" });
  expect(feedback.compareDocumentPosition(sourceSettings) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
  expect(feedback.nextElementSibling?.classList.contains("route-composition")).toBe(true);
  for (const role of ["source", "sink"]) {
    expect(view.getByRole("region", { name: role }).getAttribute("data-settings-visible")).toBe(String(expected));
  }
  expect(view.queryByText("Incompatible route") !== null).toBe(!expected);
  if (expected && selection.error) {
    const heading = selection.incompatibleConfiguration ? "Incompatible configuration" : "Configuration required";
    expect(view.getByText(heading)).toBeTruthy();
    const readiness = configurationReadiness(catalog, config, productionWidgetRegistry);
    expect(readiness.selection.error).toBeTruthy();
    expect(readiness.complete).toBe(false);
    const cleared = { ...selection };
    delete cleared.error;
    view.rerender(configuration(cleared));
    expect(view.getByRole("status")).toBe(feedback);
    expect(feedback.textContent).toBe("");
    expect(view.getByRole("region", { name: "source" })).toBe(sourceSettings);
    view.rerender(configuration(selection));
    expect(view.getByRole("status")).toBe(feedback);
    expect(feedback.textContent).toContain(heading);
  }
});
