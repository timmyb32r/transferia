// @vitest-environment jsdom
import { cleanup } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import catalogFixture from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { decodeApi } from "../src/api/contractDecoder";
import { DeliveryConfiguration } from "../src/delivery/DeliveryConfiguration";
import { selectedEndpoints, configurationReadiness } from "../src/delivery/editorConfig";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import { DELIVERY_TYPES } from "../src/recordSemantics";
import { compatibilityRoutes } from "../src/ui/CompatibilityMatrixDialog";
import { render } from "./support/render";

// Keep the production visibility gate, without mounting network-backed endpoint fields.
vi.mock("../src/delivery/EditorViews", () => ({
  EndpointCard: ({ role, showSettings }: { role: string; showSettings: boolean }) =>
    <section aria-label={role} data-settings-visible={String(showSettings)} />,
  CommonSettings: () => null,
}));
vi.mock("../src/features/variantDetails/VariantDetailsForms", () => ({
  ParserDetailsForm: () => null,
  SerializerDetailsForm: () => null,
}));
afterEach(cleanup);

const catalog = decodeApi("catalog_response", catalogFixture, "catalog");
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
