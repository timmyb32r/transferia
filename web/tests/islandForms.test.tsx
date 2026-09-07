// @vitest-environment jsdom

import { cleanup } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";
import catalogFixture from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { decodeApi } from "../src/api/contractDecoder";
import { EndpointCard } from "../src/delivery/EndpointCard";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import { ParserDetailsForm } from "../src/features/variantDetails/VariantDetailsForms";
import { compileSchema, materializeBranch } from "../src/schema/compiler";
import { hasEditableContent } from "../src/schema/widgetRegistry";
import { render } from "./support/render";

afterEach(cleanup);
const catalog = decodeApi("catalog_response", catalogFixture, "catalog");

describe("compact island forms", () => {
  it.each([
    ["postgres", "source"], ["mysql", "source"], ["clickhouse", "source"],
    ["mysql", "sink"], ["discard", "sink"],
  ] as const)("contains the whole %s %s form in one compact column", (key, role) => {
    const connector = catalog.connectors.find(item => item.key === key)!;
    const endpoint = connector[role]!;
    const onConfig = vi.fn();
    const view = render(<EndpointCard title={role} role={role} selectedKey={key}
      connectors={[connector]} endpoint={endpoint} config={{ [role]: { [key]: endpoint.initial } }}
      readOnly={false} showRequiredErrors={false} onChoose={() => {}} onConfig={onConfig} tablesHost={null} />);
    const column = view.container.querySelector(`.endpoint-card-${role} > .island-form`)!;
    expect(column).not.toBeNull();
    expect(column.contains(view.getByRole("heading", { name: role }))).toBe(true);
    expect(column.contains(view.getByRole("button", { name: connector.title, exact: true }))).toBe(true);
    expect(column.querySelector(".endpoint-fields")).not.toBeNull();
    expect(column.querySelector(".island-form")).toBeNull();
    expect(column.classList.contains("island-form-wide")).toBe(false);
    expect(onConfig).not.toHaveBeenCalled();
  });

  for (const key of ["kafka", "s3"]) {
    const endpoint = catalog.connectors.find(item => item.key === key)!.source!;
    const node = compileSchema(endpoint.schema, productionWidgetRegistry);
    if (node.kind !== "object" || node.properties.parser?.kind !== "union") throw new Error("Expected parser union");
    for (const branch of node.properties.parser.branches) {
      const capability = branch.node.xUi.capabilities;
      if (capability?.component !== "parser") continue;
      it(`keeps only JSON/TSKV full width for ${key}/${capability.key}, independent of its label`, () => {
        const onChange = vi.fn();
        const renamed = { ...node, properties: { ...node.properties, parser: {
          ...node.properties.parser!, kind: "union" as const,
          branches: [{ ...branch, label: "Renamed parser" }],
        } } };
        const view = render(<ParserDetailsForm node={renamed}
          value={{ parser: materializeBranch(branch) }} onChange={onChange} />);
        if (branch.constant !== undefined || !hasEditableContent(branch.node, productionWidgetRegistry)) {
          expect(view.container.querySelector(".parser-details-card")).toBeNull();
          expect(onChange).not.toHaveBeenCalled();
          return;
        }
        const column = view.container.querySelector(".parser-details-card.card > .island-form")!;
        expect(column).not.toBeNull();
        expect(column.classList.contains("island-form-wide"))
          .toBe(["json_parser", "s3_json", "tskv"].includes(capability.key));
        expect(column.contains(view.getByRole("heading", { name: "Renamed parser settings" }))).toBe(true);
        expect(column.querySelector(".island-form")).toBeNull();
        expect(onChange).not.toHaveBeenCalled();
      });
    }
  }
});
