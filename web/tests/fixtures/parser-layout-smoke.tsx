import { render } from "preact";
import { useState } from "preact/hooks";
import catalog from "../../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { productionWidgetRegistry } from "../../src/features/formWidgetRegistry";
import { ParserDetailsForm } from "../../src/features/variantDetails/VariantDetailsForms";
import { compileSchema, materializeBranch } from "../../src/schema/compiler";
import { FormEnvironmentProvider } from "../../src/schema/formEnvironment";
import { WidgetRegistryProvider } from "../../src/schema/widgetRegistry";
import type { JsonValue, UiCatalog } from "../../src/types";
import "../../src/style.css";

window.addEventListener("error", event => {
  document.getElementById("fixture")!.textContent = event.message;
});

// Real catalog and parser editors, without a connection or source reads.
const params = new URLSearchParams(location.search);
const endpoint = (catalog as unknown as UiCatalog).connectors
  .find(item => item.key === (params.get("connector") ?? "kafka"))!.source!;
const node = compileSchema(endpoint.schema, productionWidgetRegistry);
if (node.kind !== "object" || node.properties.parser?.kind !== "union") throw new Error("Expected parser union");
const branch = node.properties.parser.branches.find(item => item.node.xUi.capabilities?.key === (params.get("parser") ?? "schema_registry"));
if (!branch) throw new Error("Unknown fixture parser");
const initial = { parser: materializeBranch(branch) };

function Fixture() {
  const [value, setValue] = useState<JsonValue>(initial);
  return <main class="route-composition" style={{ maxWidth: "1800px", margin: "auto", padding: "24px" }}>
    <ParserDetailsForm node={node} value={value} onChange={setValue} />
  </main>;
}
render(<FormEnvironmentProvider environment={{ options: async () => ({ options: [] }) }}>
  <WidgetRegistryProvider registry={productionWidgetRegistry}><Fixture /></WidgetRegistryProvider>
</FormEnvironmentProvider>, document.getElementById("fixture")!);
