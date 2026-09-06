import { render } from "preact";
import { useState } from "preact/hooks";
import { Button } from "../../src/ui/Button";
import type { JsonValue } from "../../src/json";
import { compileSchema } from "../../src/schema/compiler";
import { SchemaForm } from "../../src/schema/SchemaForm";
import { WidgetRegistryProvider } from "../../src/schema/widgetRegistry";
import { productionWidgetRegistry } from "../../src/features/formWidgetRegistry";
import catalogFixture from "../../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import "../../src/style.css";

// Visual fixture only. Matcher correctness is covered by the Rust evaluator
// suite; this fixture supplies a deliberately large authenticated catalog.
const tables = Array.from({ length: 40 }, (_, index) => ({ namespace: "analytics", name: `reports_${index}` }));
const catalog = { tables, preview: async () => ({ cards: [{ selected: tables, excluded: [] }], issues: [] }) };
const source = catalogFixture.connectors.find(connector => connector.key === "clickhouse")!.source!;
const compiled = compileSchema(source.schema, productionWidgetRegistry);
if (compiled.kind !== "object") throw new Error("Expected ClickHouse source object");
// Render the relevant real catalog fields through SchemaForm, including its
// ordering and connection-action anchor, not a lookalike form.
const node = { ...compiled, properties: Object.fromEntries(Object.entries(compiled.properties)
  .filter(([name]) => ["password", "hide_system_tables", "tables"].includes(name))) };
function Fixture() {
  const [verified, setVerified] = useState(false);
  const [value, setValue] = useState<JsonValue>({ password: "", hide_system_tables: true,
    tables: { type: "selected", rules: [{ include: "analytics.reports_*", include_mode: "glob" }] } });
  return <main style={{ padding: "24px", maxWidth: "760px", margin: "auto" }}>
    <h1>Table selection · visual smoke fixture</h1>
    <section class="endpoint-card panel" style={{ marginTop: "16px", background: "var(--panel2)", padding: "24px" }}>
      <WidgetRegistryProvider registry={productionWidgetRegistry}>
        <SchemaForm node={node} value={value} onChange={setValue} tableCatalog={verified ? catalog : undefined}
          connectionAction={<Button onClick={() => setVerified(!verified)}>{verified ? "Invalidate connection" : "Provide verified catalog"}</Button>} />
      </WidgetRegistryProvider>
      <Button style={{ marginTop: "16px" }}>Following control</Button>
    </section>
  </main>;
}
render(<Fixture />, document.getElementById("fixture")!);
