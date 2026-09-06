import { render } from "preact";
import { useMemo, useState } from "preact/hooks";
import { Button } from "../../src/ui/Button";
import type { JsonObject } from "../../src/json";
import { visibleTableCatalog } from "../../src/features/tableSelection/catalog";
import type { SelectionPreview, TableSelectionPreviewRequest } from "../../src/generated/apiContract";
import { compileSchema } from "../../src/schema/compiler";
import { SchemaForm } from "../../src/schema/SchemaForm";
import { WidgetRegistryProvider } from "../../src/schema/widgetRegistry";
import { productionWidgetRegistry } from "../../src/features/formWidgetRegistry";
import catalogFixture from "../../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import "../../src/style.css";

// Visual fixture only. Matcher correctness is covered by the Rust evaluator
// suite; this fixture supplies a deliberately large authenticated catalog.
const options = new URLSearchParams(location.search);
const tables = options.has("short") ? [{ namespace: "analytics", name: "reports_daily" }]
  : [...Array.from({ length: 40 }, (_, index) => ({ namespace: "analytics", name: `reports_${index}` })),
  { namespace: "schema", name: "reports" }, { namespace: "schema", name: "events" },
  { namespace: "information_schema", name: "TABLES" }, { namespace: "system", name: "tables" }];
// The standalone Vite fixture does not bundle the generated AJV validators
// used by the application HTTP adapter. Live visual checks call the same API.
const preview = new URLSearchParams(location.search).has("live") ? async (body: TableSelectionPreviewRequest, signal?: AbortSignal): Promise<SelectionPreview> => {
  const response = await fetch("/api/v1/table-selection/preview", {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body), signal: signal ?? null,
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}
  : async () => ({ cards: [{ selected: tables, excluded: [] }], issues: [] });
const connectorKey = options.get("connector") ?? "clickhouse";
const source = catalogFixture.connectors.find(connector => connector.key === connectorKey)!.source!;
const compiled = compileSchema(source.schema, productionWidgetRegistry);
if (compiled.kind !== "object") throw new Error("Expected ClickHouse source object");
// Render the relevant real catalog fields through SchemaForm, including its
// ordering and connection-action anchor, not a lookalike form.
const node = { ...compiled, properties: Object.fromEntries(Object.entries(compiled.properties)
  .filter(([name]) => ["password", "hide_system_tables", "tables"].includes(name))) };
function Fixture() {
  const [verified, setVerified] = useState(false);
  const [value, setValue] = useState<JsonObject>({ password: "", hide_system_tables: true,
    tables: { type: "selected", rules: [{ include: "schema*", include_mode: "glob" }] } });
  const visible = useMemo(() => visibleTableCatalog(connectorKey, value.hide_system_tables !== false, tables), [value.hide_system_tables]);
  const catalog = { tables: visible, preview };
  return <main style={{ padding: "24px", maxWidth: "760px", margin: "auto" }}>
    <h1>Table selection · visual smoke fixture</h1>
    <section class="endpoint-card panel" style={{ marginTop: "16px", background: "var(--panel2)", padding: "24px" }}>
      <WidgetRegistryProvider registry={productionWidgetRegistry}>
        <SchemaForm node={node} value={value} onChange={next => setValue(next as JsonObject)} tableCatalog={verified ? catalog : undefined}
          connectionAction={<Button onClick={() => setVerified(!verified)}>{verified ? "Invalidate connection" : "Provide verified catalog"}</Button>} />
      </WidgetRegistryProvider>
      <Button style={{ marginTop: "16px" }}>Following control</Button>
    </section>
  </main>;
}
render(<Fixture />, document.getElementById("fixture")!);
