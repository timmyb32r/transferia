import { render } from "preact";
import { useState } from "preact/hooks";
import { ApplicationServicesProvider } from "../../src/bootstrap/ApplicationServicesProvider";
import { MiddlewareEditor } from "../../src/features/middleware/MiddlewareEditor";
import { TableNamingProvider } from "../../src/features/tableSelection/naming";
import type { ControlPlanePort } from "../../src/application/ports/controlPlane";
import type { JsonValue } from "../../src/types";
import type { TransformPreviewResult } from "../../src/generated/apiContract";
import { Button } from "../../src/ui/Button";
import { TableCatalogContext } from "../../src/schema/tableCatalog";
import type { TableSelectionPreviewRequest } from "../../src/generated/apiContract";
import "../../src/style.css";

// Visual/interaction fixture only; backend tests exercise the production Arrow chain.
const table = { namespace: "analytics", name: "reports_daily" };
const tables = [table, { namespace: "analytics", name: "reports_monthly" }, { namespace: "analytics", name: "reports_test" }];
const columns = ["id", "country", "revenue"].map(name => ({
  name, arrow_type: name === "country" ? "Utf8" : "Int64", nullable: false,
  metadata: {},
}));
const rows = [{ id: "101", country: "DE", revenue: "1200" }, { id: "102", country: "US", revenue: "-50" }, { id: "103", country: "DE", revenue: "980" }];
const result: TransformPreviewResult = {
  before: { table, columns, rows }, after: { table, columns, rows: [rows[0]!, rows[2]!] }, applied: true,
};
const services = { controlPlane: {
  checkConnection: async () => { await new Promise(resolve => setTimeout(resolve, 500)); return { status: "verified" as const, options: {}, tables }; },
  previewTables: async (request: TableSelectionPreviewRequest) => {
    await new Promise(resolve => setTimeout(resolve, 200));
    const rules = request.selection.type === "all" ? [{ include: "*" }] : request.selection.rules;
    const matches = (value: string, pattern: string, mode = "glob") => {
      const expression = mode === "regex" ? pattern : [...pattern].map(char => char === "*" ? ".*" : char === "?" ? "." : char.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("");
      return new RegExp(`^(?:${expression})$`).test(value);
    };
    return { issues: [], cards: rules.map(rule => {
      const included = request.catalog.filter(table => matches(`${table.namespace}.${table.name}`, rule.include, rule.include_mode));
      return { selected: included.filter(table => !rule.exclude || !matches(`${table.namespace}.${table.name}`, rule.exclude, rule.exclude_mode)),
        excluded: included.filter(table => rule.exclude && matches(`${table.namespace}.${table.name}`, rule.exclude, rule.exclude_mode)) };
    }) };
  },
  previewTransforms: async () => { await new Promise(resolve => setTimeout(resolve, 1200)); return result; },
} as unknown as ControlPlanePort };
function Fixture() {
  const [value, setValue] = useState<JsonValue>([
    { tables: { include: "analytics.reports_*", exclude: "analytics.reports_test*", include_mode: "glob", exclude_mode: "glob" }, datafusion: { sql: "SELECT id, country, revenue FROM input" } },
    { tables: { include: "analytics.reports_*", include_mode: "glob", exclude_mode: "glob" }, filter: { field: "country", value: "DE" } },
    { tables: { include: "*", include_mode: "glob", exclude_mode: "glob" }, datafusion: { sql: "SELECT *, revenue * 2 AS adjusted_revenue FROM input" } },
  ]);
  return <ApplicationServicesProvider services={services}>
    <main style={{ maxWidth: "1080px", margin: "0 auto", padding: "24px" }}>
      <div class="route-composition" style={{ marginBottom: "18px" }}>
        <section class="card" style={{ padding: "20px", background: "var(--panel2)" }}><h2>Source</h2><p>ClickHouse · analytics</p></section>
        <div class="route-arrow">→</div>
        <section class="card" style={{ padding: "20px", background: "var(--panel2)" }}><h2>Destination</h2><p>Discard</p></section>
      </div>
      <section class="middleware-island">
        <TableNamingProvider connector="clickhouse"><TableCatalogContext.Provider value={{ tables, preview: services.controlPlane.previewTables }}>
          <MiddlewareEditor value={value} disabled={false} onChange={setValue}
          source={{ connector: "clickhouse", config: { tables: { type: "all" } } }} />
        </TableCatalogContext.Provider></TableNamingProvider>
      </section>
      <Button style={{ marginTop: "18px" }}>Following control</Button>
    </main>
  </ApplicationServicesProvider>;
}
render(<Fixture />, document.getElementById("fixture")!);
