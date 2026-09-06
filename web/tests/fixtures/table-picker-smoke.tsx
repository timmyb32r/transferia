import { render } from "preact";
import { useState } from "preact/hooks";
import catalog from "../../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { ApplicationServicesProvider } from "../../src/bootstrap/ApplicationServicesProvider";
import { EndpointCard } from "../../src/delivery/EndpointCard";
import { SourceMetadataContext, useSourceMetadata } from "../../src/delivery/sourceMetadata";
import { WidgetRegistryProvider } from "../../src/schema/widgetRegistry";
import { productionWidgetRegistry } from "../../src/features/formWidgetRegistry";
import type { ControlPlanePort } from "../../src/application/ports/controlPlane";
import type { ConnectorDefinition, JsonObject } from "../../src/types";
import type { MetadataStatus } from "../../src/generated/apiContract";
import { qualifiedName } from "../../src/features/tableSelection/model";
import "../../src/style.css";

// Real form and metadata lifecycle; only the transport is simulated. No credentials or DB requests.
const key = new URLSearchParams(location.search).get("connector") ?? "clickhouse";
const connector = catalog.connectors.find(item => item.key === key)! as unknown as ConnectorDefinition;
const tables = ["query_log", "query_thread_log", "query_views_log", "tables", "symbols"].map(name => ({ namespace: "system", name }));
const metadata: MetadataStatus = { id: "picker-fixture", catalog_count: tables.length, loaded: tables.slice(0, 4),
  errors: [{ table: tables[4]!, message: "Introspection functions are disabled for this user." }], loading: false };
const api = {
  connectMetadata: async () => ({ connection: { status: "verified", tables, options: {}, message: null }, metadata }),
  releaseMetadata: async () => metadata,
  previewTables: async ({ selection, catalog }) => ({ issues: [], cards: (selection.type === "all" ? [{ include: "*" }] : selection.rules).map(rule => {
    const pattern = rule.include_mode === "regex" ? rule.include : [...rule.include].map(char => char === "*" ? ".*" : char === "?" ? "." : char.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("");
    return { selected: catalog.filter(table => new RegExp(`^(?:${pattern})$`).test(qualifiedName(table))), excluded: [] };
  }) }),
} satisfies Pick<ControlPlanePort, "connectMetadata" | "releaseMetadata" | "previewTables">;

function Fixture() {
  const [tablesHost, setTablesHost] = useState<HTMLElement | null>(null);
  const [config, setConfig] = useState<JsonObject>({ delivery_type: "batch", source: { [key]: { ...connector.source!.initial,
    hide_system_tables: false, tables: { type: "selected", rules: [{ include: "system.query_*" }, { include: "system.tables" }] },
  } } });
  const source = (config.source as JsonObject)[key] as JsonObject;
  const metadata = useSourceMetadata({ connector: key, config: source, mode: "batch", sessionKey: "fixture", validating: false });
  return <SourceMetadataContext.Provider value={metadata}>
    <main class="route-composition" style={{ maxWidth: "1080px", padding: "24px", margin: "auto" }}>
      <EndpointCard title="Source" role="source" selectedKey={key} connectors={[connector]} endpoint={connector.source!}
        config={config} readOnly={false} showRequiredErrors={false} onChoose={() => {}} onConfig={setConfig} tablesHost={tablesHost} />
      <div class="route-arrow">→</div>
      <article class="card endpoint-card endpoint-card-sink"><h2>Destination</h2><p>Discard</p></article>
      <div class="source-details-bridge" aria-hidden="true" />
      <section class="source-details-card source-tables-card" ref={setTablesHost} tabIndex={-1} aria-label="Source tables" />
    </main>
  </SourceMetadataContext.Provider>;
}
render(<ApplicationServicesProvider services={{ controlPlane: api as unknown as ControlPlanePort }}>
  <WidgetRegistryProvider registry={productionWidgetRegistry}><Fixture /></WidgetRegistryProvider>
</ApplicationServicesProvider>, document.getElementById("fixture")!);
