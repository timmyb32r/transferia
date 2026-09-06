import { render } from "preact";
import { useState } from "preact/hooks";
import { ApplicationServicesProvider } from "../../src/bootstrap/ApplicationServicesProvider";
import { ConnectionCheck } from "../../src/delivery/ConnectionCheck";
import { SourceMetadataContext, useSourceMetadata } from "../../src/delivery/sourceMetadata";
import { MiddlewareEditor } from "../../src/features/middleware/MiddlewareEditor";
import { TableCatalogContext } from "../../src/schema/tableCatalog";
import { TableNamingProvider } from "../../src/features/tableSelection/naming";
import type { MetadataStatus, TableIdentity } from "../../src/generated/apiContract";
import type { ControlPlanePort } from "../../src/application/ports/controlPlane";
import type { JsonValue } from "../../src/types";
import "../../src/style.css";

// Visual fixture only: production hooks and controls, simulated transport.
// No source credentials, database requests or delivery writes.
const count = new URLSearchParams(location.search).get("catalog") === "small" ? 48 : 2400;
const tables = Array.from({ length: count }, (_, index) => ({ namespace: "public", name: `events_${String(index).padStart(4, "0")}` }));
const source = { connector: "postgres", config: { host: "localhost", database: "db", username: "reader", tables: { type: "all" } } };
let current: MetadataStatus = { id: "fixture", catalog_count: count, loaded: [], errors: [], loading: false, validation: null };
let connectedAt = 0;
const delay = (milliseconds: number) => new Promise<void>(resolve => setTimeout(resolve, milliseconds));
const api = {
  connectMetadata: async () => {
    await delay(1000); connectedAt = Date.now();
    current = { ...current, id: String(connectedAt), loaded: [], loading: count < 1000 };
    return { connection: { status: "verified", tables, options: {}, message: null }, metadata: current };
  },
  metadataStatus: async () => {
    if (count < 1000) {
      const loaded = Math.min(count, Math.floor((Date.now() - connectedAt) / 100));
      current = { ...current, loaded: tables.slice(0, loaded), loading: loaded < count };
    }
    return current;
  },
  releaseMetadata: async () => current,
  loadMetadataSchemas: async (_id, request) => {
    current = { ...current, loading: true };
    await delay(1600);
    const unique = new Map([...current.loaded, ...request.tables].map(table => [JSON.stringify(table), table]));
    current = { ...current, loading: false, loaded: [...unique.values()] };
    return current;
  },
  previewTables: async request => {
    const rules = request.selection.type === "all" ? [{ include: "*" }] : request.selection.rules;
    const matches = (table: TableIdentity, pattern: string, mode = "glob") => {
      const regex = mode === "regex" ? pattern : [...pattern].map(char => char === "*" ? ".*" : char === "?" ? "." : char.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("");
      return new RegExp(`^(?:${regex})$`).test(`${table.namespace}.${table.name}`);
    };
    return { issues: [], cards: rules.map(rule => ({
      selected: request.catalog.filter(table => matches(table, rule.include, rule.include_mode) && (!rule.exclude || !matches(table, rule.exclude, rule.exclude_mode))),
      excluded: request.catalog.filter(table => rule.exclude && matches(table, rule.exclude, rule.exclude_mode)),
    })) };
  },
} satisfies Pick<ControlPlanePort, "connectMetadata" | "metadataStatus" | "releaseMetadata" | "loadMetadataSchemas" | "previewTables">;

function Fixture() {
  const metadata = useSourceMetadata({ ...source, mode: "batch", sessionKey: "fixture", validating: false });
  const [steps, setSteps] = useState<JsonValue>([{ tables: { include: "public.events_000*" }, filter: { field: "status", value: "ready" } }]);
  const catalog = metadata.check.state === "success" && metadata.check.tables
    ? { tables: metadata.check.tables, preview: api.previewTables } : undefined;
  return <SourceMetadataContext.Provider value={metadata}>
    <main style={{ maxWidth: "1080px", margin: "0 auto", padding: "24px", display: "grid", gap: "24px" }}>
      <section class="card" style={{ padding: "24px", background: "var(--panel2)" }}>
        <h2>Source · PostgreSQL</h2>
        <ConnectionCheck check={metadata.check} required onCheck={() => { void metadata.checkConnection(); }} />
      </section>
      <section class="middleware-island">
        <TableNamingProvider connector="postgres"><TableCatalogContext.Provider value={catalog}>
          <MiddlewareEditor value={steps} disabled={false} onChange={setSteps} source={source} />
        </TableCatalogContext.Provider></TableNamingProvider>
      </section>
    </main>
  </SourceMetadataContext.Provider>;
}
render(<ApplicationServicesProvider services={{ controlPlane: api as unknown as ControlPlanePort }}><Fixture /></ApplicationServicesProvider>, document.getElementById("fixture")!);
