import { useMemo, useState } from "preact/hooks";
import type { UiCatalog } from "../generated/apiContract";
import { orderedEndpointConnectors } from "../connectorCatalog";
import { AutofillResistantInput } from "./AutofillResistantField";
import { Button } from "./Button";

export function TypeMappingBrowser({ catalog, role, initialConnector }: {
  catalog: UiCatalog;
  role: "source" | "sink";
  initialConnector?: string | undefined;
}) {
  const [connectorKey, setConnectorKey] = useState(initialConnector);
  const [connectorSearch, setConnectorSearch] = useState("");
  const [typeSearch, setTypeSearch] = useState("");
  const connectors = useMemo(() => orderedEndpointConnectors(catalog, role), [catalog, role]);
  const selected = connectors.find((c) => c.key === connectorKey) ?? connectors[0];
  const mapping = selected?.[role]?.type_mapping;
  const query = typeSearch.trim().toLocaleLowerCase();
  const rows = mapping?.rows.filter((row) => [row.input, row.output, row.error].some((text) => text?.toLocaleLowerCase().includes(query))) ?? [];
  return <div class="type-mapping-browser">
    <nav class="type-mapping-connectors" aria-label={role === "source" ? "Sources" : "Destinations"}>
      <AutofillResistantInput type="search" aria-label="Find connector" placeholder="Find connector" value={connectorSearch}
        onInput={(e) => setConnectorSearch(e.currentTarget.value)} />
      <div class="type-mapping-connector-list">
        {connectors.filter((c) => c.title.toLocaleLowerCase().includes(connectorSearch.trim().toLocaleLowerCase())).map((c) =>
          <Button key={c.key} variant="plain" aria-pressed={selected?.key === c.key} onClick={() => { setConnectorKey(c.key); setTypeSearch(""); }}>{c.title}</Button>)}
      </div>
    </nav>
    <section class="type-mapping-detail" aria-label="Type mappings">
      <div class="type-mapping-heading"><h3>{role === "source" ? `${selected?.title ?? "Source"} → Arrow` : `Arrow → ${selected?.title ?? "Destination"}`}</h3>
        <small>{mapping ? "From runtime resolvers" : "Connector catalog"}</small></div>
      <p class="type-mapping-context">{mapping?.context ?? "This endpoint does not publish a native type mapping. For message and file endpoints, the selected parser or serialization format determines the data schema."}</p>
      <AutofillResistantInput type="search" aria-label="Find type" placeholder="Find a type or rejection reason" value={typeSearch} onInput={(e) => setTypeSearch(e.currentTarget.value)} />
      <div class="type-mapping-table-scroll" tabIndex={0} aria-label="Mapping examples">
        <table class="type-mapping-table">
          <thead><tr><th scope="col">{role === "source" ? "Source type" : "Arrow type"}</th><th scope="col">{role === "source" ? "Arrow type" : "Destination type"}</th><th scope="col">Details</th></tr></thead>
          <tbody>{rows.map((row) => <tr key={row.input}><th scope="row"><code>{row.input}</code></th><td><code>{row.output ?? "Not supported"}</code></td><td class={row.error ? "type-mapping-rejected" : ""}>{row.error ?? "Resolved"}</td></tr>)}</tbody>
        </table>
        {rows.length === 0 && <p class="type-mapping-empty">{mapping?.rows.length ? "No matching types." : "No native type examples for this endpoint."}</p>}
      </div>
    </section>
  </div>;
}
