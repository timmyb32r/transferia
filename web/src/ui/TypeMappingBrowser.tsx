import { useId, useMemo, useState } from "preact/hooks";
import type { UiCatalog } from "../generated/apiContract";
import { orderedEndpointConnectors } from "../connectorCatalog";
import { AutofillResistantInput } from "./AutofillResistantField";
import { Button } from "./Button";

export function TypeMappingBrowser({ catalog, role }: {
  catalog: UiCatalog;
  role: "source" | "sink";
}) {
  const [connectorKey, setConnectorKey] = useState<string>();
  const helpId = useId();
  const [connectorSearch, setConnectorSearch] = useState("");
  const [typeSearch, setTypeSearch] = useState("");
  const connectors = useMemo(() => orderedEndpointConnectors(catalog, role), [catalog, role]);
  const selected = connectors.find((c) => c.key === connectorKey) ?? connectors[0];
  const mapping = selected?.[role]?.type_mapping;
  const explanation = ["kafka", "logbroker", "s3"].includes(selected?.key ?? "")
    ? role === "source"
      ? "The selected parser determines the output columns and their Arrow types. This source reads messages or files and has no fixed native column-type mapping. See the parser settings and output schema in your delivery."
      : "The selected serializer or output format determines how Arrow values are written. There is no single destination type mapping for this connector. See the serializer or output-format settings in your delivery."
    : selected?.key === "discard"
      ? "Discard is a benchmark destination: it consumes and discards data without storing it. No destination schema is created and no type conversion is needed."
      : selected?.key === "data_generator"
        ? "Data generator creates synthetic benchmark data. The selected preset defines the generated schema and Arrow types; there are no external source types to convert."
        : !mapping?.rows.length
          ? mapping?.context ?? "This connector does not expose a native type mapping."
          : undefined;
  const help = `Concrete examples evaluated by production resolvers, not an exhaustive list. Parameters, extensions and configuration can affect the result.\n\n${mapping?.context ?? ""}`;
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
    <section class={`type-mapping-detail${explanation ? " type-mapping-detail-message" : ""}`} aria-label="Type mappings">
      <div class="type-mapping-heading"><h3>{explanation ? selected?.title : role === "source" ? `${selected?.title ?? "Source"} → Arrow` : `Arrow → ${selected?.title ?? "Destination"}`}</h3>
        {!explanation && <span class="help" tabIndex={0} title={help} aria-describedby={helpId} aria-label="About these mappings">
          <span aria-hidden="true">?</span>
          <span id={helpId} role="tooltip" class="visually-hidden">{help}</span>
        </span>}</div>
      {explanation ? <p class="type-mapping-explanation">{explanation}</p> : <>
      <AutofillResistantInput type="search" aria-label="Find type" placeholder="Find a type or rejection reason" value={typeSearch} onInput={(e) => setTypeSearch(e.currentTarget.value)} />
      <div class="type-mapping-table-scroll" tabIndex={0} aria-label="Mapping examples">
        <table class="type-mapping-table">
          <thead><tr><th scope="col">{role === "source" ? "Source type" : "Arrow type"}</th><th scope="col">{role === "source" ? "Arrow type" : "Destination type"}</th><th scope="col">Details</th></tr></thead>
          <tbody>{rows.map((row) => <tr key={row.input}><th scope="row"><code>{row.input}</code></th><td><code>{row.output ?? "Not supported"}</code></td><td class={row.error ? "type-mapping-rejected" : ""}>{row.error ?? "Resolved"}</td></tr>)}</tbody>
        </table>
        {rows.length === 0 && <p class="type-mapping-empty">{mapping?.rows.length ? "No matching types." : "No native type examples for this endpoint."}</p>}
      </div>
      </>}
    </section>
  </div>;
}
