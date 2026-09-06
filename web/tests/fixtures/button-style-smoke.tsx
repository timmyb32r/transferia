import { render } from "preact";
import { useState } from "preact/hooks";
import { Button } from "../../src/ui/Button";
import { SegmentedControl } from "../../src/ui/SegmentedControl";
import { SelectControl } from "../../src/ui/SelectControl";
import { MatchedTablesDisclosure } from "../../src/features/tableSelection/MatchedTablesDisclosure";
import "../../src/style.css";

document.documentElement.dataset.theme = new URLSearchParams(location.search).get("theme") === "dark" ? "dark" : "light";

function Fixture() {
  const [pending, setPending] = useState(false);
  const [requests, setRequests] = useState(0);
  const [open, setOpen] = useState(true);
  const [mode, setMode] = useState("selected");
  const [table, setTable] = useState("logs");
  const row = { display: "flex", alignItems: "center", gap: "12px", flexWrap: "wrap" } as const;
  return <main style={{ maxWidth: "1000px", margin: "24px auto", padding: "0 20px", display: "grid", gap: "20px" }}>
    <header class="page-header">
      <div class="transfer-id-line">
        <small class="transfer-id-slot">TRANSFER ID · dttabcdefghijklmnopq</small>
        <Button variant="plain" shape="icon" class="transfer-id-copy copy-action" data-copy="id" aria-label="Copy transfer ID">
          <span class="ui-icon copy-icon" aria-hidden="true" />
        </Button>
      </div>
    </header>
    <section class="middleware-island">
      <h2>Transforms</h2>
      <p class="middleware-empty">No transforms. Rows pass through unchanged.</p>
      <div style={{ ...row, marginTop: "20px" }}>
        <Button class="middleware-add" data-action="add"><span aria-hidden="true">+</span> Add transform</Button>
        <Button class="middleware-add" disabled><span aria-hidden="true">+</span> Add transform</Button>
      </div>
    </section>
    <section class="card" style={{ background: "var(--panel2)", display: "grid", gap: "20px" }}>
      <h2>Shared form actions</h2>
      <div style={row}>
        <Button shape="add-row" data-action="column">+ Add column</Button>
        <Button shape="icon" data-action="rule" aria-label="Add table rule">+</Button>
        <Button class="parser-preview-button" data-action="preview">Preview</Button>
        <div class="middleware-strip-heading" style={{ gridTemplateColumns: "max-content", padding: 0 }}>
          <Button class="middleware-clone" data-action="clone"><span class="ui-icon copy-icon" />Clone</Button>
        </div>
        <div class="available-table-row"><span>system.query_log</span><Button variant="plain" shape="icon" class="copy-action copy-action-framed" data-copy="table" aria-label="Copy table name"><span class="ui-icon copy-icon" aria-hidden="true" /></Button></div>
      </div>
      <div style={row}>
        <Button data-action="pending" pending={pending} onClick={() => {
          setRequests(value => value + 1); setPending(true);
          setTimeout(() => setPending(false), 800);
        }}>Load tables</Button>
        <span role="status" style={{ width: "100px", fontVariantNumeric: "tabular-nums" }}>Requests: {requests}</span>
        <Button variant="primary">Check connection</Button>
        <Button variant="danger">Delete</Button>
      </div>
      <SelectControl value={table} placeholder="Table" options={[{ value: "logs", label: "system.query_log" }]} onChange={setTable} />
      <SegmentedControl value={mode} label="Tables" options={[{ value: "selected", label: "Selected tables" }, { value: "all", label: "All tables" }]} onChange={setMode} />
      <div>
        <MatchedTablesDisclosure id="matched" label="Matched tables" headerClass="table-selection-footer"
          open={open} onToggle={() => setOpen(value => !value)} tables={[{ namespace: "system", name: "query_log" }]} />
      </div>
      <div class="editor-view-tabs" role="tablist" aria-label="Configuration view">
        <Button variant="plain" role="tab" class="active" aria-selected="true">UI</Button>
        <Button variant="plain" role="tab" aria-selected="false">YAML</Button>
      </div>
      <fieldset disabled class="connection-dependent-fields">
        <Button>Add locked rule</Button>
      </fieldset>
    </section>
  </main>;
}
render(<Fixture />, document.getElementById("fixture")!);
