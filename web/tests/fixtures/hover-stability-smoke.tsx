import { render } from "preact";
import { useLayoutEffect, useState } from "preact/hooks";
import { AvailableTablesDialog } from "../../src/features/tableSelection/AvailableTablesDialog";
import { AutofillResistantInput } from "../../src/ui/AutofillResistantField";
import { Button } from "../../src/ui/Button";
import { FormField } from "../../src/ui/FormField";
import "../../src/style.css";

function BooleanField({ id, type, disabled = false, external = false }: {
  id: string; type: "checkbox" | "radio"; disabled?: boolean; external?: boolean;
}) {
  const input = <AutofillResistantInput id={id} type={type} disabled={disabled} />;
  return external ? <FormField controlId={id} label={id} optional={false} description="Field help">
    <label class="toggle">{input}</label>
  </FormField> : <label class="toggle">{input}<span>{id} <strong>label</strong></span>
    <span class="help" title="Field help">?</span></label>;
}

const tables = [{ namespace: "public", name: "events" }];
function Fixture() {
  const [open, setOpen] = useState(false);
  const [status, setStatus] = useState("pending");
  useLayoutEffect(() => {
    const update = (event: Event) => setStatus((event as CustomEvent<string>).detail);
    window.addEventListener("hover-fixture-status", update);
    return () => window.removeEventListener("hover-fixture-status", update);
  }, []);
  return <main style={{ padding: "24px", maxWidth: "720px", display: "grid", gap: "16px" }}>
    {(["checkbox", "radio"] as const).map(type => <section key={type} style={{ display: "grid", gap: "12px" }}>
      <BooleanField id={`${type}-enabled`} type={type} />
      <BooleanField id={`${type}-disabled`} type={type} disabled />
      <fieldset disabled>
        <legend><BooleanField id={`${type}-legend-enabled`} type={type} /></legend>
        <BooleanField id={`${type}-fieldset-disabled`} type={type} />
      </fieldset>
    </section>)}
    <BooleanField id="external-enabled" type="checkbox" external />
    <BooleanField id="external-disabled" type="checkbox" external disabled />
    <fieldset disabled><BooleanField id="external-fieldset-disabled" type="checkbox" external /></fieldset>
    <FormField controlId="compound" label="Compound field" optional={false}>
      <AutofillResistantInput id="compound" type="text" />
      <div class="nested-section"><BooleanField id="nested-boolean" type="checkbox" /></div>
    </FormField>
    <Button onClick={() => setOpen(true)}>Browse tables</Button>
    {open && <AvailableTablesDialog catalog={{ tables, preview: async () => ({ cards: [], issues: [] }), metadata: {
      id: "hover-fixture", catalog_count: 1, loading: status === "pending",
      loaded: status === "loaded" ? tables : [],
      errors: status === "failed" ? [{ table: tables[0]!, message: "Schema failure for events" }] : [],
    } }} onUse={() => {}} onClose={() => setOpen(false)} />}
  </main>;
}
render(<Fixture />, document.getElementById("fixture")!);
