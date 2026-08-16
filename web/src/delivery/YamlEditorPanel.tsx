export function YamlEditorPanel({
  value,
  disabled,
  onChange,
}: {
  value: string;
  disabled: boolean;
  onChange: (value: string) => void;
}) {
  return (
    <section class="yaml-editor card" role="tabpanel">
      <div class="card-heading">
        <div>
          <small>RUNNABLE CONFIGURATION</small>
          <h2>YAML</h2>
        </div>
        <Button onClick={() => void navigator.clipboard.writeText(value)}>
          Copy
        </Button>
      </div>
      <textarea
        aria-label="YAML configuration"
        spellcheck={false}
        value={value}
        disabled={disabled}
        onInput={(event) => onChange(event.currentTarget.value)}
      />
      <p>Switch to UI to parse this YAML and continue editing it as a form.</p>
    </section>
  );
}
import { Button } from "../ui/Button";
