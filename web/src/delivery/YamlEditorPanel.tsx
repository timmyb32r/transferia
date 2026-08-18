import { useRef } from "preact/hooks";

import { Button } from "../ui/Button";
import { SyntaxHighlight } from "../ui/SyntaxHighlight";

export function YamlEditorPanel({
  value,
  disabled,
  onChange,
}: {
  value: string;
  disabled: boolean;
  onChange: (value: string) => void;
}) {
  const highlight = useRef<HTMLPreElement>(null);
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
      <div class="yaml-code-editor">
        <pre ref={highlight} aria-hidden="true">
          <SyntaxHighlight value={`${value}\n`} language="yaml" />
        </pre>
        <textarea
          aria-label="YAML configuration"
          spellcheck={false}
          value={value}
          disabled={disabled}
          onScroll={(event) => {
            if (!highlight.current) return;
            highlight.current.scrollTop = event.currentTarget.scrollTop;
            highlight.current.scrollLeft = event.currentTarget.scrollLeft;
          }}
          onInput={(event) => onChange(event.currentTarget.value)}
        />
      </div>
      <p>Switch to UI to parse this YAML and continue editing it as a form.</p>
    </section>
  );
}
