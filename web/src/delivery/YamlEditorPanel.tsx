import { useRef, useState } from "preact/hooks";

import { AutofillResistantTextarea } from "../ui/AutofillResistantField";
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
  const [copyState, setCopyState] = useState<
    "idle" | "copying" | "copied" | "error"
  >("idle");
  const copy = async () => {
    if (copyState === "copying") return;
    setCopyState("copying");
    try {
      await navigator.clipboard.writeText(value);
      setCopyState("copied");
    } catch {
      setCopyState("error");
    }
  };
  return (
    <section class="yaml-editor card" role="tabpanel">
      <div class="card-heading">
        <div>
          <small>RUNNABLE CONFIGURATION</small>
          <h2>YAML</h2>
        </div>
        <div class="yaml-copy-action">
          <span
            class={`yaml-copy-status ${copyState}`}
            role={copyState === "error" ? "alert" : "status"}
            aria-live="polite"
          >
            {copyState === "copied"
              ? "Copied"
              : copyState === "error"
                ? "Copy failed"
                : copyState === "copying"
                  ? "Copying…"
                  : "Copy status"}
          </span>
          <Button pending={copyState === "copying"} onClick={() => void copy()}>
            Copy
          </Button>
        </div>
      </div>
      <div class="yaml-code-editor">
        <pre ref={highlight} aria-hidden="true">
          <SyntaxHighlight value={`${value}\n`} language="yaml" />
        </pre>
        <AutofillResistantTextarea
          aria-label="YAML configuration"
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
