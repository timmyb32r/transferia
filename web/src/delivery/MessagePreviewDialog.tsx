import { useEffect, useMemo, useRef } from "preact/hooks";

import type {
  MessagePreviewResult,
  ParserDetection,
} from "../generated/apiContract";
import { Button } from "../ui/Button";

export function MessagePreviewDialog({
  result,
  error,
  loading,
  allowApply,
  onApply,
  onClose,
}: {
  result?: MessagePreviewResult | undefined;
  error?: string | undefined;
  loading: boolean;
  allowApply: boolean;
  onApply: (detection: ParserDetection) => void;
  onClose: () => void;
}) {
  const dialog = useRef<HTMLElement>(null);
  useEffect(() => {
    dialog.current
      ?.querySelector<HTMLButtonElement>("[aria-label='Close message preview']")
      ?.focus();
    const keydown = (event: KeyboardEvent) =>
      event.key === "Escape" && onClose();
    document.addEventListener("keydown", keydown);
    return () => document.removeEventListener("keydown", keydown);
  }, [onClose]);
  const bytes = useMemo(
    () => (result ? decodeBase64(result.payload_base64) : []),
    [result],
  );
  const binary = useMemo(() => hexDump(bytes), [bytes]);
  return (
    <div class="message-preview-backdrop" onMouseDown={onClose}>
      <section
        ref={dialog}
        class="message-preview-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="message-preview-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header>
          <div>
            <h2 id="message-preview-title">Message preview</h2>
            <span>
              {result
                ? `${result.byte_length} bytes · not committed`
                : "Read-only · no commit"}
            </span>
          </div>
          <Button
            shape="icon"
            aria-label="Close message preview"
            onClick={onClose}
          >
            ×
          </Button>
        </header>
        {loading && (
          <div class="message-preview-state">
            <span class="connection-check-spinner" />
            Reading one message…
          </div>
        )}
        {error && (
          <div class="message-preview-error" role="alert">
            {error}
          </div>
        )}
        {result && (
          <>
            <div class="message-preview-panes">
              <section>
                <h3>Text</h3>
                <pre>{result.text}</pre>
              </section>
              <section>
                <h3>Binary</h3>
                <pre class="hex-editor">{binary}</pre>
              </section>
            </div>
            <footer>
              <div>
                <strong>Detected parsers</strong>
                {result.detections.length === 0 && (
                  <span class="muted">No parser recognized this message</span>
                )}
              </div>
              <div class="detected-parsers">
                {result.detections.map((detection) => (
                  <div class="detected-parser" key={detection.key}>
                    <span>{detection.label}</span>
                    <Button
                      variant="primary"
                      disabled={!allowApply}
                      title={
                        allowApply
                          ? "Use detected parser"
                          : "Enter edit mode to apply this parser"
                      }
                      onClick={() => onApply(detection)}
                    >
                      Use parser
                    </Button>
                  </div>
                ))}
              </div>
              {!allowApply && result.detections.length > 0 && (
                <span class="muted">
                  Enter edit mode to apply a detected parser.
                </span>
              )}
            </footer>
          </>
        )}
      </section>
    </div>
  );
}

function decodeBase64(value: string): number[] {
  const binary = atob(value);
  return Array.from(binary, (character) => character.charCodeAt(0));
}

function hexDump(bytes: number[]): string {
  const rows: string[] = [];
  for (let offset = 0; offset < bytes.length; offset += 16) {
    const slice = bytes.slice(offset, offset + 16);
    const address = offset.toString(16).padStart(8, "0");
    const hex = slice
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join(" ")
      .padEnd(47, " ");
    const ascii = slice
      .map((byte) =>
        byte >= 32 && byte <= 126 ? String.fromCharCode(byte) : "·",
      )
      .join("");
    rows.push(`${address}  ${hex}  ${ascii}`);
  }
  return rows.join("\n");
}
