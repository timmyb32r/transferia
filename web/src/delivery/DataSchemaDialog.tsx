import { useId, useLayoutEffect, useRef } from "preact/hooks";
import type { DiscoveryResult } from "../types";
import { Button } from "../ui/Button";
import { ContractView } from "./EditorViews";

export function DataSchemaDialog({ result, error, onClose }: {
  result?: DiscoveryResult | undefined; error?: string | undefined; onClose: () => void;
}) {
  const id = useId();
  const dialog = useRef<HTMLElement>(null);
  useLayoutEffect(() => {
    const previous = document.activeElement;
    const overflow = document.documentElement.style.overflow;
    document.documentElement.style.overflow = "hidden";
    dialog.current?.querySelector<HTMLButtonElement>("button")?.focus({ preventScroll: true });
    return () => {
      document.documentElement.style.overflow = overflow;
      if (previous instanceof HTMLElement && previous.isConnected) previous.focus({ preventScroll: true });
    };
  }, []);
  return <div class="message-preview-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose(); }}>
    <section ref={dialog} class="data-schema-dialog" role="dialog" aria-modal="true" aria-labelledby={`${id}-title`}
      onKeyDown={event => {
        if (event.key === "Escape" && !event.defaultPrevented) { event.stopPropagation(); onClose(); }
        if (event.key === "Tab") {
          const controls = Array.from(dialog.current!.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), summary:not([tabindex="-1"]), [tabindex="0"]'))
            .filter(item => item.getClientRects().length > 0);
          const first = controls[0], last = controls.at(-1);
          if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus({ preventScroll: true }); }
          else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus({ preventScroll: true }); }
        }
      }}>
      <header><h2 id={`${id}-title`}>Schema viewer</h2><Button shape="icon" aria-label="Close Schema viewer" onClick={onClose}>×</Button></header>
      <div class="data-schema-dialog-content" tabIndex={0} aria-label="Discovered schemas" aria-busy={!result && !error}>
        {result ? <ContractView result={result} /> : <p role="status" aria-live="polite" class={error ? "error" : ""}>
          {error ?? "Discovering the data schema…"}
        </p>}
      </div>
    </section>
  </div>;
}
