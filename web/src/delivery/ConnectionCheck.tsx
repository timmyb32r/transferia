import { Button } from "../ui/Button";
import type { ConnectionCheckState } from "./useEndpointActions";

export function tableSettingsReady(check: ConnectionCheckState): boolean {
  return check.state === "success" && check.status === "verified" && check.tables !== undefined;
}

function CheckIcon({ ready = false }: { ready?: boolean }) {
  return <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor"
    stroke-width="1.5" aria-hidden="true" focusable="false">
    <circle cx="8" cy="8" r="6.5" />
    {ready && <path d="m4.5 8 2.3 2.3 4.7-4.6" />}
  </svg>;
}

export function ConnectionCheck({ check, required, onCheck }: {
  check: ConnectionCheckState; required: boolean; onCheck: () => void;
}) {
  const checking = check.state === "checking";
  const ready = tableSettingsReady(check);
  const missingCatalog = required && check.state === "success" && check.status === "verified" && !ready;
  const tone = missingCatalog ? "network_reachable" : check.state === "success" ? check.status : check.state;
  const message = check.state === "error" ? check.message
    : check.state === "success" ? missingCatalog ? "Connection verified, but the table list is unavailable. Check again."
      : required && ready ? "Connected"
      : check.message ?? "Connection verified, including access to the configured entities."
    : required ? checking ? "Connecting and loading table names…" : "Required to unlock tables and transforms" : "";
  const row = <div class="connection-check">
    <Button variant="primary" class="connection-check-button" pending={checking} onClick={onCheck}
      title={required && ready ? "Refresh metadata: reload the table catalog and discard cached schemas" : undefined}>
      {required ? <span class="metadata-button-label">
        <span aria-hidden="true">Connect & load metadata</span>
        <span>{ready ? "Refresh metadata" : "Connect & load metadata"}</span>
      </span> : "Check connection"}
    </Button>
    {!required && <span class="connection-check-spinner-slot"
      aria-label={checking ? "Checking connection…" : undefined} role={checking ? "status" : undefined}>
      {checking && <span class="connection-check-spinner" aria-hidden="true" />}
    </span>}
    <span class={`connection-check-result connection-check-${tone}`}
      role={check.state === "error" ? "alert" : "status"} aria-atomic="true" tabIndex={message ? 0 : -1}>
      {required && <span class="connection-check-state-icon" aria-hidden="true">
        {checking ? <span class="connection-check-spinner" /> : <CheckIcon ready={ready} />}
      </span>}
      <span>{message}</span>
    </span>
  </div>;
  return required ? <section class="connection-check-required" aria-label="Required connection check">
    <div class="connection-check-label">Source metadata <span class="connection-required-badge">Required</span></div>
    {row}
  </section> : row;
}
