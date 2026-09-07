import { Button } from "../ui/Button";
import type { ConnectionCheckState } from "./useEndpointActions";

export function ConnectionCheck({ check, onCheck }: {
  check: ConnectionCheckState; onCheck: () => void;
}) {
  const checking = check.state === "checking";
  const tone = check.state === "success" ? check.status : check.state;
  const message = check.state === "error" ? check.message
    : check.state === "success" ? check.message ?? "Connection verified."
    : checking ? "Checking connection…" : "";
  return <div class="connection-check">
    <Button variant="primary" class="connection-check-button" pending={checking} onClick={onCheck}>Check connection</Button>
    <span class="connection-check-spinner-slot" aria-hidden="true">
      {checking && <span class="connection-check-spinner" />}
    </span>
    <span class={`connection-check-result connection-check-${tone}`}
      role={check.state === "error" ? "alert" : "status"} aria-atomic="true" tabIndex={message ? 0 : -1}>
      <span>{message}</span>
    </span>
  </div>;
}
