import { Button } from "../ui/Button";
import type { ConnectionCheckState } from "./useEndpointActions";

export function tableSettingsReady(discovery: ConnectionCheckState): boolean {
  return discovery.state === "success" && discovery.status === "verified" && discovery.tables !== undefined;
}

export function TableDiscovery({ discovery, onDiscover }: {
  discovery: ConnectionCheckState; onDiscover: () => void;
}) {
  const pending = discovery.state === "checking";
  const ready = tableSettingsReady(discovery);
  const message = discovery.state === "error" ? discovery.message
    : ready ? "Tables discovered"
    : discovery.state === "success" ? discovery.message ?? "An authenticated table catalog is unavailable. Discover tables again."
    : pending ? "Discovering tables…" : "Discover tables to unlock table selection and transforms.";
  return <div class="table-discovery" aria-label="Table discovery">
    <Button variant="primary" class="table-discovery-button" pending={pending} onClick={onDiscover}
      title={ready ? "Reload the table catalog and discard cached schemas" : "Connect to the source and load its table catalog"}>
      <span class="metadata-button-label"><span aria-hidden="true">Discover tables</span>
        <span>{ready ? "Refresh tables" : "Discover tables"}</span></span>
    </Button>
    <span class={`table-discovery-result connection-check-${discovery.state === "success" ? ready ? "verified" : "network_reachable" : discovery.state}`}
      role={discovery.state === "error" ? "alert" : "status"} aria-atomic="true" tabIndex={0}>{message}</span>
  </div>;
}
