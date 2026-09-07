import { vi } from "vitest";
import type { ConnectionCheckResult, MetadataConnectRequest, MetadataConnection } from "../../src/generated/apiContract";
import { httpControlPlane as api } from "../../src/infrastructure/controlPlane/httpControlPlane";

export function metadataResponse(connection: ConnectionCheckResult): MetadataConnection {
  return { connection, metadata: { id: "discovery", catalog_count: connection.tables?.length ?? 0,
    loaded: connection.tables ?? [], errors: [], loading: false } };
}

export function mockTableDiscovery() {
  const discover = vi.fn<(request: MetadataConnectRequest, signal?: AbortSignal) => Promise<ConnectionCheckResult>>();
  vi.spyOn(api, "connectMetadata").mockImplementation(async (request, signal) => metadataResponse(await discover(request, signal)));
  vi.spyOn(api, "releaseMetadata").mockResolvedValue(metadataResponse({ status: "verified", options: {} }).metadata);
  return discover;
}
