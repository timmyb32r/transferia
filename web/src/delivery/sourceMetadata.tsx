import { createContext } from "preact";
import { useContext, useEffect } from "preact/hooks";
import { useEndpointActions } from "./useEndpointActions";
import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import type { DeliveryType, MetadataStatus } from "../generated/apiContract";
import type { JsonObject } from "../json";

export type SourceMetadata = ReturnType<typeof useSourceMetadata>;
export const SourceMetadataContext = createContext<SourceMetadata | undefined>(undefined);
export const useSourceMetadataContext = () => useContext(SourceMetadataContext);

export function useSourceMetadata({ connector, config, mode, sessionKey, validating }: {
  connector: string; config: JsonObject; mode: DeliveryType | undefined;
  sessionKey: string; validating: boolean;
}) {
  const api = useControlPlane();
  const actions = useEndpointActions({ api, connector, config, role: "source", metadataMode: mode, sessionKey });
  const metadata = actions.check.state === "success" ? actions.check.metadata : undefined;
  const metadataError = actions.check.state === "success" ? actions.check.metadataError : undefined;
  const poll = metadata !== undefined && !metadataError && (metadata.loading || validating);
  useEffect(() => {
    if (!metadata || !poll) return;
    const request = new AbortController();
    let timer: number | undefined;
    const refresh = async () => {
      try {
        const status = await api.metadataStatus(metadata.id, request.signal);
        if (!request.signal.aborted) actions.updateMetadata(status);
      } catch (error) {
        if (!request.signal.aborted) actions.metadataFailed(metadata.id,
          `Metadata status unavailable: ${error instanceof Error ? error.message : String(error)}. Connect & load metadata to retry.`);
        return;
      }
      if (!request.signal.aborted) timer = window.setTimeout(() => { void refresh(); }, 500);
    };
    void refresh();
    return () => { request.abort(); window.clearTimeout(timer); };
  }, [metadata?.id, poll, api, actions.updateMetadata, actions.metadataFailed]);
  return { ...actions, metadata, metadataError };
}

export function metadataSummary(metadata: MetadataStatus): string {
  const loaded = metadata.loaded.length;
  const failed = metadata.errors.length;
  if (metadata.loading) return `Schemas loaded ${loaded}/${metadata.catalog_count}${failed ? ` · ${failed} failed` : ""}`;
  if (failed) return `Schemas loaded ${loaded}/${metadata.catalog_count} · ${failed} failed`;
  if (loaded === metadata.catalog_count) return `Schemas cached ${loaded}/${metadata.catalog_count}`;
  return `Schemas cached ${loaded}/${metadata.catalog_count} · Load matching schemas in Transforms`;
}
