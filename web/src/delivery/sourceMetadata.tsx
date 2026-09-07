import { createContext } from "preact";
import { useContext, useEffect } from "preact/hooks";
import { useEndpointActions } from "./useEndpointActions";
import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import type { DeliveryType } from "../generated/apiContract";
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
  const metadata = actions.discovery.state === "success" ? actions.discovery.metadata : undefined;
  const metadataError = actions.discovery.state === "success" ? actions.discovery.metadataError : undefined;
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
          `Metadata status unavailable: ${error instanceof Error ? error.message : String(error)}. Use Discover tables in Tables to retry.`);
        return;
      }
      if (!request.signal.aborted) timer = window.setTimeout(() => { void refresh(); }, 500);
    };
    void refresh();
    return () => { request.abort(); window.clearTimeout(timer); };
  }, [metadata?.id, poll, api, actions.updateMetadata, actions.metadataFailed]);
  return { ...actions, metadata, metadataError };
}
