import { useCallback, useEffect, useRef, useState } from "preact/hooks";

import type { ControlPlanePort } from "../application/ports/controlPlane";
import type {
  ConnectionCheckStatus,
  TableIdentity,
  MessagePreviewResult,
  MetadataStatus,
  DeliveryType,
} from "../generated/apiContract";
import type { JsonObject } from "../types";
import { tableConnectionConfig } from "../features/tableSelection/catalog";

export function tableConnectionIdentity(connector: string, config: JsonObject): string | undefined {
  const connection = tableConnectionConfig(connector, config);
  if (connection === undefined) return undefined;
  return `${connector}:${JSON.stringify(connection)}`;
}

export type ConnectionCheckState =
  | { state: "idle" | "checking"; options: Record<string, string[]> }
  | {
      state: "success";
      message?: string;
      status: ConnectionCheckStatus;
      tables?: TableIdentity[];
      metadata?: MetadataStatus;
      metadataError?: string | undefined;
      options: Record<string, string[]>;
    }
  | { state: "error"; message: string; options: Record<string, string[]> };

export type MessagePreviewState = {
  open: boolean;
  loading: boolean;
  result?: MessagePreviewResult;
  error?: string;
};

export function useEndpointActions({
  api,
  connector,
  role,
  config,
  metadataMode,
  sessionKey,
}: {
  api: ControlPlanePort;
  connector: string;
  role: "source" | "sink";
  config: JsonObject;
  metadataMode?: DeliveryType | undefined;
  sessionKey?: string | undefined;
}) {
  const [check, setCheck] = useState<ConnectionCheckState>({ state: "idle", options: {} });
  const checkController = useRef<AbortController>();
  const checkPromise = useRef<Promise<void>>();
  const [discovery, setDiscovery] = useState<ConnectionCheckState>({
    state: "idle",
    options: {},
  });
  const [preview, setPreview] = useState<MessagePreviewState>({
    open: false,
    loading: false,
  });
  const discoveryController = useRef<AbortController>();
  const discoveryPromise = useRef<Promise<MetadataStatus | undefined>>();
  const previewController = useRef<AbortController>();
  const metadataId = useRef<string>();
  const releaseMetadata = () => {
    const id = metadataId.current;
    metadataId.current = undefined;
    if (id) void api.releaseMetadata(id).catch(() => { /* Already released or server restarted. */ });
  };
  const endpointIdentity = `${role}:${connector}:${metadataMode ?? ""}:${sessionKey ?? ""}`;
  // Rules depend on the discovered catalog, but do not change the connection.
  const configFingerprint = JSON.stringify(tableConnectionConfig(connector, config) ?? config);
  const previousEndpointIdentity = useRef(endpointIdentity);
  const previousConfigFingerprint = useRef(configFingerprint);

  useEffect(() => {
    if (previousEndpointIdentity.current === endpointIdentity) return;
    previousEndpointIdentity.current = endpointIdentity;
    previousConfigFingerprint.current = configFingerprint;
    checkController.current?.abort();
    checkController.current = undefined;
    checkPromise.current = undefined;
    setCheck({ state: "idle", options: {} });
    discoveryController.current?.abort();
    releaseMetadata();
    previewController.current?.abort();
    discoveryController.current = undefined;
    discoveryPromise.current = undefined;
    previewController.current = undefined;
    setDiscovery({ state: "idle", options: {} });
    setPreview({ open: false, loading: false });
  }, [configFingerprint, endpointIdentity]);

  useEffect(() => {
    if (previousConfigFingerprint.current === configFingerprint) return;
    previousConfigFingerprint.current = configFingerprint;
    checkController.current?.abort();
    checkController.current = undefined;
    checkPromise.current = undefined;
    setCheck((current) => ({ state: "idle", options: current.options }));
    discoveryController.current?.abort();
    releaseMetadata();
    discoveryController.current = undefined;
    discoveryPromise.current = undefined;
    setDiscovery((current) => ({ state: "idle", options: current.options }));
  }, [configFingerprint]);

  useEffect(
    () => () => {
      checkController.current?.abort();
      discoveryController.current?.abort();
      previewController.current?.abort();
      releaseMetadata();
    },
    [],
  );

  const checkConnection = (): Promise<void> => {
    if (checkPromise.current) return checkPromise.current;
    const request = new AbortController();
    checkController.current = request;
    setCheck(current => ({ state: "checking", options: current.options }));
    const promise = (async () => {
      try {
        const result = await api.checkConnection({ connector, role, config }, request.signal);
        if (request.signal.aborted || checkController.current !== request) return;
        setCheck({ state: "success", status: result.status, options: result.options,
          ...(result.message == null ? {} : { message: result.message }) });
      } catch (error) {
        if (!request.signal.aborted && checkController.current === request)
          setCheck({ state: "error", message: error instanceof Error ? error.message : String(error), options: {} });
      } finally {
        if (checkController.current === request) {
          checkController.current = undefined;
          checkPromise.current = undefined;
        }
      }
    })();
    checkPromise.current = promise;
    return promise;
  };

  const discoverTables = (): Promise<MetadataStatus | undefined> => {
    if (discoveryPromise.current) return discoveryPromise.current;
    if (metadataMode === undefined) {
      setDiscovery({ state: "error", message: "Select a delivery type before discovering tables.", options: {} });
      return Promise.resolve(undefined);
    }
    const request = new AbortController();
    discoveryController.current = request;
    const previousMetadataId = metadataId.current;
    metadataId.current = undefined;
    setDiscovery((current) => ({ state: "checking", options: current.options }));
    const promise = (async () => {
    try {
      const response = await api.connectMetadata({
        source: { connector, config }, delivery_type: metadataMode,
        replace_metadata_id: previousMetadataId ?? null,
      }, request.signal);
      const result = response.connection;
      if (discoveryController.current !== request || request.signal.aborted) {
        void api.releaseMetadata(response.metadata.id).catch(() => {});
        return;
      }
      metadataId.current = response.metadata.id;
      setDiscovery({
        state: "success",
        ...(result.message == null ? {} : { message: result.message }),
        status: result.status,
        options: result.options,
        ...(result.tables === undefined ? {} : { tables: result.tables }),
        metadata: response.metadata,
      });
      return response.metadata;
    } catch (error) {
      if (request.signal.aborted || discoveryController.current !== request) return;
      setDiscovery({
        state: "error",
        message: error instanceof Error ? error.message : String(error),
        options: {},
      });
    } finally {
      if (discoveryController.current === request) {
        discoveryController.current = undefined;
        discoveryPromise.current = undefined;
      }
    }
    })();
    discoveryPromise.current = promise;
    return promise;
  };

  const previewMessage = async () => {
    previewController.current?.abort();
    const request = new AbortController();
    previewController.current = request;
    setPreview({ open: true, loading: true });
    try {
      const result = await api.previewMessage(
        { connector, config, max_bytes: 32 * 1024 * 1024 },
        request.signal,
      );
      if (previewController.current === request)
        setPreview({ open: true, loading: false, result });
    } catch (error) {
      if (!request.signal.aborted && previewController.current === request) {
        setPreview({
          open: true,
          loading: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  };

  const closePreview = () => {
    previewController.current?.abort();
    setPreview({ open: false, loading: false });
  };

  const updateMetadata = useCallback((metadata: MetadataStatus) => {
    setDiscovery(current => current.state === "success" && current.metadata?.id === metadata.id
      ? { ...current, metadata, metadataError: undefined } : current);
  }, []);

  const metadataFailed = useCallback((id: string, message: string) => {
    setDiscovery(current => current.state === "success" && current.metadata?.id === id
      ? { ...current, metadata: { ...current.metadata, loading: false }, metadataError: message } : current);
  }, []);

  return {
    check: previousConfigFingerprint.current === configFingerprint && previousEndpointIdentity.current === endpointIdentity
      ? check : { state: "idle" as const, options: {} },
    checkConnection,
    updateMetadata,
    metadataFailed,
    discovery: previousConfigFingerprint.current === configFingerprint && previousEndpointIdentity.current === endpointIdentity
      ? discovery : { state: "idle" as const, options: {} },
    preview,
    discoverTables,
    previewMessage,
    closePreview,
  };
}
