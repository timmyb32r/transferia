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
  const [check, setCheck] = useState<ConnectionCheckState>({
    state: "idle",
    options: {},
  });
  const [preview, setPreview] = useState<MessagePreviewState>({
    open: false,
    loading: false,
  });
  const checkController = useRef<AbortController>();
  const checkPromise = useRef<Promise<MetadataStatus | undefined>>();
  const previewController = useRef<AbortController>();
  const metadataId = useRef<string>();
  const releaseMetadata = () => {
    const id = metadataId.current;
    metadataId.current = undefined;
    if (id) void api.releaseMetadata(id).catch(() => { /* Already released or server restarted. */ });
  };
  const endpointIdentity = `${role}:${connector}:${metadataMode ?? ""}:${sessionKey ?? ""}`;
  // Rules depend on the checked catalog, but do not change the connection.
  const configFingerprint = JSON.stringify(tableConnectionConfig(connector, config) ?? config);
  const previousEndpointIdentity = useRef(endpointIdentity);
  const previousConfigFingerprint = useRef(configFingerprint);

  useEffect(() => {
    if (previousEndpointIdentity.current === endpointIdentity) return;
    previousEndpointIdentity.current = endpointIdentity;
    previousConfigFingerprint.current = configFingerprint;
    checkController.current?.abort();
    releaseMetadata();
    previewController.current?.abort();
    checkController.current = undefined;
    checkPromise.current = undefined;
    previewController.current = undefined;
    setCheck({ state: "idle", options: {} });
    setPreview({ open: false, loading: false });
  }, [configFingerprint, endpointIdentity]);

  useEffect(() => {
    if (previousConfigFingerprint.current === configFingerprint) return;
    previousConfigFingerprint.current = configFingerprint;
    checkController.current?.abort();
    releaseMetadata();
    checkController.current = undefined;
    checkPromise.current = undefined;
    setCheck((current) => ({ state: "idle", options: current.options }));
  }, [configFingerprint]);

  useEffect(
    () => () => {
      checkController.current?.abort();
      previewController.current?.abort();
      releaseMetadata();
    },
    [],
  );

  const checkConnection = (): Promise<MetadataStatus | undefined> => {
    if (checkPromise.current) return checkPromise.current;
    const request = new AbortController();
    checkController.current = request;
    const previousMetadataId = metadataId.current;
    metadataId.current = undefined;
    setCheck((current) => ({ state: "checking", options: current.options }));
    const promise = (async () => {
    try {
      const response = metadataMode === undefined ? undefined : await api.connectMetadata({
        source: { connector, config }, delivery_type: metadataMode,
        replace_metadata_id: previousMetadataId ?? null,
      }, request.signal);
      const result = response?.connection ?? await api.checkConnection({ connector, role, config }, request.signal);
      if (checkController.current !== request || request.signal.aborted) {
        if (response) void api.releaseMetadata(response.metadata.id).catch(() => {});
        return;
      }
      metadataId.current = response?.metadata.id;
      setCheck({
        state: "success",
        ...(result.message == null ? {} : { message: result.message }),
        status: result.status,
        options: result.options,
        ...(result.tables === undefined ? {} : { tables: result.tables }),
        ...(response === undefined ? {} : { metadata: response.metadata }),
      });
      return response?.metadata;
    } catch (error) {
      if (request.signal.aborted || checkController.current !== request) return;
      setCheck({
        state: "error",
        message: error instanceof Error ? error.message : String(error),
        options: {},
      });
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
    setCheck(current => current.state === "success" && current.metadata?.id === metadata.id
      ? { ...current, metadata, metadataError: undefined } : current);
  }, []);

  const metadataFailed = useCallback((id: string, message: string) => {
    setCheck(current => current.state === "success" && current.metadata?.id === id
      ? { ...current, metadata: { ...current.metadata, loading: false }, metadataError: message } : current);
  }, []);

  return {
    updateMetadata,
    metadataFailed,
    check: previousConfigFingerprint.current === configFingerprint && previousEndpointIdentity.current === endpointIdentity
      ? check : { state: "idle" as const, options: {} },
    preview,
    checkConnection,
    previewMessage,
    closePreview,
  };
}
