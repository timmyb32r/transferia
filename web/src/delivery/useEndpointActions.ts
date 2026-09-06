import { useEffect, useRef, useState } from "preact/hooks";

import type { ControlPlanePort } from "../application/ports/controlPlane";
import type {
  ConnectionCheckStatus,
  TableIdentity,
  MessagePreviewResult,
} from "../generated/apiContract";
import type { JsonObject } from "../types";

export function tableConnectionIdentity(connector: string, config: JsonObject): string | undefined {
  const { tables, ...connection } = config;
  if (tables === null || typeof tables !== "object" || Array.isArray(tables) || (tables.type !== "selected" && tables.type !== "all")) return undefined;
  return `${connector}:${JSON.stringify(connection)}`;
}

export type ConnectionCheckState =
  | { state: "idle" | "checking"; options: Record<string, string[]> }
  | {
      state: "success";
      message?: string;
      status: ConnectionCheckStatus;
      tables?: TableIdentity[];
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
}: {
  api: ControlPlanePort;
  connector: string;
  role: "source" | "sink";
  config: JsonObject;
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
  const previewController = useRef<AbortController>();
  const endpointIdentity = `${role}:${connector}`;
  // Rules depend on the checked catalog, but do not change the connection.
  const { tables: _tables, ...connectionConfig } = config;
  const configFingerprint = JSON.stringify(
    _tables !== null && typeof _tables === "object" && !Array.isArray(_tables) && (_tables.type === "selected" || _tables.type === "all")
      ? connectionConfig : config,
  );
  const previousEndpointIdentity = useRef(endpointIdentity);
  const previousConfigFingerprint = useRef(configFingerprint);

  useEffect(() => {
    if (previousEndpointIdentity.current === endpointIdentity) return;
    previousEndpointIdentity.current = endpointIdentity;
    previousConfigFingerprint.current = configFingerprint;
    checkController.current?.abort();
    previewController.current?.abort();
    checkController.current = undefined;
    previewController.current = undefined;
    setCheck({ state: "idle", options: {} });
    setPreview({ open: false, loading: false });
  }, [configFingerprint, endpointIdentity]);

  useEffect(() => {
    if (previousConfigFingerprint.current === configFingerprint) return;
    previousConfigFingerprint.current = configFingerprint;
    checkController.current?.abort();
    checkController.current = undefined;
    setCheck((current) => ({ state: "idle", options: current.options }));
  }, [configFingerprint]);

  useEffect(
    () => () => {
      checkController.current?.abort();
      previewController.current?.abort();
    },
    [],
  );

  const checkConnection = async () => {
    if (checkController.current !== undefined) return;
    const request = new AbortController();
    checkController.current = request;
    setCheck((current) => ({ state: "checking", options: current.options }));
    try {
      const result = await api.checkConnection(
        { connector, role, config },
        request.signal,
      );
      if (checkController.current !== request) return;
      setCheck({
        state: "success",
        ...(result.message == null ? {} : { message: result.message }),
        status: result.status,
        options: result.options,
        ...(result.tables === undefined ? {} : { tables: result.tables }),
      });
    } catch (error) {
      if (request.signal.aborted || checkController.current !== request) return;
      setCheck({
        state: "error",
        message: error instanceof Error ? error.message : String(error),
        options: {},
      });
    } finally {
      if (checkController.current === request)
        checkController.current = undefined;
    }
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

  return {
    check: previousConfigFingerprint.current === configFingerprint && previousEndpointIdentity.current === endpointIdentity
      ? check : { state: "idle" as const, options: {} },
    preview,
    checkConnection,
    previewMessage,
    closePreview,
  };
}
