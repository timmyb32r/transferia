import { decodeApi } from "../../api/contractDecoder";
import type {
  ControlPlanePort,
  DynamicOptionsQuery,
} from "../../application/ports/controlPlane";
import type {
  ApiContract,
  ApiContractName,
  ConfigRequest,
  ConnectionCheckRequest,
  CreateDraftRequest,
  MessagePreviewRequest,
  OptionsRequest,
  RevisionRequest,
  SqlPlaygroundRequest,
  StopRequest,
  UpdateDraftRequest,
  YamlRequest,
} from "../../generated/apiContract";

export const OPTIONS_TRANSPORT_VERSION = "transferia-options-post-v1";
import type { JsonObject } from "../../json";

async function request<Name extends ApiContractName>(
  path: string,
  contract: Name,
  init?: RequestInit,
): Promise<ApiContract[Name]> {
  const requestInit: RequestInit = { ...init };
  if (init?.body !== undefined) {
    const headers = new Headers(init.headers);
    headers.set("content-type", "application/json");
    requestInit.headers = headers;
  }
  const response = await fetch(path, requestInit);
  const text = await response.text();
  if (!response.ok) {
    let message = text || `${response.status} ${response.statusText}`;
    try {
      message = decodeApi("error_response", JSON.parse(text), path).error
        .message;
    } catch {
      // Non-JSON proxy errors still produce a useful message.
    }
    throw new Error(message);
  }
  return decodeApi(contract, text ? JSON.parse(text) : undefined, path);
}

function json(value: object): string {
  return JSON.stringify(value);
}

export const httpControlPlane: ControlPlanePort = {
  catalog: (signal) =>
    request("/api/v1/catalog", "catalog_response", {
      ...(signal === undefined ? {} : { signal }),
    }),
  options: ({
    key,
    query,
    dependencies = {},
    refresh = false,
    signal,
  }: DynamicOptionsQuery) => {
    const body: OptionsRequest = {
      refresh,
      dependencies,
      ...(query === undefined ? {} : { query }),
    };
    return request(
      `/api/v1/options/${encodeURIComponent(key)}`,
      "dynamic_options_response",
      {
        method: "POST",
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  checkConnection: (body: ConnectionCheckRequest, signal?: AbortSignal) =>
    request("/api/v1/check-connection", "connection_check_response", {
      method: "POST",
      body: json(body),
      ...(signal === undefined ? {} : { signal }),
    }),
  previewMessage: (body: MessagePreviewRequest, signal?: AbortSignal) =>
    request("/api/v1/preview-message", "message_preview_response", {
      method: "POST",
      body: json(body),
      ...(signal === undefined ? {} : { signal }),
    }),
  sqlPlayground: (body: SqlPlaygroundRequest, signal?: AbortSignal) =>
    request("/api/v1/playground/sql", "sql_playground_response", {
      method: "POST",
      body: json(body),
      ...(signal === undefined ? {} : { signal }),
    }),
  deliveries: (signal) =>
    request("/api/v1/deliveries", "delivery_list_response", {
      ...(signal === undefined ? {} : { signal }),
    }),
  delivery: (id: string, signal?: AbortSignal) =>
    request(
      `/api/v1/deliveries/${encodeURIComponent(id)}`,
      "delivery_response",
      { ...(signal === undefined ? {} : { signal }) },
    ),
  deliveryLogs: (id: string, signal?: AbortSignal) =>
    request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/logs`,
      "worker_logs_response",
      { ...(signal === undefined ? {} : { signal }) },
    ),
  deliveryLog: (
    id: string,
    workerId: string,
    cursor?: number,
    signal?: AbortSignal,
  ) => {
    const query = new URLSearchParams({ limit_bytes: String(128 * 1024) });
    if (cursor !== undefined) query.set("cursor", String(cursor));
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/logs/${encodeURIComponent(workerId)}?${query}`,
      "worker_log_response",
      { ...(signal === undefined ? {} : { signal }) },
    );
  },
  create: (
    name: string,
    description: string,
    config: JsonObject,
    signal?: AbortSignal,
  ) => {
    const body: CreateDraftRequest = { name, description, config };
    return request("/api/v1/deliveries", "delivery_response", {
      method: "POST",
      body: json(body),
      ...(signal === undefined ? {} : { signal }),
    });
  },
  update: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    name: string,
    description: string,
    config: JsonObject,
    signal?: AbortSignal,
  ) => {
    const body: UpdateDraftRequest = {
      expected_revision: expectedRevision,
      expected_record_version: expectedRecordVersion,
      name,
      description,
      config,
    };
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}`,
      "delivery_response",
      {
        method: "PUT",
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  delete: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    signal?: AbortSignal,
  ) => {
    const body: RevisionRequest = {
      expected_revision: expectedRevision,
      expected_record_version: expectedRecordVersion,
    };
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}`,
      "delivery_response",
      {
        method: "DELETE",
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  yaml: (config: JsonObject, signal?: AbortSignal) => {
    const body: ConfigRequest = { config };
    return request("/api/v1/config/yaml", "yaml_response", {
      method: "POST",
      body: json(body),
      ...(signal === undefined ? {} : { signal }),
    });
  },
  parseYaml: (yaml: string, signal?: AbortSignal) => {
    const body: YamlRequest = { yaml };
    return request("/api/v1/config/from-yaml", "config_response", {
      method: "POST",
      body: json(body),
      ...(signal === undefined ? {} : { signal }),
    });
  },
  discover: (config: JsonObject, signal?: AbortSignal) => {
    const body: ConfigRequest = { config };
    return request("/api/v1/discover", "discovery_response", {
      method: "POST",
      body: json(body),
      ...(signal === undefined ? {} : { signal }),
    });
  },
  validate: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    signal?: AbortSignal,
  ) => {
    const body: RevisionRequest = {
      expected_revision: expectedRevision,
      expected_record_version: expectedRecordVersion,
    };
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/validate`,
      "validation_response",
      {
        method: "POST",
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  activate: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    signal?: AbortSignal,
  ) => {
    const body: RevisionRequest = {
      expected_revision: expectedRevision,
      expected_record_version: expectedRecordVersion,
    };
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/activate`,
      "delivery_response",
      {
        method: "POST",
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  stop: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    expectedRunId: string,
    signal?: AbortSignal,
  ) => {
    const body: StopRequest = {
      expected_revision: expectedRevision,
      expected_record_version: expectedRecordVersion,
      expected_run_id: expectedRunId,
    };
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/stop`,
      "delivery_response",
      {
        method: "POST",
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
};
