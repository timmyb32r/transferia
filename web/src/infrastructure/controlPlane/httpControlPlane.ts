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
import {
  API_ROUTES,
  type ApiRouteContract,
  type ApiRouteName,
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

function routeRequest<Name extends ApiRouteName>(
  name: Name,
  parameters: Record<string, string> = {},
  init?: RequestInit,
  query?: URLSearchParams,
): Promise<ApiRouteContract[Name]> {
  const route = API_ROUTES[name];
  const path = `${routePath(name, parameters)}${query === undefined ? "" : `?${query}`}`;
  return request(path, route.response, {
    ...init,
    method: route.method,
  }) as Promise<ApiRouteContract[Name]>;
}

function routePath(
  name: ApiRouteName,
  parameters: Record<string, string> = {},
): string {
  let path: string = API_ROUTES[name].path;
  for (const [key, value] of Object.entries(parameters))
    path = path.replace(`{${key}}`, encodeURIComponent(value));
  return path;
}

export const httpControlPlane: ControlPlanePort = {
  catalog: (signal) =>
    routeRequest(
      "catalog",
      {},
      {
        ...(signal === undefined ? {} : { signal }),
      },
    ),
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
    return routeRequest(
      "options",
      { key },
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  checkConnection: (body: ConnectionCheckRequest, signal?: AbortSignal) =>
    routeRequest(
      "check_connection",
      {},
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    ),
  previewMessage: (body: MessagePreviewRequest, signal?: AbortSignal) =>
    routeRequest(
      "preview_message",
      {},
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    ),
  sqlPlayground: (body: SqlPlaygroundRequest, signal?: AbortSignal) =>
    routeRequest(
      "sql_playground",
      {},
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    ),
  deliveries: (signal) =>
    routeRequest(
      "list_deliveries",
      {},
      {
        ...(signal === undefined ? {} : { signal }),
      },
    ),
  delivery: (id: string, signal?: AbortSignal) =>
    routeRequest(
      "get_delivery",
      { id },
      { ...(signal === undefined ? {} : { signal }) },
    ),
  deliveryLogs: (id: string, signal?: AbortSignal) =>
    routeRequest(
      "worker_logs",
      { id },
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
    return routeRequest(
      "worker_log",
      { id, worker_id: workerId },
      { ...(signal === undefined ? {} : { signal }) },
      query,
    );
  },
  create: (
    name: string,
    description: string,
    config: JsonObject,
    signal?: AbortSignal,
  ) => {
    const body: CreateDraftRequest = { name, description, config };
    return routeRequest(
      "create_delivery",
      {},
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
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
    return routeRequest(
      "update_delivery",
      { id },
      {
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
    return routeRequest(
      "delete_delivery",
      { id },
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  yaml: (config: JsonObject, signal?: AbortSignal) => {
    const body: ConfigRequest = { config };
    return routeRequest(
      "render_yaml",
      {},
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  parseYaml: (yaml: string, signal?: AbortSignal) => {
    const body: YamlRequest = { yaml };
    return routeRequest(
      "parse_yaml",
      {},
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  discover: (config: JsonObject, signal?: AbortSignal) => {
    const body: ConfigRequest = { config };
    return routeRequest(
      "discover",
      {},
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
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
    return routeRequest(
      "validate",
      { id },
      {
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
    return routeRequest(
      "activate",
      { id },
      {
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
    return routeRequest(
      "stop",
      { id },
      {
        body: json(body),
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
};
