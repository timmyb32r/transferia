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
  TableSelectionPreviewRequest,
  CreateDraftRequest,
  MessagePreviewRequest,
  OptionsRequest,
  RevisionRequest,
  SqlPlaygroundRequest,
  SpeedtestEstimateRequest,
  SpeedtestTuneRequest,
  StopRequest,
  UpdateDraftRequest,
  YamlRequest,
} from "../../generated/apiContract";
import {
  API_ROUTES,
  type ApiRouteBody,
  type ApiRouteContract,
  type ApiRouteName,
  type ApiRouteParameters,
  type ApiRouteQuery,
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

type BodyOption<Name extends ApiRouteName> = ApiRouteBody[Name] extends undefined
  ? { body?: never }
  : { body: ApiRouteBody[Name] };

type QueryOption<Name extends ApiRouteName> =
  ApiRouteQuery[Name] extends undefined
    ? { query?: never }
    : { query: ApiRouteQuery[Name] };

type RouteOptions<Name extends ApiRouteName> = BodyOption<Name> &
  QueryOption<Name> & { signal?: AbortSignal };

type RouteArguments<Name extends ApiRouteName> =
  ApiRouteBody[Name] extends undefined
    ? ApiRouteQuery[Name] extends undefined
      ? [options?: RouteOptions<Name>]
      : [options: RouteOptions<Name>]
    : [options: RouteOptions<Name>];

function routeRequest<Name extends ApiRouteName>(
  name: Name,
  parameters: ApiRouteParameters[Name],
  ...args: RouteArguments<Name>
): Promise<ApiRouteContract[Name]> {
  const options = args[0];
  const route = API_ROUTES[name];
  const query = options?.query;
  const search = new URLSearchParams();
  if (query !== undefined) {
    for (const [key, value] of Object.entries(query)) {
      if (value !== undefined) search.set(key, String(value));
    }
  }
  const path = `${routePath(name, parameters)}${search.size === 0 ? "" : `?${search}`}`;
  const body = options?.body;
  return request(path, route.response, {
    ...(options?.signal === undefined ? {} : { signal: options.signal }),
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    method: route.method,
  }) as Promise<ApiRouteContract[Name]>;
}

function routePath(
  name: ApiRouteName,
  parameters: Record<string, string>,
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
      { ...(signal === undefined ? {} : { signal }) },
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
        body,
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
  checkConnection: (body: ConnectionCheckRequest, signal?: AbortSignal) =>
    routeRequest(
      "check_connection",
      {},
      {
        body,
        ...(signal === undefined ? {} : { signal }),
      },
    ),
  previewTables: (body: TableSelectionPreviewRequest, signal?: AbortSignal) =>
    routeRequest("table_selection_preview", {}, { body, ...(signal === undefined ? {} : { signal }) }),
  previewMessage: (body: MessagePreviewRequest, signal?: AbortSignal) =>
    routeRequest(
      "preview_message",
      {},
      {
        body,
        ...(signal === undefined ? {} : { signal }),
      },
    ),
  sqlPlayground: (body: SqlPlaygroundRequest, signal?: AbortSignal) =>
    routeRequest(
      "sql_playground",
      {},
      {
        body,
        ...(signal === undefined ? {} : { signal }),
      },
    ),
  speedtestEstimate: (
    body: SpeedtestEstimateRequest,
    signal?: AbortSignal,
  ) =>
    routeRequest(
      "speedtest_estimate",
      {},
      {
        body,
        ...(signal === undefined ? {} : { signal }),
      },
    ),
  speedtestTune: (body: SpeedtestTuneRequest, signal?: AbortSignal) =>
    routeRequest(
      "speedtest_tune",
      {},
      {
        body,
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
    const query = {
      limit_bytes: 128 * 1024,
      ...(cursor === undefined ? {} : { cursor }),
    };
    return routeRequest(
      "worker_log",
      { id, worker_id: workerId },
      { query, ...(signal === undefined ? {} : { signal }) },
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
        body,
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
        body,
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
        body,
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
        body,
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
        body,
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
        body,
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
        body,
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
        body,
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
        body,
        ...(signal === undefined ? {} : { signal }),
      },
    );
  },
};
