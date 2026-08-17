import { decodeApi } from "./api/contractDecoder";
import type {
  ApiContract,
  ApiContractName,
  ConfigRequest,
  CreateDraftRequest,
  OptionsRequest,
  RevisionRequest,
  StopRequest,
  UpdateDraftRequest,
  YamlRequest,
} from "./generated/apiContract";
import type { JsonObject } from "./json";

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

export const api = {
  catalog: () => request("/api/v1/catalog", "catalog_response"),
  options: (
    key: string,
    dependencies: Record<string, string> = {},
    refresh = false,
    signal?: AbortSignal,
  ) => {
    const body: OptionsRequest = { refresh, dependencies };
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
  deliveries: () => request("/api/v1/deliveries", "delivery_list_response"),
  delivery: (id: string) =>
    request(
      `/api/v1/deliveries/${encodeURIComponent(id)}`,
      "delivery_response",
    ),
  create: (name: string, description: string, config: JsonObject) => {
    const body: CreateDraftRequest = { name, description, config };
    return request("/api/v1/deliveries", "delivery_response", {
      method: "POST",
      body: json(body),
    });
  },
  update: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    name: string,
    description: string,
    config: JsonObject,
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
      },
    );
  },
  delete: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
  ) => {
    const body: RevisionRequest = {
      expected_revision: expectedRevision,
      expected_record_version: expectedRecordVersion,
    };
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}`,
      "delivery_response",
      { method: "DELETE", body: json(body) },
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
  parseYaml: (yaml: string) => {
    const body: YamlRequest = { yaml };
    return request("/api/v1/config/from-yaml", "config_response", {
      method: "POST",
      body: json(body),
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
  ) => {
    const body: RevisionRequest = {
      expected_revision: expectedRevision,
      expected_record_version: expectedRecordVersion,
    };
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/validate`,
      "validation_response",
      { method: "POST", body: json(body) },
    );
  },
  activate: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
  ) => {
    const body: RevisionRequest = {
      expected_revision: expectedRevision,
      expected_record_version: expectedRecordVersion,
    };
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/activate`,
      "delivery_response",
      { method: "POST", body: json(body) },
    );
  },
  stop: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    expectedRunId: string,
  ) => {
    const body: StopRequest = {
      expected_revision: expectedRevision,
      expected_record_version: expectedRecordVersion,
      expected_run_id: expectedRunId,
    };
    return request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/stop`,
      "delivery_response",
      { method: "POST", body: json(body) },
    );
  },
};
