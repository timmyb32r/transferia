import type {
  DeliveryRecord,
  DeliverySummary,
  DiscoveryResult,
  JsonObject,
  UiCatalog,
} from "./types";

interface ApiErrorEnvelope {
  error?: { code?: string; message?: string };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
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
      const envelope = JSON.parse(text) as ApiErrorEnvelope;
      message = envelope.error?.message ?? message;
    } catch {
      // Non-JSON proxy errors still produce a useful message.
    }
    throw new Error(message);
  }
  return (text ? JSON.parse(text) : undefined) as T;
}

const json = (value: unknown): string => JSON.stringify(value);

export const api = {
  catalog: (): Promise<UiCatalog> => request("/api/v1/catalog"),
  deliveries: (): Promise<DeliverySummary[]> => request("/api/v1/deliveries"),
  delivery: (id: string): Promise<DeliveryRecord> =>
    request(`/api/v1/deliveries/${encodeURIComponent(id)}`),
  create: (name: string, description: string, config: JsonObject): Promise<DeliveryRecord> =>
    request("/api/v1/deliveries", {
      method: "POST",
      body: json({ name, description, config }),
    }),
  update: (
    id: string,
    expectedRevision: number,
    name: string,
    description: string,
    config: JsonObject,
  ): Promise<DeliveryRecord> =>
    request(`/api/v1/deliveries/${encodeURIComponent(id)}`, {
      method: "PUT",
      body: json({ expected_revision: expectedRevision, name, description, config }),
    }),
  yaml: (config: JsonObject, signal?: AbortSignal): Promise<{ yaml: string }> =>
    request("/api/v1/config/yaml", {
      method: "POST",
      body: json({ config }),
      ...(signal === undefined ? {} : { signal }),
    }),
  parseYaml: (yaml: string): Promise<{ config: JsonObject }> =>
    request("/api/v1/config/from-yaml", {
      method: "POST",
      body: json({ yaml }),
    }),
  discover: (
    config: JsonObject,
    signal?: AbortSignal,
  ): Promise<DiscoveryResult> =>
    request("/api/v1/discover", {
      method: "POST",
      body: json({ config }),
      ...(signal === undefined ? {} : { signal }),
    }),
  validate: (id: string, expectedRevision: number): Promise<DiscoveryResult> =>
    request(`/api/v1/deliveries/${encodeURIComponent(id)}/validate`, {
      method: "POST",
      body: json({ expected_revision: expectedRevision }),
    }),
  activate: (id: string, expectedRevision: number): Promise<DeliveryRecord> =>
    request(`/api/v1/deliveries/${encodeURIComponent(id)}/activate`, {
      method: "POST",
      body: json({ expected_revision: expectedRevision }),
    }),
  stop: (id: string, expectedRevision: number): Promise<DeliveryRecord> =>
    request(`/api/v1/deliveries/${encodeURIComponent(id)}/stop`, {
      method: "POST",
      body: json({ expected_revision: expectedRevision }),
    }),
};
