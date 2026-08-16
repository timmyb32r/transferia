import type {
  ApiErrorEnvelope,
  DeliveryRecord,
  DeliverySummary,
  DiscoveryResult,
  DynamicOptions,
  JsonObject,
  UiCatalog,
  ValidationCommandResult,
} from "./types";

type Decoder<T> = (value: unknown, path: string) => T;

async function request<T>(
  path: string,
  decoder: Decoder<T>,
  init?: RequestInit,
): Promise<T> {
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
  return decoder(text ? JSON.parse(text) : undefined, path);
}

const json = (value: unknown): string => JSON.stringify(value);

export const api = {
  catalog: (): Promise<UiCatalog> => request("/api/v1/catalog", decodeCatalog),
  options: (key: string, refresh = false): Promise<DynamicOptions> =>
    request(
      `/api/v1/options/${encodeURIComponent(key)}?refresh=${String(refresh)}`,
      decodeDynamicOptions,
    ),
  deliveries: (): Promise<DeliverySummary[]> =>
    request("/api/v1/deliveries", arrayOf(decodeDeliverySummary)),
  delivery: (id: string): Promise<DeliveryRecord> =>
    request(`/api/v1/deliveries/${encodeURIComponent(id)}`, decodeDeliveryRecord),
  create: (
    name: string,
    description: string,
    config: JsonObject,
  ): Promise<DeliveryRecord> =>
    request("/api/v1/deliveries", decodeDeliveryRecord, {
      method: "POST",
      body: json({ name, description, config }),
    }),
  update: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    name: string,
    description: string,
    config: JsonObject,
  ): Promise<DeliveryRecord> =>
    request(`/api/v1/deliveries/${encodeURIComponent(id)}`, decodeDeliveryRecord, {
      method: "PUT",
      body: json({
        expected_revision: expectedRevision,
        expected_record_version: expectedRecordVersion,
        name,
        description,
        config,
      }),
    }),
  yaml: (config: JsonObject, signal?: AbortSignal): Promise<{ yaml: string }> =>
    request("/api/v1/config/yaml", decodeYaml, {
      method: "POST",
      body: json({ config }),
      ...(signal === undefined ? {} : { signal }),
    }),
  parseYaml: (yaml: string): Promise<{ config: JsonObject }> =>
    request("/api/v1/config/from-yaml", decodeConfig, {
      method: "POST",
      body: json({ yaml }),
    }),
  discover: (
    config: JsonObject,
    signal?: AbortSignal,
  ): Promise<DiscoveryResult> =>
    request("/api/v1/discover", decodeDiscovery, {
      method: "POST",
      body: json({ config }),
      ...(signal === undefined ? {} : { signal }),
    }),
  validate: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
  ): Promise<ValidationCommandResult> =>
    request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/validate`,
      decodeValidation,
      {
        method: "POST",
        body: json({
          expected_revision: expectedRevision,
          expected_record_version: expectedRecordVersion,
        }),
      },
    ),
  activate: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
  ): Promise<DeliveryRecord> =>
    request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/activate`,
      decodeDeliveryRecord,
      {
        method: "POST",
        body: json({
          expected_revision: expectedRevision,
          expected_record_version: expectedRecordVersion,
        }),
      },
    ),
  stop: (
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    expectedRunId: string,
  ): Promise<DeliveryRecord> =>
    request(
      `/api/v1/deliveries/${encodeURIComponent(id)}/stop`,
      decodeDeliveryRecord,
      {
        method: "POST",
        body: json({
          expected_revision: expectedRevision,
          expected_record_version: expectedRecordVersion,
          expected_run_id: expectedRunId,
        }),
      },
    ),
};

function decodeCatalog(value: unknown, path: string): UiCatalog {
  const object = objectAt(value, path);
  objectAt(object.common_schema, `${path}.common_schema`);
  objectAt(object.initial, `${path}.initial`);
  for (const [index, providerValue] of arrayAt(object.providers, `${path}.providers`).entries()) {
    const provider = objectAt(providerValue, `${path}.providers[${index}]`);
    stringAt(provider.key, `${path}.providers[${index}].key`);
    stringAt(provider.title, `${path}.providers[${index}].title`);
    for (const role of ["source", "sink"] as const) {
      if (provider[role] === undefined) continue;
      const endpoint = objectAt(provider[role], `${path}.providers[${index}].${role}`);
      objectAt(endpoint.schema, `${path}.providers[${index}].${role}.schema`);
      objectAt(endpoint.initial, `${path}.providers[${index}].${role}.initial`);
    }
  }
  return object as unknown as UiCatalog;
}

function decodeDeliverySummary(value: unknown, path: string): DeliverySummary {
  const object = objectAt(value, path);
  for (const field of ["id", "name", "description"])
    stringAt(object[field], `${path}.${field}`);
  integerAt(object.revision, `${path}.revision`);
  integerAt(object.updated_at_ms, `${path}.updated_at_ms`);
  decodeValidationState(object.validation, `${path}.validation`);
  decodeRuntimeState(object.runtime, `${path}.runtime`);
  return object as unknown as DeliverySummary;
}

function decodeDeliveryRecord(value: unknown, path: string): DeliveryRecord {
  const object = objectAt(value, path);
  decodeDeliverySummary(object, path);
  decimalTokenAt(object.record_version, `${path}.record_version`);
  objectAt(object.config, `${path}.config`);
  integerAt(object.created_at_ms, `${path}.created_at_ms`);
  return object as unknown as DeliveryRecord;
}

function decodeValidationState(value: unknown, path: string): void {
  const object = objectAt(value, path);
  const state = stringAt(object.state, `${path}.state`);
  if (state === "draft") return;
  if (state !== "ready" && state !== "invalid") invalid(path, "unknown validation state");
  integerAt(object.revision, `${path}.revision`);
  if (state === "invalid") stringAt(object.message, `${path}.message`);
}

function decodeRuntimeState(value: unknown, path: string): void {
  const object = objectAt(value, path);
  const state = stringAt(object.state, `${path}.state`);
  if (state === "stopped") return;
  if (!["starting", "running", "stopping", "failed"].includes(state))
    invalid(path, "unknown runtime state");
  stringAt(object.run_id, `${path}.run_id`);
  if (state === "running") integerAt(object.pid, `${path}.pid`);
  if (state === "failed") stringAt(object.message, `${path}.message`);
}

function decodeDiscovery(value: unknown, path: string): DiscoveryResult {
  const object = objectAt(value, path);
  stringAt(object.source, `${path}.source`);
  stringAt(object.sink, `${path}.sink`);
  arrayAt(object.datasets, `${path}.datasets`);
  objectAt(object.sink_limits, `${path}.sink_limits`);
  return object as unknown as DiscoveryResult;
}

function decodeValidation(value: unknown, path: string): ValidationCommandResult {
  const object = objectAt(value, path);
  decodeDeliveryRecord(object.delivery, `${path}.delivery`);
  if (object.discovery !== undefined) decodeDiscovery(object.discovery, `${path}.discovery`);
  return object as unknown as ValidationCommandResult;
}

function decodeDynamicOptions(value: unknown, path: string): DynamicOptions {
  const object = objectAt(value, path);
  for (const [index, optionValue] of arrayAt(object.options, `${path}.options`).entries()) {
    const option = objectAt(optionValue, `${path}.options[${index}]`);
    stringAt(option.value, `${path}.options[${index}].value`);
    stringAt(option.label, `${path}.options[${index}].label`);
  }
  if (object.warning !== undefined) stringAt(object.warning, `${path}.warning`);
  return object as unknown as DynamicOptions;
}

function decodeYaml(value: unknown, path: string): { yaml: string } {
  const object = objectAt(value, path);
  stringAt(object.yaml, `${path}.yaml`);
  return object as { yaml: string };
}

function decodeConfig(value: unknown, path: string): { config: JsonObject } {
  const object = objectAt(value, path);
  objectAt(object.config, `${path}.config`);
  return object as { config: JsonObject };
}

function arrayOf<T>(decoder: Decoder<T>): Decoder<T[]> {
  return (value, path) =>
    arrayAt(value, path).map((item, index) => decoder(item, `${path}[${index}]`));
}

function objectAt(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    invalid(path, "expected an object");
  return value as Record<string, unknown>;
}

function arrayAt(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) invalid(path, "expected an array");
  return value;
}

function stringAt(value: unknown, path: string): string {
  if (typeof value !== "string") invalid(path, "expected a string");
  return value;
}

function integerAt(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    invalid(path, "expected a non-negative safe integer");
  return value;
}

function decimalTokenAt(value: unknown, path: string): string {
  const text = stringAt(value, path);
  if (!/^(?:0|[1-9]\d*)$/.test(text)) invalid(path, "expected a decimal integer token");
  return text;
}

function invalid(path: string, message: string): never {
  throw new Error(`Invalid control-plane response at ${path}: ${message}`);
}
