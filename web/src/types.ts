export type JsonPrimitive = string | number | boolean | null;
export type JsonValue =
  | JsonPrimitive
  | JsonValue[]
  | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export interface JsonSchema {
  $ref?: string;
  $defs?: Record<string, JsonSchema>;
  type?: string | string[];
  title?: string;
  description?: string;
  default?: JsonValue;
  const?: JsonValue;
  enum?: JsonValue[];
  oneOf?: JsonSchema[];
  anyOf?: JsonSchema[];
  properties?: Record<string, JsonSchema>;
  required?: string[];
  items?: JsonSchema;
  minimum?: number;
  maximum?: number;
  format?: string;
  additionalProperties?: boolean | JsonSchema;
  ["x-ui"]?: Record<string, JsonValue>;
  [key: string]: unknown;
}

export interface EndpointDefinition {
  schema: JsonSchema;
  initial: JsonObject;
  delivery_modes?: Array<"batch" | "stream">;
}

export interface ProviderDefinition {
  key: string;
  title: string;
  source?: EndpointDefinition;
  sink?: EndpointDefinition;
}

export interface UiCatalog {
  common_schema: JsonSchema;
  initial: JsonObject;
  providers: ProviderDefinition[];
}

export type ValidationState =
  | { state: "draft" }
  | { state: "ready"; revision: number }
  | { state: "invalid"; revision: number; message: string };

export type RuntimeState =
  | { state: "stopped" }
  | { state: "starting" }
  | { state: "running"; pid: number }
  | { state: "stopping" }
  | { state: "failed"; message: string };

export interface DeliverySummary {
  id: string;
  name: string;
  description: string;
  revision: number;
  validation: ValidationState;
  runtime: RuntimeState;
  updated_at_ms: number;
}

export interface DeliveryRecord extends DeliverySummary {
  config: JsonObject;
  created_at_ms: number;
}

export interface ColumnView {
  name: string;
  arrow_type: string;
  nullable: boolean;
  primary_key: boolean;
  low_cardinality: boolean;
  max_length?: number;
}

export interface DatasetView {
  role: string;
  name: string;
  columns: ColumnView[];
}

export interface DiscoveryResult {
  source: string;
  sink: string;
  datasets: DatasetView[];
  sink_limits: unknown;
}
