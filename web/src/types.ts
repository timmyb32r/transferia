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
  partitioned?: boolean;
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
  | { state: "starting"; run_id: string }
  | { state: "running"; run_id: string; pid: number }
  | { state: "stopping"; run_id: string }
  | { state: "failed"; run_id: string; message: string };

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
  record_version: number;
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
  role: "Main" | "DeadLetterQueue";
  name: string;
  columns: ColumnView[];
}

export interface DiscoveryResult {
  source: string;
  sink: string;
  datasets: DatasetView[];
  sink_limits: SinkLimitsDescription;
}

export type NameSyntax =
  | "any_non_empty_utf8"
  | "ascii_identifier"
  | "object_store_path_segment";

export type ArrowTypeFamily =
  | "utf8"
  | "binary"
  | "signed_integer"
  | "unsigned_integer"
  | "floating_point"
  | "boolean"
  | "date32"
  | "date64"
  | "timestamp";

export interface TextLimit {
  syntax: NameSyntax;
  max_utf8_bytes: number | null;
}

export interface ObjectKeyLimit {
  max_utf8_bytes: number;
  normalized_relative_path: boolean;
}

export interface SinkLimitsDescription {
  sink: string;
  dataset_name: TextLimit | null;
  column_name: TextLimit | null;
  supported_arrow_types: ArrowTypeFamily[];
  object_key: ObjectKeyLimit | null;
}

export type ApiErrorCode =
  | "invalid_request"
  | "payload_too_large"
  | "not_found"
  | "conflict"
  | "validation_failed"
  | "internal_error";

export interface ApiErrorEnvelope {
  error: {
    code: ApiErrorCode;
    message: string;
  };
}

export interface DynamicOption {
  value: string;
  label: string;
}

export interface DynamicOptions {
  options: DynamicOption[];
  warning?: string;
}
