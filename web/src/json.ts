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
  minItems?: number;
  minimum?: number;
  maximum?: number;
  format?: string;
  additionalProperties?: boolean | JsonSchema;
  ["x-ui"]?: Record<string, JsonValue>;
  [key: string]: unknown;
}
