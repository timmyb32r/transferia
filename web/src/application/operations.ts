export type OperationKey =
  | "bootstrap"
  | "list"
  | "open"
  | "save"
  | "validate"
  | "action"
  | "yaml"
  | "parseYaml"
  | "discovery"
  | "poll";

export interface OperationState {
  requestId: number;
  label?: string;
  error?: string;
  success?: string;
}
