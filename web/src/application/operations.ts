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

export function isOperationPending(
  operation: OperationState | undefined,
): boolean {
  return (
    operation !== undefined &&
    operation.error === undefined &&
    operation.success === undefined
  );
}
