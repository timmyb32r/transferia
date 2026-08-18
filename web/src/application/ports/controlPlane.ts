import type {
  ConnectionCheckRequest,
  ConnectionCheckResult,
  DeliveryRecord,
  DeliverySummary,
  DynamicOptions,
  MessagePreviewRequest,
  MessagePreviewResult,
  DiscoveryResult,
  SqlPlaygroundRequest,
  SqlPlaygroundResult,
  ValidationCommandResult,
  WorkerLogChunkView,
  WorkerLogsResult,
} from "../../generated/apiContract";
import type { JsonObject } from "../../json";
import type { UiCatalog } from "../../types";

export interface DynamicOptionsQuery {
  key: string;
  dependencies?: Record<string, string>;
  refresh?: boolean;
  signal?: AbortSignal;
}

export interface ControlPlanePort {
  catalog(signal?: AbortSignal): Promise<UiCatalog>;
  options(query: DynamicOptionsQuery): Promise<DynamicOptions>;
  checkConnection(
    request: ConnectionCheckRequest,
    signal?: AbortSignal,
  ): Promise<ConnectionCheckResult>;
  previewMessage(
    request: MessagePreviewRequest,
    signal?: AbortSignal,
  ): Promise<MessagePreviewResult>;
  sqlPlayground(
    request: SqlPlaygroundRequest,
    signal?: AbortSignal,
  ): Promise<SqlPlaygroundResult>;
  deliveries(signal?: AbortSignal): Promise<DeliverySummary[]>;
  delivery(id: string, signal?: AbortSignal): Promise<DeliveryRecord>;
  deliveryLogs(id: string, signal?: AbortSignal): Promise<WorkerLogsResult>;
  deliveryLog(
    id: string,
    workerId: string,
    cursor?: number,
    signal?: AbortSignal,
  ): Promise<WorkerLogChunkView>;
  create(
    name: string,
    description: string,
    config: JsonObject,
    signal?: AbortSignal,
  ): Promise<DeliveryRecord>;
  update(
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    name: string,
    description: string,
    config: JsonObject,
    signal?: AbortSignal,
  ): Promise<DeliveryRecord>;
  delete(
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    signal?: AbortSignal,
  ): Promise<DeliveryRecord>;
  yaml(config: JsonObject, signal?: AbortSignal): Promise<{ yaml: string }>;
  parseYaml(
    yaml: string,
    signal?: AbortSignal,
  ): Promise<{ config: JsonObject }>;
  discover(config: JsonObject, signal?: AbortSignal): Promise<DiscoveryResult>;
  validate(
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    signal?: AbortSignal,
  ): Promise<ValidationCommandResult>;
  activate(
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    signal?: AbortSignal,
  ): Promise<DeliveryRecord>;
  stop(
    id: string,
    expectedRevision: number,
    expectedRecordVersion: string,
    expectedRunId: string,
    signal?: AbortSignal,
  ): Promise<DeliveryRecord>;
}
