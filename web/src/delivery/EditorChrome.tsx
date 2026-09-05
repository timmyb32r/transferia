import type { EditorState } from "../state";
import { isDirty } from "../state";
import type { DeliverySummary, UiCatalog } from "../types";
import { AppearanceSettings } from "../ui/AppearanceSettings";
import { Button } from "../ui/Button";
import { CompatibilityMatrixLauncher } from "../ui/CompatibilityMatrixDialog";
import { InstantTooltip } from "../ui/InstantTooltip";
import type { Appearance } from "../ui/appearance";
import { StatusPill } from "./EditorViews";
import type { EditorView } from "./useYamlEditor";
import type { OperationKey, OperationState } from "../application/operations";

export function EditorActions({
  editor,
  blocked,
  validatePending = false,
  activatePending = false,
  runtimeActionIntent,
  requiredFieldsComplete,
  onMissingRequired,
  onEdit,
  onClone,
  onDelete,
  onSave,
  onValidate,
  onActivate,
  onPause,
  onStop,
}: {
  editor: EditorState;
  blocked: boolean;
  validatePending?: boolean;
  activatePending?: boolean;
  runtimeActionIntent?: "activate" | "pause" | "stop" | undefined;
  requiredFieldsComplete: boolean;
  onMissingRequired: () => void;
  onEdit: () => void;
  onClone: () => void;
  onDelete: () => void;
  onSave: () => void;
  onValidate: () => void;
  onActivate: () => void;
  onPause?: ((runId: string) => void) | undefined;
  onStop: (runId: string) => void;
}) {
  const runtimeIsActive =
    runtimeActionIntent === "activate" ||
    (runtimeActionIntent === undefined &&
      (editor.runtime.state === "starting" ||
        editor.runtime.state === "running"));
  const runtimeIsTransitioning =
    editor.runtime.state === "starting" || editor.runtime.state === "stopping";
  const runId =
    editor.runtime.state === "starting" ||
    editor.runtime.state === "running" ||
    editor.runtime.state === "stopping" ||
    editor.runtime.state === "failed"
      ? editor.runtime.run_id
      : undefined;
  const activationReady =
    editor.id !== undefined &&
    !isDirty(editor) &&
    !runtimeIsTransitioning &&
    editor.validation.state === "ready" &&
    editor.validation.revision === editor.persistedRevision;
  const activationIsDiagnostic = !activationReady && !requiredFieldsComplete;
  const activationUnavailableReason = blocked
    ? activatePending && runtimeActionIntent !== undefined
      ? runtimeActionIntent === "pause"
        ? "Pausing the worker…"
        : runtimeActionIntent === "stop"
          ? "Deactivating the worker…"
          : "Starting the worker…"
      : "Another operation is in progress"
    : !requiredFieldsComplete
      ? "Complete the required delivery, source, and destination fields"
      : editor.id === undefined
        ? "Save and validate the delivery first"
        : isDirty(editor)
          ? "Save and validate the current changes first"
          : editor.validation.state !== "ready" ||
              editor.validation.revision !== editor.persistedRevision
            ? "Validate the current revision first"
            : undefined;
  const activate = () =>
    activationIsDiagnostic ? onMissingRequired() : onActivate();
  const validate = () =>
    requiredFieldsComplete ? onValidate() : onMissingRequired();
  const transportControls = (
    <TransportControls
      active={runtimeIsActive}
      runId={runId}
      activationReady={activationReady}
      activationIsDiagnostic={activationIsDiagnostic}
      blocked={blocked || editor.runtime.state === "stopping"}
      pending={activatePending && runtimeActionIntent !== undefined}
      pendingIntent={runtimeActionIntent}
      activationUnavailableReason={activationUnavailableReason}
      onActivate={activate}
      onPause={onPause ?? onStop}
      onStop={onStop}
    />
  );
  const runtimeAllowsEditing =
    editor.runtime.state === "created" ||
    editor.runtime.state === "stopped" ||
    editor.runtime.state === "failed";
  const savedActionReason = editor.id === undefined
    ? "Save the delivery first"
    : editor.editing
      ? "Finish editing the delivery first"
      : blocked
        ? "Another operation is in progress"
        : runtimeIsActive || runtimeIsTransitioning
          ? "Deactivate the delivery first"
          : undefined;
  return (
    <div class="actions">
      <Button variant="danger" disabled={savedActionReason !== undefined}
        title={savedActionReason} onClick={onDelete}>
        Delete
      </Button>
      <Button disabled={savedActionReason !== undefined}
        title={savedActionReason} onClick={onClone}>
        Clone
      </Button>
      <Button disabled={savedActionReason !== undefined || !runtimeAllowsEditing}
        title={savedActionReason} onClick={onEdit}>
        Edit
      </Button>
      <Button
        disabled={
          !editor.editing ||
          blocked ||
          runtimeIsActive ||
          runtimeIsTransitioning ||
          !isDirty(editor)
        }
        onClick={onSave}
      >
        Save
      </Button>
      <Button
        disabled={blocked || runtimeIsActive || runtimeIsTransitioning}
        pending={validatePending}
        onClick={validate}
      >
        Validate
      </Button>
      {transportControls}
    </div>
  );
}

function TransportControls({
  active,
  runId,
  activationReady,
  activationIsDiagnostic,
  blocked,
  pending,
  pendingIntent,
  activationUnavailableReason,
  onActivate,
  onPause,
  onStop,
}: {
  active: boolean;
  runId: string | undefined;
  activationReady: boolean;
  activationIsDiagnostic: boolean;
  blocked: boolean;
  pending: boolean;
  pendingIntent: "activate" | "pause" | "stop" | undefined;
  activationUnavailableReason: string | undefined;
  onActivate: () => void;
  onPause: (runId: string) => void;
  onStop: (runId: string) => void;
}) {
  const deactivate = (
    <Button
      class="transport-action deactivate-action"
      aria-label="Deactivate"
      disabled={blocked || !active || runId === undefined}
      pending={pending && pendingIntent === "stop"}
      onClick={() => runId !== undefined && onStop(runId)}
    >
      <span class="transport-icon stop-icon" aria-hidden="true" />
      <span>Deactivate</span>
    </Button>
  );
  const toggle = active ? (
    <Button
      class="transport-action run-toggle-action pause-action"
      aria-label="Pause"
      disabled={blocked || runId === undefined}
      pending={pending && pendingIntent !== "stop"}
      onClick={() => runId !== undefined && onPause(runId)}
    >
      <span class="transport-icon pause-icon" aria-hidden="true" />
      <span>Pause</span>
    </Button>
  ) : (
    <ActivationButton
      ready={activationReady}
      diagnostic={activationIsDiagnostic}
      blocked={blocked}
      pending={pending && pendingIntent !== "stop"}
      reason={activationUnavailableReason}
      onClick={onActivate}
    />
  );
  return (
    <div class="transport-controls" aria-label="Delivery controls">
      {deactivate}
      {toggle}
    </div>
  );
}

function ActivationButton({
  ready,
  diagnostic,
  blocked,
  pending,
  reason,
  onClick,
}: {
  ready: boolean;
  diagnostic: boolean;
  blocked: boolean;
  pending: boolean;
  reason: string | undefined;
  onClick: () => void;
}) {
  const button = (
    <Button
      class={`transport-action run-toggle-action activate-action${diagnostic ? " diagnostic-disabled" : ""}`}
      aria-label="Activate"
      aria-disabled={!ready || pending}
      disabled={blocked || (!ready && !diagnostic)}
      pending={pending}
      onClick={onClick}
    >
      <span class="transport-icon play-icon" aria-hidden="true" />
      <span>Activate</span>
    </Button>
  );
  return reason === undefined ? button : (
    <InstantTooltip content={reason} class="action-disabled-tooltip">
      {button}
    </InstantTooltip>
  );
}

export function DeliverySidebar({
  deliveries,
  selectedId,
  onNew,
  onOpen,
  appearance,
  catalog,
  onAppearance,
  dataWidgetAvailable,
  dataWidgetUnavailableReason,
  dataWidgetVisible,
  onToggleDataWidget,
}: {
  deliveries: DeliverySummary[];
  selectedId: string | undefined;
  onNew: () => void;
  onOpen: (id: string) => void;
  appearance: Appearance;
  catalog: UiCatalog;
  onAppearance: (appearance: Appearance) => void;
  dataWidgetAvailable: boolean;
  dataWidgetUnavailableReason?: string | undefined;
  dataWidgetVisible: boolean;
  onToggleDataWidget: () => void;
}) {
  return (
    <aside class="sidebar">
      <button
        type="button"
        class="brand"
        aria-label="Open Transferia home"
        onClick={onNew}
      >
        <span class="brand-mark">T</span>
        <div>
          <strong>Transferia</strong>
          <small>Local control plane</small>
        </div>
      </button>
      <Button variant="primary" class="new-button" onClick={onNew}>
        + New delivery
      </Button>
      <nav class="delivery-list">
        {deliveries.map((delivery) => (
          <Button
            class={
              delivery.id === selectedId
                ? "delivery-item active"
                : "delivery-item"
            }
            title={delivery.name}
            onClick={() => onOpen(delivery.id)}
          >
            <span class="delivery-item-name">{delivery.name}</span>
            <StatusPill runtime={delivery.runtime.state} />
          </Button>
        ))}
        {deliveries.length === 0 && (
          <p class="empty-list">No saved deliveries yet.</p>
        )}
      </nav>
      <InstantTooltip
        class="sidebar-tool-tooltip"
        placement="right"
        content={
          dataWidgetAvailable
            ? dataWidgetVisible
              ? "Hide the data widget"
              : "Show the data widget"
            : (dataWidgetUnavailableReason ?? "No data schema is available")
        }
      >
        <Button
          variant={dataWidgetAvailable ? "primary" : "default"}
          class={
            dataWidgetAvailable
              ? "sidebar-tool-button data-widget-ready"
              : "sidebar-tool-button"
          }
          aria-pressed={dataWidgetVisible}
          disabled={!dataWidgetAvailable}
          onClick={onToggleDataWidget}
        >
          Data widget
        </Button>
      </InstantTooltip>
      <CompatibilityMatrixLauncher catalog={catalog} />
      <AppearanceSettings
        value={appearance}
        onChange={onAppearance}
      />
    </aside>
  );
}

export function EditorTabs({
  active,
  disabled,
  dataSchemaAvailable,
  dataSchemaUnavailableReason,
  speedtestAvailable = false,
  speedtestUnavailableReason,
  performanceAdviceCount,
  onUi,
  onYaml,
  onDataSchema,
  onDataSchemaUnavailable,
  onSpeedtest,
  onSpeedtestUnavailable,
  onPerformanceAdvice,
  onLogs,
}: {
  active: EditorView;
  disabled: boolean;
  dataSchemaAvailable: boolean;
  dataSchemaUnavailableReason?: string | undefined;
  speedtestAvailable?: boolean;
  speedtestUnavailableReason?: string | undefined;
  performanceAdviceCount?: number | undefined;
  onUi: () => void;
  onYaml: () => void;
  onDataSchema: () => void;
  onDataSchemaUnavailable?: (() => void) | undefined;
  onSpeedtest?: (() => void) | undefined;
  onSpeedtestUnavailable?: (() => void) | undefined;
  onPerformanceAdvice: () => void;
  onLogs?: () => void;
}) {
  const performanceAdviceAvailable =
    performanceAdviceCount !== undefined && performanceAdviceCount > 0;
  const performanceAdviceLabel = performanceAdviceAvailable
    ? `Performance advice (${performanceAdviceCount})`
    : "Performance advice";
  const performanceAdviceTooltip = performanceAdviceAvailable
    ? "Open advice from the latest successful validation"
    : performanceAdviceCount === 0
      ? "No performance advice for this validated configuration"
      : "Available after successful validation";

  return (
    <div class="editor-tabs">
      <div
        class="editor-view-tabs"
        role="tablist"
        aria-label="Configuration view"
      >
        <Button
          role="tab"
          aria-selected={active === "ui"}
          class={active === "ui" ? "active" : ""}
          disabled={disabled}
          onClick={onUi}
        >
          UI
        </Button>
        <Button
          role="tab"
          aria-selected={active === "yaml"}
          class={active === "yaml" ? "active" : ""}
          disabled={disabled}
          onClick={onYaml}
        >
          YAML
        </Button>
        <InstantTooltip
          class="editor-tab-tooltip"
          content={
            dataSchemaAvailable
              ? "Open the discovered data schema"
              : (dataSchemaUnavailableReason ??
                "Data schema becomes available after discovery has produced a table")
          }
        >
          <Button
            role="tab"
            aria-selected={active === "data_schema"}
            aria-disabled={!dataSchemaAvailable}
            class={[
              active === "data_schema" ? "active" : "",
              !dataSchemaAvailable ? "diagnostic-disabled" : "",
            ]
              .filter(Boolean)
              .join(" ")}
            disabled={disabled}
            onClick={
              dataSchemaAvailable
                ? onDataSchema
                : (onDataSchemaUnavailable ?? onDataSchema)
            }
          >
            Data schema
          </Button>
        </InstantTooltip>
        <InstantTooltip
          class="editor-tab-tooltip"
          content={
            speedtestAvailable
              ? "Open the one-stream performance test"
              : (speedtestUnavailableReason ??
                "Complete the required source and destination fields")
          }
        >
          <Button
            role="tab"
            aria-selected={active === "speedtest"}
            aria-disabled={!speedtestAvailable}
            class={[
              active === "speedtest" ? "active" : "",
              !speedtestAvailable ? "diagnostic-disabled" : "",
            ]
              .filter(Boolean)
              .join(" ")}
            disabled={disabled}
            onClick={
              speedtestAvailable
                ? onSpeedtest
                : (onSpeedtestUnavailable ?? onSpeedtest)
            }
          >
            Speedtest
          </Button>
        </InstantTooltip>
        <InstantTooltip
          class="editor-tab-tooltip performance-advice-tab-tooltip"
          content={performanceAdviceTooltip}
        >
          <Button
            role="tab"
            aria-label={performanceAdviceLabel}
            aria-selected={active === "performance_advice"}
            aria-disabled={!performanceAdviceAvailable}
            class={[
              active === "performance_advice" ? "active" : "",
              !performanceAdviceAvailable ? "diagnostic-disabled" : "",
            ]
              .filter(Boolean)
              .join(" ")}
            disabled={disabled}
            onClick={
              performanceAdviceAvailable ? onPerformanceAdvice : undefined
            }
          >
            <span>Performance advice</span>
            <span class="performance-advice-tab-count" aria-hidden="true">
              {performanceAdviceAvailable
                ? `(${performanceAdviceCount})`
                : "\u00a0"}
            </span>
          </Button>
        </InstantTooltip>
        <Button
          role="tab"
          aria-selected={active === "logs"}
          class={active === "logs" ? "active" : ""}
          disabled={disabled}
          onClick={onLogs}
        >
          Logs
        </Button>
      </div>
    </div>
  );
}

export function OperationNotices({
  operations,
  onDismiss,
}: {
  operations: Partial<Record<OperationKey, OperationState>>;
  onDismiss: (key: OperationKey, requestId: number) => void;
}) {
  return (
    <aside
      class="operation-notices"
      aria-label="Operation status"
      aria-live="polite"
    >
      {Object.entries(operations).map(
        ([key, operation]) =>
          operation?.error && (
            <div
              class="notice error"
              key={key}
              role="alert"
              tabIndex={0}
              aria-label={`Dismiss error: ${operation.error}`}
              onClick={() =>
                onDismiss(key as OperationKey, operation.requestId)
              }
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  onDismiss(key as OperationKey, operation.requestId);
                }
              }}
            >
              <span>{operation.error}</span>
              <Button
                shape="icon"
                aria-label="Dismiss error"
                title="Dismiss"
                onClick={(event) => {
                  event.stopPropagation();
                  onDismiss(key as OperationKey, operation.requestId);
                }}
              >
                ×
              </Button>
            </div>
          ),
      )}
      {Object.entries(operations).map(
        ([key, operation]) =>
          operation?.success && (
            <div
              class="notice success"
              key={key}
              role="button"
              tabIndex={0}
              aria-label={`Dismiss: ${operation.success}`}
              onClick={() =>
                onDismiss(key as OperationKey, operation.requestId)
              }
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  onDismiss(key as OperationKey, operation.requestId);
                }
              }}
            >
              <span>{operation.success}</span>
              <span aria-hidden="true">×</span>
            </div>
          ),
      )}
      {Object.values(operations).map(
        (operation) =>
          operation?.label && (
            <div
              class="notice progress"
              key={operation.requestId}
              role="status"
            >
              <span class="spinner" />
              {operation.label}
            </div>
          ),
      )}
    </aside>
  );
}
