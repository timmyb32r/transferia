import type { EditorState } from "../state";
import { isDirty } from "../state";
import type { DeliverySummary } from "../types";
import { AppearanceSettings } from "../ui/AppearanceSettings";
import { Button } from "../ui/Button";
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
  requiredFieldsComplete,
  onMissingRequired,
  onEdit,
  onDelete,
  onSave,
  onValidate,
  onActivate,
  onStop,
}: {
  editor: EditorState;
  blocked: boolean;
  validatePending?: boolean;
  activatePending?: boolean;
  requiredFieldsComplete: boolean;
  onMissingRequired: () => void;
  onEdit: () => void;
  onDelete: () => void;
  onSave: () => void;
  onValidate: () => void;
  onActivate: () => void;
  onStop: (runId: string) => void;
}) {
  if (editor.runtime.state === "running") {
    const runId = editor.runtime.run_id;
    return (
      <div class="actions">
        <Button
          variant="danger"
          disabled={blocked}
          onClick={() => onStop(runId)}
        >
          Stop
        </Button>
      </div>
    );
  }
  const activationReady =
    editor.id !== undefined &&
    !isDirty(editor) &&
    editor.validation.state === "ready" &&
    editor.validation.revision === editor.persistedRevision;
  const activationIsDiagnostic = !activationReady && !requiredFieldsComplete;
  const activationUnavailableReason = blocked
    ? activatePending
      ? "Starting the worker…"
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
  if (!editor.editing && editor.id !== undefined) {
    const runtimeAllowsEditing =
      editor.runtime.state === "created" ||
      editor.runtime.state === "stopped" ||
      editor.runtime.state === "failed";
    return (
      <div class="actions">
        <Button variant="danger" disabled={blocked} onClick={onDelete}>
          Delete
        </Button>
        <Button disabled={blocked || !runtimeAllowsEditing} onClick={onEdit}>
          Edit
        </Button>
        <Button
          disabled={blocked}
          pending={validatePending}
          onClick={validate}
        >
          Validate
        </Button>
        <ActivationButton
          ready={activationReady}
          diagnostic={activationIsDiagnostic}
          blocked={blocked}
          pending={activatePending}
          reason={activationUnavailableReason}
          onClick={activate}
        />
      </div>
    );
  }
  return (
    <div class="actions">
      <Button disabled={blocked || !isDirty(editor)} onClick={onSave}>
        Save
      </Button>
      <Button
        disabled={blocked}
        pending={validatePending}
        onClick={validate}
      >
        Validate
      </Button>
      <ActivationButton
        ready={activationReady}
        diagnostic={activationIsDiagnostic}
        blocked={blocked}
        pending={activatePending}
        reason={activationUnavailableReason}
        onClick={activate}
      />
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
      variant="primary"
      class={`activate-action${diagnostic ? " diagnostic-disabled" : ""}`}
      aria-disabled={!ready || pending}
      disabled={blocked || (!ready && !diagnostic)}
      pending={pending}
      onClick={onClick}
    >
      Activate
    </Button>
  );
  return reason === undefined ? (
    button
  ) : (
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
          class="sidebar-tool-button"
          aria-pressed={dataWidgetVisible}
          disabled={!dataWidgetAvailable}
          onClick={onToggleDataWidget}
        >
          Data widget
        </Button>
      </InstantTooltip>
      <AppearanceSettings value={appearance} onChange={onAppearance} />
    </aside>
  );
}

export function EditorTabs({
  active,
  disabled,
  dataSchemaAvailable,
  dataSchemaUnavailableReason,
  onUi,
  onYaml,
  onDataSchema,
  onDataSchemaUnavailable,
  onPerformanceAdvice,
  onLogs,
}: {
  active: EditorView;
  disabled: boolean;
  dataSchemaAvailable: boolean;
  dataSchemaUnavailableReason?: string | undefined;
  onUi: () => void;
  onYaml: () => void;
  onDataSchema: () => void;
  onDataSchemaUnavailable?: (() => void) | undefined;
  onPerformanceAdvice: () => void;
  onLogs?: () => void;
}) {
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
        <Button
          role="tab"
          aria-selected={active === "performance_advice"}
          class={active === "performance_advice" ? "active" : ""}
          disabled={disabled}
          onClick={onPerformanceAdvice}
        >
          Performance advice
        </Button>
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
                  onDismiss(key as OperationKey, operation.requestId)
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
              <span aria-hidden="true">
                ×
              </span>
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
