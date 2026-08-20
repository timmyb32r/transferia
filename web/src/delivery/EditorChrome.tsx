import type { EditorState } from "../state";
import { isDirty } from "../state";
import type { DeliverySummary } from "../types";
import { AppearanceSettings } from "../ui/AppearanceSettings";
import { Button } from "../ui/Button";
import type { Appearance } from "../ui/appearance";
import { StatusPill } from "./EditorViews";
import type { EditorView } from "./useYamlEditor";
import type { OperationKey, OperationState } from "../application/operations";

export function EditorActions({
  editor,
  blocked,
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
        <Button disabled={blocked} onClick={validate}>
          Validate
        </Button>
        <Button
          variant="primary"
          class={activationIsDiagnostic ? "diagnostic-disabled" : undefined}
          aria-disabled={!activationReady}
          disabled={blocked || (!activationReady && !activationIsDiagnostic)}
          onClick={activate}
        >
          Activate
        </Button>
      </div>
    );
  }
  return (
    <div class="actions">
      <Button disabled={blocked || !isDirty(editor)} onClick={onSave}>
        Save
      </Button>
      <Button disabled={blocked} onClick={validate}>
        Validate
      </Button>
      <Button
        variant="primary"
        class={activationIsDiagnostic ? "diagnostic-disabled" : undefined}
        aria-disabled={!activationReady}
        disabled={blocked || (!activationReady && !activationIsDiagnostic)}
        onClick={activate}
      >
        Activate
      </Button>
    </div>
  );
}

export function DeliverySidebar({
  deliveries,
  selectedId,
  onNew,
  onOpen,
  appearance,
  onAppearance,
}: {
  deliveries: DeliverySummary[];
  selectedId: string | undefined;
  onNew: () => void;
  onOpen: (id: string) => void;
  appearance: Appearance;
  onAppearance: (appearance: Appearance) => void;
}) {
  return (
    <aside class="sidebar">
      <div class="brand">
        <span class="brand-mark">T</span>
        <div>
          <strong>Transferia</strong>
          <small>Local control plane</small>
        </div>
      </div>
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
            onClick={() => onOpen(delivery.id)}
          >
            <span>{delivery.name}</span>
            <StatusPill runtime={delivery.runtime.state} />
          </Button>
        ))}
        {deliveries.length === 0 && (
          <p class="empty-list">No saved deliveries yet.</p>
        )}
      </nav>
      <AppearanceSettings value={appearance} onChange={onAppearance} />
    </aside>
  );
}

export function EditorTabs({
  active,
  disabled,
  dataSchemaAvailable,
  dataSchemaUnavailableReason,
  schemaInspectorVisible = false,
  onUi,
  onYaml,
  onDataSchema,
  onDataSchemaUnavailable,
  onLogs,
  onToggleSchemaInspector,
}: {
  active: EditorView;
  disabled: boolean;
  dataSchemaAvailable: boolean;
  dataSchemaUnavailableReason?: string | undefined;
  schemaInspectorVisible?: boolean;
  onUi: () => void;
  onYaml: () => void;
  onDataSchema: () => void;
  onDataSchemaUnavailable?: (() => void) | undefined;
  onLogs?: () => void;
  onToggleSchemaInspector?: () => void;
}) {
  return (
    <div class="editor-tabs" role="tablist" aria-label="Configuration view">
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
      <span
        class="editor-tab-tooltip"
        title={
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
      </span>
      <Button
        role="tab"
        aria-selected={active === "logs"}
        class={active === "logs" ? "active" : ""}
        disabled={disabled}
        onClick={onLogs}
      >
        Logs
      </Button>
      <span
        class="editor-tab-tooltip schema-widget-toggle"
        title={
          dataSchemaAvailable
            ? schemaInspectorVisible
              ? "Hide the schema widget"
              : "Show the schema widget"
            : (dataSchemaUnavailableReason ?? "No data schema is available")
        }
      >
        <Button
          class="schema-widget-button"
          aria-pressed={schemaInspectorVisible}
          disabled={disabled || !dataSchemaAvailable}
          onClick={onToggleSchemaInspector}
        >
          Schema widget
        </Button>
      </span>
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
            <div class="notice error" key={key} role="alert">
              <span>{operation.error}</span>
              <Button
                onClick={() =>
                  onDismiss(key as OperationKey, operation.requestId)
                }
              >
                ×
              </Button>
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
