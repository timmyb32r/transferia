import type { EditorState } from "../state";
import { isDirty } from "../state";
import type { DeliverySummary } from "../types";
import { AppearanceSettings } from "../ui/AppearanceSettings";
import { Button } from "../ui/Button";
import type { Appearance } from "../ui/appearance";
import { StatusPill } from "./EditorViews";

export type OperationKey =
  | "bootstrap"
  | "list"
  | "open"
  | "save"
  | "validate"
  | "action"
  | "yaml"
  | "parseYaml"
  | "discovery";

export interface OperationState {
  requestId: number;
  label?: string;
  error?: string;
}

export function EditorActions({
  editor,
  blocked,
  requiredFieldsComplete,
  onMissingRequired,
  onEdit,
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
  if (!editor.editing && editor.id !== undefined) {
    const runtimeAllowsEditing =
      editor.runtime.state === "created" ||
      editor.runtime.state === "stopped" ||
      editor.runtime.state === "failed";
    return (
      <div class="actions">
        <Button disabled={blocked || !runtimeAllowsEditing} onClick={onEdit}>
          Edit
        </Button>
        <Button
          disabled={blocked || editor.name.trim() === ""}
          onClick={onValidate}
        >
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
      <Button
        disabled={blocked || editor.name.trim() === ""}
        onClick={onValidate}
      >
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
  onUi,
  onYaml,
}: {
  active: "ui" | "yaml";
  disabled: boolean;
  onUi: () => void;
  onYaml: () => void;
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
    <>
      {Object.entries(operations).map(
        ([key, operation]) =>
          operation?.error && (
            <div class="notice error" key={key}>
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
            <div class="notice progress" key={operation.requestId}>
              <span class="spinner" />
              {operation.label}
            </div>
          ),
      )}
    </>
  );
}
