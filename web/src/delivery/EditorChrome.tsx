import type { EditorState } from "../state";
import { isDirty } from "../state";
import type { DeliverySummary } from "../types";
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
  onSave,
  onValidate,
  onActivate,
  onStop,
}: {
  editor: EditorState;
  blocked: boolean;
  onSave: () => void;
  onValidate: () => void;
  onActivate: () => void;
  onStop: (runId: string) => void;
}) {
  if (editor.runtime.state === "running") {
    const runId = editor.runtime.run_id;
    return (
      <div class="actions">
        <button
          class="danger-button"
          type="button"
          disabled={blocked}
          onClick={() => onStop(runId)}
        >
          Stop
        </button>
      </div>
    );
  }
  return (
    <div class="actions">
      <button
        type="button"
        disabled={blocked || !isDirty(editor)}
        onClick={onSave}
      >
        Save draft
      </button>
      <button
        type="button"
        disabled={blocked || editor.name.trim() === ""}
        onClick={onValidate}
      >
        Validate
      </button>
      <button
        class="primary"
        type="button"
        disabled={
          blocked ||
          editor.id === undefined ||
          isDirty(editor) ||
          editor.validation.state !== "ready" ||
          editor.validation.revision !== editor.persistedRevision
        }
        onClick={onActivate}
      >
        Activate
      </button>
    </div>
  );
}

export function DeliverySidebar({
  deliveries,
  selectedId,
  onNew,
  onOpen,
}: {
  deliveries: DeliverySummary[];
  selectedId: string | undefined;
  onNew: () => void;
  onOpen: (id: string) => void;
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
      <button class="primary new-button" type="button" onClick={onNew}>
        + New delivery
      </button>
      <nav class="delivery-list">
        {deliveries.map((delivery) => (
          <button
            type="button"
            class={
              delivery.id === selectedId
                ? "delivery-item active"
                : "delivery-item"
            }
            onClick={() => onOpen(delivery.id)}
          >
            <span>{delivery.name}</span>
            <StatusPill runtime={delivery.runtime.state} />
          </button>
        ))}
        {deliveries.length === 0 && (
          <p class="empty-list">No saved deliveries yet.</p>
        )}
      </nav>
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
      <button
        type="button"
        role="tab"
        aria-selected={active === "ui"}
        class={active === "ui" ? "active" : ""}
        disabled={disabled}
        onClick={onUi}
      >
        UI
      </button>
      <button
        type="button"
        role="tab"
        aria-selected={active === "yaml"}
        class={active === "yaml" ? "active" : ""}
        disabled={disabled}
        onClick={onYaml}
      >
        YAML
      </button>
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
              <button
                type="button"
                onClick={() =>
                  onDismiss(key as OperationKey, operation.requestId)
                }
              >
                ×
              </button>
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
