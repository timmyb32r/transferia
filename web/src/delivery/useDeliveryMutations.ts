import { api } from "../api";
import { isDirty, type EditorState } from "../state";
import type {
  DeliveryRecord,
  DeliverySummary,
  DiscoveryResult,
} from "../types";
import type { EditorRequestContext } from "./useDeliveryJobs";
import type { useDeliveryJobs } from "./useDeliveryJobs";
import type { useOperations } from "./useOperations";

type DeliveryJobs = ReturnType<typeof useDeliveryJobs>;
type Operations = ReturnType<typeof useOperations>;

export function useDeliveryMutations({
  editor,
  jobs,
  operations,
  onDeliveries,
  onPersisted,
  onRuntime,
  onDiscovery,
  isCurrentContext,
}: {
  editor: EditorState;
  jobs: Pick<DeliveryJobs, "list" | "save" | "validate" | "action">;
  operations: Pick<Operations, "beginOperation" | "finishOperation">;
  onDeliveries: (deliveries: DeliverySummary[]) => void;
  onPersisted: (
    context: EditorRequestContext,
    delivery: DeliveryRecord,
  ) => void;
  onRuntime: (context: EditorRequestContext, delivery: DeliveryRecord) => void;
  onDiscovery: (discovery: DiscoveryResult) => void;
  isCurrentContext: (context: EditorRequestContext) => boolean;
}) {
  const { beginOperation, finishOperation } = operations;

  const refreshList = async () => {
    const requestId = beginOperation("list");
    try {
      const result = await jobs.list.run(undefined, undefined, () =>
        api.deliveries(),
      );
      if (result !== undefined) onDeliveries(result.value);
      finishOperation("list", requestId);
    } catch (reason) {
      finishOperation(
        "list",
        requestId,
        `Delivery list refresh failed: ${errorMessage(reason)}`,
      );
    }
  };

  const runAction = async (
    label: string,
    action: () => Promise<DeliveryRecord>,
  ) => {
    const requestId = beginOperation("action", label);
    const context = editorContext(editor);
    try {
      const result = await jobs.action.run(context, undefined, action);
      if (result === undefined) {
        finishOperation("action", requestId);
        return undefined;
      }
      onRuntime(result.context, result.value);
      await refreshList();
      finishOperation("action", requestId);
      return result.value;
    } catch (reason) {
      finishOperation("action", requestId, errorMessage(reason));
      return undefined;
    }
  };

  const remove = async (): Promise<boolean> => {
    if (
      editor.id === undefined ||
      editor.persistedRevision === undefined ||
      editor.recordVersion === undefined
    )
      return false;
    const requestId = beginOperation("action", "Deleting delivery…");
    const context = editorContext(editor);
    try {
      const result = await jobs.action.run(context, undefined, () =>
        api.delete(
          editor.id!,
          editor.persistedRevision!,
          editor.recordVersion!,
        ),
      );
      if (result === undefined) {
        finishOperation("action", requestId);
        return false;
      }
      await refreshList();
      finishOperation("action", requestId);
      return isCurrentContext(result.context);
    } catch (reason) {
      finishOperation("action", requestId, errorMessage(reason));
      return false;
    }
  };

  const save = async (): Promise<DeliveryRecord | undefined> => {
    const requestId = beginOperation("save", "Saving…");
    const context = editorContext(editor);
    const snapshot = {
      id: editor.id,
      persistedRevision: editor.persistedRevision,
      recordVersion: editor.recordVersion,
      name: editor.name,
      description: editor.description,
      config: editor.config,
    };
    try {
      const result = await jobs.save.run(context, undefined, () =>
        snapshot.id === undefined
          ? api.create(snapshot.name, snapshot.description, snapshot.config)
          : api.update(
              snapshot.id,
              snapshot.persistedRevision!,
              snapshot.recordVersion!,
              snapshot.name,
              snapshot.description,
              snapshot.config,
            ),
      );
      if (result === undefined) {
        finishOperation("save", requestId);
        return undefined;
      }
      onPersisted(result.context, result.value);
      await refreshList();
      finishOperation("save", requestId);
      return result.value;
    } catch (reason) {
      finishOperation("save", requestId, errorMessage(reason));
      return undefined;
    }
  };

  const validate = async () => {
    const mustSave = isDirty(editor) || editor.id === undefined;
    const saved = mustSave ? await save() : undefined;
    if (mustSave && saved === undefined) return;
    const id = saved?.id ?? editor.id;
    const revision = saved?.revision ?? editor.persistedRevision;
    const recordVersion = saved?.record_version ?? editor.recordVersion;
    if (
      id === undefined ||
      revision === undefined ||
      recordVersion === undefined
    )
      return;
    const requestId = beginOperation(
      "validate",
      "Validating current revision…",
    );
    const context = editorContext(editor);
    try {
      const result = await jobs.validate.run(context, undefined, () =>
        api.validate(id, revision, recordVersion),
      );
      if (result === undefined) {
        finishOperation("validate", requestId);
        return;
      }
      if (
        result.value.discovery !== undefined &&
        isCurrentContext(result.context)
      )
        onDiscovery(result.value.discovery);
      onRuntime(result.context, result.value.delivery);
      await refreshList();
      finishOperation("validate", requestId);
    } catch (reason) {
      finishOperation("validate", requestId, errorMessage(reason));
    }
  };

  return { refreshList, remove, runAction, save, validate };
}

function editorContext(editor: EditorState): EditorRequestContext {
  return {
    sessionId: editor.sessionId,
    localRevision: editor.localRevision,
  };
}

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}
