import { useEffect } from "preact/hooks";

import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import type { LatestJob } from "../effects";
import { isDirty, type EditorSessionId, type EditorState } from "../state";
import type { DeliveryRecord, DeliverySummary } from "../types";

export function useDeliveryPolling({
  editor,
  listJob,
  pollJob,
  onDeliveries,
  onRuntime,
  onError,
}: {
  editor: EditorState;
  listJob: LatestJob<void, undefined, DeliverySummary[]>;
  pollJob: LatestJob<EditorSessionId, string, DeliveryRecord>;
  onDeliveries: (deliveries: DeliverySummary[]) => void;
  onRuntime: (
    sessionId: EditorSessionId,
    expectedLocalRevision: number,
    delivery: DeliveryRecord,
  ) => void;
  onError: (message: string | undefined) => void;
}) {
  const api = useControlPlane();
  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;
    const tick = async () => {
      let failed: unknown;
      const reads: Promise<void>[] = [];
      reads.push(
        listJob
          .run(undefined, undefined, () => api.deliveries())
          .then((result) => {
            if (result !== undefined) onDeliveries(result.value);
          })
          .catch((reason: unknown) => {
            failed = reason;
          }),
      );
      if (editor.id !== undefined && !isDirty(editor)) {
        const sessionId = editor.sessionId;
        const expectedLocalRevision = editor.localRevision;
        reads.push(
          pollJob
            .run(sessionId, editor.id, (id) => api.delivery(id))
            .then((result) => {
              if (result !== undefined)
                onRuntime(result.context, expectedLocalRevision, result.value);
            })
            .catch((reason: unknown) => {
              failed = reason;
            }),
        );
      }
      await Promise.all(reads);
      if (cancelled) return;
      onError(
        failed === undefined
          ? undefined
          : failed instanceof Error
            ? failed.message
            : String(failed),
      );
      timer = window.setTimeout(() => void tick(), 2000);
    };
    timer = window.setTimeout(() => void tick(), 2000);
    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [
    editor.id,
    editor.sessionId,
    editor.localRevision,
    editor.savedLocalRevision,
    listJob,
    pollJob,
    onDeliveries,
    onRuntime,
    onError,
  ]);
}
