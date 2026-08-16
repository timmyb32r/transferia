import { useEffect } from "preact/hooks";

import { api } from "../api";
import type { LatestJob } from "../effects";
import { isDirty, type EditorSessionId, type EditorState } from "../state";
import type { DeliveryRecord, DeliverySummary } from "../types";

export function useDeliveryPolling({
  editor,
  listJob,
  pollJob,
  onDeliveries,
  onRuntime,
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
}) {
  useEffect(() => {
    const timer = window.setInterval(() => {
      void listJob
        .run(undefined, undefined, () => api.deliveries())
        .then((result) => {
          if (result !== undefined) onDeliveries(result.value);
        })
        .catch(() => undefined);
      if (editor.id !== undefined && !isDirty(editor)) {
        const sessionId = editor.sessionId;
        const expectedLocalRevision = editor.localRevision;
        void pollJob
          .run(sessionId, editor.id, (id) => api.delivery(id))
          .then((result) => {
            if (result !== undefined)
              onRuntime(
                result.context,
                expectedLocalRevision,
                result.value,
              );
          })
          .catch(() => undefined);
      }
    }, 2000);
    return () => window.clearInterval(timer);
  }, [
    editor.id,
    editor.sessionId,
    editor.localRevision,
    editor.savedLocalRevision,
    listJob,
    pollJob,
    onDeliveries,
    onRuntime,
  ]);
}
