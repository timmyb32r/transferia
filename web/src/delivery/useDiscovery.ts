import { useEffect, useState } from "preact/hooks";

import { api } from "../api";
import type { EditorState } from "../state";
import type { DiscoveryResult } from "../types";
import type { EditorRequestContext, useDeliveryJobs } from "./useDeliveryJobs";
import type { useOperations } from "./useOperations";

type DeliveryJobs = ReturnType<typeof useDeliveryJobs>;
type Operations = ReturnType<typeof useOperations>;

export function useDiscovery({
  editor,
  structurallyComplete,
  job,
  operations,
  isCurrentContext,
}: {
  editor: EditorState;
  structurallyComplete: boolean;
  job: DeliveryJobs["discovery"];
  operations: Pick<
    Operations,
    "beginOperation" | "finishOperation" | "clearOperation"
  >;
  isCurrentContext: (context: EditorRequestContext) => boolean;
}) {
  const [discovery, setDiscovery] = useState<DiscoveryResult>();

  useEffect(() => setDiscovery(undefined), [editor.sessionId]);

  useEffect(() => {
    job.cancel();
    operations.clearOperation("discovery");
    if (!structurallyComplete) {
      setDiscovery(undefined);
      return;
    }
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    const timer = window.setTimeout(() => {
      const requestId = operations.beginOperation(
        "discovery",
        "Discovering topology and schema…",
      );
      void job
        .run(context, editor.config, (config, signal) =>
          api.discover(config, signal),
        )
        .then((result) => {
          if (result !== undefined && isCurrentContext(result.context)) {
            setDiscovery(result.value);
          }
          operations.finishOperation("discovery", requestId);
        })
        .catch((reason: unknown) =>
          operations.finishOperation(
            "discovery",
            requestId,
            errorMessage(reason),
          ),
        );
    }, 450);
    return () => {
      window.clearTimeout(timer);
      job.cancel();
    };
  }, [
    editor.config,
    editor.sessionId,
    editor.localRevision,
    structurallyComplete,
  ]);

  return { discovery, setDiscovery };
}

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}
