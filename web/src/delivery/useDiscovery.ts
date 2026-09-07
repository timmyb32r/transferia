import { useEffect, useState } from "preact/hooks";

import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import type { EditorState } from "../state";
import type { DiscoveryResult } from "../types";
import type { MetadataStatus } from "../generated/apiContract";
import type { EditorRequestContext, useDeliveryJobs } from "./useDeliveryJobs";
import type { useOperations } from "./useOperations";

type DeliveryJobs = ReturnType<typeof useDeliveryJobs>;
type Operations = ReturnType<typeof useOperations>;

export function useDiscovery({
  editor,
  structurallyComplete,
  metadataRequired = false,
  metadata,
  job,
  operations,
  isCurrentContext,
}: {
  editor: EditorState;
  structurallyComplete: boolean;
  metadataRequired?: boolean;
  metadata?: MetadataStatus | undefined;
  job: DeliveryJobs["discovery"];
  operations: Pick<
    Operations,
    "beginOperation" | "finishOperation" | "clearOperation"
  >;
  isCurrentContext: (context: EditorRequestContext) => boolean;
}) {
  const api = useControlPlane();
  const [snapshot, setSnapshot] = useState<{
    value: DiscoveryResult; sessionId: EditorState["sessionId"]; sourceKey: string;
  }>();
  // Sink/transform edits do not change parser outputs. Source edits invalidate
  // the usable catalog immediately, including during the discovery debounce.
  const sourceKey = JSON.stringify([editor.config.delivery_type, editor.config.source]);
  const discovery = snapshot?.value;
  const sourceDiscovery = structurallyComplete && snapshot?.sessionId === editor.sessionId
    && snapshot.sourceKey === sourceKey ? snapshot.value : undefined;
  const clearDiscovery = () => setSnapshot(undefined);
  const [error, setError] = useState<string>();

  useEffect(() => {
    clearDiscovery();
    setError(undefined);
  }, [editor.sessionId]);

  useEffect(() => {
    job.cancel();
    operations.clearOperation("discovery");
    if (!structurallyComplete || (metadataRequired && (!metadata || metadata.loading))) {
      clearDiscovery();
      setError(metadataRequired && structurallyComplete ? metadata
        ? "Source schemas are loading…" : "Use Discover tables in Tables first." : undefined);
      return;
    }
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    const timer = window.setTimeout(() => {
      setError(undefined);
      // Automatic discovery is background work. Its progress is rendered by
      // the controls that consume it; a global notice would be unrelated to
      // the user's current interaction and would flicker while editing.
      const requestId = operations.beginOperation("discovery");
      void job
        .run(context, editor.config, (config, signal) =>
          metadataRequired && metadata ? api.metadataDiscovery(metadata.id, config, signal) : api.discover(config, signal),
        )
        .then((result) => {
          if (result !== undefined && isCurrentContext(result.context)) {
            setSnapshot({ value: result.value, sessionId: context.sessionId, sourceKey });
          }
          operations.finishOperation("discovery", requestId);
        })
        .catch((reason: unknown) => {
          if (isCurrentContext(context)) {
            clearDiscovery();
            setError(errorMessage(reason));
          }
          operations.finishOperation("discovery", requestId);
        });
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
    metadataRequired,
    metadata?.id,
    metadata?.loading,
    metadata?.loaded.length,
    metadata?.errors.length,
  ]);

  return { discovery, sourceDiscovery, clearDiscovery, error };
}

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}
