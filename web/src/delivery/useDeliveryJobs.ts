import { useMemo } from "preact/hooks";

import { api } from "../api";
import { LatestJob } from "../effects";
import type {
  DeliveryRecord,
  DeliverySummary,
  DiscoveryResult,
  JsonObject,
} from "../types";
import type { EditorSessionId } from "../state";

export interface EditorRequestContext {
  sessionId: EditorSessionId;
  localRevision: number;
}

export function useDeliveryJobs() {
  return useMemo(() => {
    const jobs = {
      yaml: new LatestJob<EditorRequestContext, JsonObject, { yaml: string }>(),
      discovery: new LatestJob<
        EditorRequestContext,
        JsonObject,
        DiscoveryResult
      >(),
      list: new LatestJob<void, undefined, DeliverySummary[]>(),
      poll: new LatestJob<EditorSessionId, string, DeliveryRecord>(),
      open: new LatestJob<EditorSessionId, string, DeliveryRecord>(),
      save: new LatestJob<EditorRequestContext, undefined, DeliveryRecord>(),
      validate: new LatestJob<
        EditorRequestContext,
        undefined,
        Awaited<ReturnType<typeof api.validate>>
      >(),
      action: new LatestJob<EditorRequestContext, undefined, DeliveryRecord>(),
      parseYaml: new LatestJob<
        EditorRequestContext,
        string,
        { config: JsonObject }
      >(),
    };
    return {
      ...jobs,
      cancelEditorJobs() {
        jobs.yaml.cancel();
        jobs.discovery.cancel();
        jobs.poll.cancel();
        jobs.open.cancel();
        jobs.save.cancel();
        jobs.validate.cancel();
        jobs.action.cancel();
        jobs.parseYaml.cancel();
      },
    };
  }, []);
}
