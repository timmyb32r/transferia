import { useMemo } from "preact/hooks";

import type { ValidationCommandResult } from "../generated/apiContract";
import { TaskRegistry } from "../application/tasks";
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
    const tasks = new TaskRegistry();
    const jobs = {
      yaml: tasks.latest<EditorRequestContext, JsonObject, { yaml: string }>(
        "revision",
      ),
      discovery: tasks.latest<
        EditorRequestContext,
        JsonObject,
        DiscoveryResult
      >("revision"),
      list: tasks.latest<void, undefined, DeliverySummary[]>("global"),
      poll: tasks.latest<EditorSessionId, string, DeliveryRecord>("revision"),
      open: tasks.latest<EditorSessionId, string, DeliveryRecord>("session"),
      save: tasks.latest<EditorRequestContext, undefined, DeliveryRecord>(
        "session",
      ),
      validate: tasks.latest<
        EditorRequestContext,
        undefined,
        ValidationCommandResult
      >("revision"),
      action: tasks.latest<EditorRequestContext, undefined, DeliveryRecord>(
        "revision",
      ),
      parseYaml: tasks.latest<
        EditorRequestContext,
        string,
        { config: JsonObject }
      >("revision"),
    };
    return {
      ...jobs,
      cancelRevisionJobs() {
        tasks.cancel("revision");
      },
      cancelEditorJobs() {
        tasks.cancel("revision");
        tasks.cancel("session");
      },
    };
  }, []);
}
