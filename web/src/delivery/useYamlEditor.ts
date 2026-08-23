import { useEffect, useRef, useState } from "preact/hooks";

import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import type { EditorState } from "../state";
import type { JsonObject } from "../types";
import type { EditorRequestContext, useDeliveryJobs } from "./useDeliveryJobs";
import type { useOperations } from "./useOperations";

type DeliveryJobs = ReturnType<typeof useDeliveryJobs>;
type Operations = ReturnType<typeof useOperations>;
export type EditorView = "ui" | "yaml" | "data_schema" | "logs";
export type ApplyYamlResult =
  | { status: "current" }
  | { status: "applied"; context: EditorRequestContext }
  | { status: "failed" };

export function useYamlEditor({
  enabled,
  editable,
  editor,
  jobs,
  operations,
  isCurrentContext,
  applyConfig,
}: {
  enabled: boolean;
  editable: boolean;
  editor: EditorState;
  jobs: Pick<DeliveryJobs, "yaml" | "parseYaml">;
  operations: Pick<
    Operations,
    "beginOperation" | "finishOperation" | "clearOperation" | "clearErrors"
  >;
  isCurrentContext: (context: EditorRequestContext) => boolean;
  applyConfig: (config: JsonObject) => void;
}) {
  const api = useControlPlane();
  const [yaml, setYaml] = useState("");
  const [yamlDraft, setYamlDraft] = useState("");
  const [activeView, setActiveView] = useState<EditorView>("ui");
  const yamlEditing = useRef(false);
  const yamlContext = useRef<EditorRequestContext>();

  useEffect(() => {
    jobs.yaml.cancel();
    operations.clearOperation("yaml");
    if (!enabled) return;
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    const timer = window.setTimeout(() => {
      void jobs.yaml
        .run(context, editor.config, (config, signal) =>
          api.yaml(config, signal),
        )
        .then((result) => {
          if (result === undefined || !isCurrentContext(result.context)) return;
          setYaml(result.value.yaml);
          yamlContext.current = result.context;
          if (!yamlEditing.current) setYamlDraft(result.value.yaml);
        })
        .catch((reason: unknown) => {
          const requestId = operations.beginOperation("yaml");
          operations.finishOperation("yaml", requestId, errorMessage(reason));
        });
    }, 120);
    return () => {
      window.clearTimeout(timer);
      jobs.yaml.cancel();
    };
  }, [enabled, editor.config, editor.sessionId, editor.localRevision]);

  const showYaml = async () => {
    if (activeView === "yaml") return;
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    let currentYaml = yaml;
    if (
      yamlContext.current?.sessionId !== context.sessionId ||
      yamlContext.current.localRevision !== context.localRevision
    ) {
      const requestId = operations.beginOperation(
        "yaml",
        "Rendering current YAML…",
      );
      try {
        const result = await jobs.yaml.run(
          context,
          editor.config,
          (config, signal) => api.yaml(config, signal),
        );
        if (result === undefined || !isCurrentContext(result.context)) {
          operations.finishOperation("yaml", requestId);
          return;
        }
        currentYaml = result.value.yaml;
        setYaml(currentYaml);
        yamlContext.current = result.context;
        operations.finishOperation("yaml", requestId);
      } catch (reason) {
        operations.finishOperation("yaml", requestId, errorMessage(reason));
        return;
      }
    }
    yamlEditing.current = editable;
    setYamlDraft(currentYaml);
    setActiveView("yaml");
    operations.clearErrors();
  };

  const applyYamlAndShow = async (
    target: Exclude<EditorView, "yaml">,
  ): Promise<ApplyYamlResult> => {
    if (activeView === target) return { status: "current" };
    if (activeView !== "yaml") {
      setActiveView(target);
      return { status: "current" };
    }
    if (!editable) {
      yamlEditing.current = false;
      setActiveView(target);
      return { status: "current" };
    }
    const requestId = operations.beginOperation("parseYaml", "Applying YAML…");
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    try {
      const result = await jobs.parseYaml.run(context, yamlDraft, (text) =>
        api.parseYaml(text),
      );
      if (result === undefined || !isCurrentContext(result.context)) {
        operations.finishOperation("parseYaml", requestId);
        return { status: "failed" };
      }
      applyConfig(result.value.config);
      yamlEditing.current = false;
      setActiveView(target);
      operations.finishOperation("parseYaml", requestId);
      return {
        status: "applied",
        context: {
          sessionId: result.context.sessionId,
          localRevision: result.context.localRevision + 1,
        },
      };
    } catch (reason) {
      operations.finishOperation("parseYaml", requestId, errorMessage(reason));
      return { status: "failed" };
    }
  };

  const reset = () => {
    yamlEditing.current = false;
    setActiveView("ui");
  };

  const editYaml = (value: string) => {
    yamlEditing.current = true;
    setYamlDraft(value);
  };

  return {
    activeView,
    yamlDraft,
    editYaml,
    showYaml,
    applyYamlAndShowUi: () => applyYamlAndShow("ui"),
    showDataSchema: () => applyYamlAndShow("data_schema"),
    showLogs: () => applyYamlAndShow("logs"),
    reset,
  };
}

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}
