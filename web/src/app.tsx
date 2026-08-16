import { render } from "preact";
import {
  useCallback,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "preact/hooks";

import { api } from "./api";
import { DeliveryConfiguration } from "./delivery/DeliveryConfiguration";
import {
  DeliverySidebar,
  EditorActions,
  EditorTabs,
  OperationNotices,
  type OperationKey,
  type OperationState,
} from "./delivery/EditorChrome";
import { StatusPill } from "./delivery/EditorViews";
import { YamlEditorPanel } from "./delivery/YamlEditorPanel";
import {
  compiledSchema,
  endpointValue,
  errorMessage,
  freshConfig,
  selectedEndpoints,
} from "./delivery/editorConfig";
import {
  useDeliveryJobs,
  type EditorRequestContext,
} from "./delivery/useDeliveryJobs";
import { useDeliveryMutations } from "./delivery/useDeliveryMutations";
import { useDeliveryPolling } from "./delivery/useDeliveryPolling";
import { useDiscovery } from "./delivery/useDiscovery";
import { useOperations } from "./delivery/useOperations";
import { useYamlEditor } from "./delivery/useYamlEditor";
import { isComplete } from "./schema/compiler";
import {
  editorReducer,
  isReadOnly,
  type EditorSessionId,
  type EditorState,
} from "./state";
import type {
  DeliveryRecord,
  DeliverySummary,
  JsonObject,
  UiCatalog,
} from "./types";

const EMPTY_STATE: EditorState = {
  sessionId: "bootstrap",
  localRevision: 0,
  name: "",
  description: "",
  config: {},
  validation: { state: "draft" },
  runtime: { state: "stopped" },
};

export function App() {
  const [catalog, setCatalog] = useState<UiCatalog>();
  const [deliveries, setDeliveries] = useState<DeliverySummary[]>([]);
  const [editor, dispatch] = useReducer(editorReducer, EMPTY_STATE);
  const {
    operations,
    beginOperation,
    finishOperation,
    clearErrors,
    clearOperation,
    resetOperations,
  } = useOperations();
  const sessionSequence = useRef(0);
  const jobs = useDeliveryJobs();
  const {
    yaml: yamlJob,
    discovery: discoveryJob,
    list: listJob,
    poll: pollJob,
    open: openJob,
    save: saveJob,
    validate: validateJob,
    action: actionJob,
    parseYaml: parseYamlJob,
  } = jobs;
  const currentEditorContext = useRef<EditorRequestContext>({
    sessionId: editor.sessionId,
    localRevision: editor.localRevision,
  });
  currentEditorContext.current = {
    sessionId: editor.sessionId,
    localRevision: editor.localRevision,
  };

  const nextSession = (): EditorSessionId =>
    `editor-${++sessionSequence.current}`;
  const isCurrentContext = (context: EditorRequestContext): boolean =>
    context.sessionId === currentEditorContext.current.sessionId &&
    context.localRevision === currentEditorContext.current.localRevision;
  const dispatchLocalChange = (
    action:
      | { type: "name"; name: string }
      | { type: "description"; description: string }
      | { type: "config"; config: JsonObject },
  ) => {
    yamlJob.cancel();
    discoveryJob.cancel();
    pollJob.cancel();
    validateJob.cancel();
    actionJob.cancel();
    parseYamlJob.cancel();
    dispatch(action);
  };

  useEffect(() => {
    validateJob.cancel();
    actionJob.cancel();
    parseYamlJob.cancel();
  }, [editor.sessionId, editor.localRevision]);

  useEffect(() => {
    const requestId = beginOperation("bootstrap", "Loading control plane…");
    void Promise.all([api.catalog(), api.deliveries()])
      .then(([nextCatalog, nextDeliveries]) => {
        setCatalog(nextCatalog);
        setDeliveries(nextDeliveries);
        dispatch({
          type: "new",
          sessionId: nextSession(),
          config: freshConfig(nextCatalog),
        });
        finishOperation("bootstrap", requestId);
      })
      .catch((reason: unknown) =>
        finishOperation("bootstrap", requestId, errorMessage(reason)),
      );
  }, []);

  const applyPolledRuntime = useCallback(
    (
      sessionId: EditorSessionId,
      expectedLocalRevision: number,
      delivery: DeliveryRecord,
    ) =>
      dispatch({
        type: "runtime",
        sessionId,
        expectedLocalRevision,
        delivery,
      }),
    [],
  );
  useDeliveryPolling({
    editor,
    listJob,
    pollJob,
    onDeliveries: setDeliveries,
    onRuntime: applyPolledRuntime,
  });

  const selection = useMemo(
    () =>
      catalog === undefined
        ? undefined
        : selectedEndpoints(catalog, editor.config),
    [catalog, editor.config],
  );
  const structurallyComplete =
    catalog !== undefined &&
    selection !== undefined &&
    selection.error === undefined &&
    selection.source !== undefined &&
    selection.sink !== undefined &&
    isComplete(compiledSchema(catalog.common_schema), editor.config) &&
    isComplete(
      compiledSchema(selection.source.schema),
      endpointValue(editor.config, "source", selection.sourceKey),
    ) &&
    isComplete(
      compiledSchema(selection.sink.schema),
      endpointValue(editor.config, "sink", selection.sinkKey),
    );
  const { discovery, setDiscovery } = useDiscovery({
    editor,
    structurallyComplete,
    job: discoveryJob,
    operations: { beginOperation, finishOperation, clearOperation },
    isCurrentContext,
  });
  const yamlEditor = useYamlEditor({
    enabled: catalog !== undefined,
    editor,
    jobs: { yaml: yamlJob, parseYaml: parseYamlJob },
    operations: {
      beginOperation,
      finishOperation,
      clearOperation,
      clearErrors,
    },
    isCurrentContext,
    applyConfig: (config) => dispatchLocalChange({ type: "config", config }),
  });
  const applyPersisted = useCallback(
    (context: EditorRequestContext, delivery: DeliveryRecord) =>
      dispatch({
        type: "persisted",
        sessionId: context.sessionId,
        savedLocalRevision: context.localRevision,
        delivery,
      }),
    [],
  );
  const applyMutationRuntime = useCallback(
    (context: EditorRequestContext, delivery: DeliveryRecord) =>
      dispatch({
        type: "runtime",
        sessionId: context.sessionId,
        expectedLocalRevision: context.localRevision,
        delivery,
      }),
    [],
  );
  const mutations = useDeliveryMutations({
    editor,
    jobs: {
      list: listJob,
      save: saveJob,
      validate: validateJob,
      action: actionJob,
    },
    operations: { beginOperation, finishOperation },
    onDeliveries: setDeliveries,
    onPersisted: applyPersisted,
    onRuntime: applyMutationRuntime,
    onDiscovery: setDiscovery,
    isCurrentContext,
  });
  const { activeView, yamlDraft, editYaml, showYaml, applyYamlAndShowUi } =
    yamlEditor;

  if (catalog === undefined)
    return (
      <main class="loading-screen">
        {operations.bootstrap?.error === undefined ? (
          <>
            <span class="spinner" /> Loading control plane…
          </>
        ) : (
          <span>{operations.bootstrap.error}</span>
        )}
      </main>
    );

  const readOnly = isReadOnly(editor);
  const updateConfig = (next: JsonObject) =>
    dispatchLocalChange({ type: "config", config: next });
  const chooseEndpoint = (role: "source" | "sink", key: string) => {
    const provider = catalog.providers.find(
      (candidate) => candidate.key === key,
    );
    const endpoint = provider?.[role];
    updateConfig({
      ...editor.config,
      [role]:
        endpoint === undefined
          ? {}
          : { [key]: structuredClone(endpoint.initial) },
    });
  };
  const blockingOperation = (
    ["bootstrap", "open", "save", "validate", "action", "parseYaml"] as const
  ).some((key) => operations[key]?.label !== undefined);
  const actionButtons = (
    <EditorActions
      editor={editor}
      blocked={blockingOperation}
      onSave={() => void mutations.save()}
      onValidate={() => void mutations.validate()}
      onActivate={() =>
        void mutations.runAction("Starting worker…", () =>
          api.activate(
            editor.id!,
            editor.persistedRevision!,
            editor.recordVersion!,
          ),
        )
      }
      onStop={(runId) =>
        void mutations.runAction("Stopping worker…", () =>
          api.stop(
            editor.id!,
            editor.persistedRevision!,
            editor.recordVersion!,
            runId,
          ),
        )
      }
    />
  );

  return (
    <div class="shell">
      <DeliverySidebar
        deliveries={deliveries}
        selectedId={editor.id}
        onNew={() => {
          jobs.cancelEditorJobs();
          resetOperations({});
          dispatch({
            type: "new",
            sessionId: nextSession(),
            config: freshConfig(catalog),
          });
          yamlEditor.reset();
          setDiscovery(undefined);
        }}
        onOpen={(id) => {
          jobs.cancelEditorJobs();
          const sessionId = nextSession();
          const requestId = beginOperation("open", "Opening delivery…");
          void openJob
            .run(sessionId, id, (deliveryId) => api.delivery(deliveryId))
            .then((result) => {
              if (result === undefined) {
                finishOperation("open", requestId);
                return;
              }
              yamlEditor.reset();
              setDiscovery(undefined);
              dispatch({
                type: "open",
                sessionId: result.context,
                delivery: result.value,
              });
              finishOperation("open", requestId);
            })
            .catch((reason: unknown) =>
              finishOperation("open", requestId, errorMessage(reason)),
            );
        }}
      />

      <main class="workspace">
        <header class="page-header">
          <div>
            <small>
              {editor.id === undefined
                ? "NEW DELIVERY"
                : `DELIVERY · ${editor.id}`}
            </small>
            <h1>{editor.name || "Untitled delivery"}</h1>
          </div>
          <div class="header-controls">
            {editor.id !== undefined && (
              <StatusPill runtime={editor.runtime.state} />
            )}
            {actionButtons}
          </div>
        </header>
        <EditorTabs
          active={activeView}
          disabled={blockingOperation}
          onUi={() => void applyYamlAndShowUi()}
          onYaml={() => void showYaml()}
        />
        <OperationNotices
          operations={operations}
          onDismiss={(key, requestId) => finishOperation(key, requestId)}
        />

        {activeView === "ui" ? (
          <DeliveryConfiguration
            catalog={catalog}
            editor={editor}
            selection={selection}
            discovery={discovery}
            readOnly={readOnly}
            onName={(name) => dispatchLocalChange({ type: "name", name })}
            onDescription={(description) =>
              dispatchLocalChange({ type: "description", description })
            }
            onConfig={updateConfig}
            onChooseEndpoint={chooseEndpoint}
          />
        ) : (
          <YamlEditorPanel
            value={yamlDraft}
            disabled={readOnly}
            onChange={editYaml}
          />
        )}
      </main>
    </div>
  );
}

const appRoot = document.getElementById("app");
if (appRoot !== null) render(<App />, appRoot);
