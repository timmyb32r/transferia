import {
  useCallback,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "preact/hooks";

import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import { DeliveryConfiguration } from "./DeliveryConfiguration";
import { DeliveryLogs } from "./DeliveryLogs";
import {
  DeliverySidebar,
  EditorActions,
  EditorTabs,
  OperationNotices,
} from "./EditorChrome";
import type { OperationKey, OperationState } from "../application/operations";
import {
  DataSchemaInspector,
  DataSchemaWorkspace,
  StatusPill,
} from "./EditorViews";
import { YamlEditorPanel } from "./YamlEditorPanel";
import {
  compiledSchema,
  endpointValue,
  errorMessage,
  freshConfig,
  selectedEndpoints,
  validateCatalogSchemas,
} from "./editorConfig";
import { useDeliveryJobs, type EditorRequestContext } from "./useDeliveryJobs";
import { useDeliveryMutations } from "./useDeliveryMutations";
import { useDeliveryPolling } from "./useDeliveryPolling";
import { useDiscovery } from "./useDiscovery";
import { useOperations } from "./useOperations";
import { useYamlEditor } from "./useYamlEditor";
import { RequiredFieldGuide } from "./RequiredFieldGuide";
import {
  nextRequiredTarget,
  requestRequiredGuidance,
  REQUIRED_CONTROL_SELECTOR,
} from "../ui/requiredGuidance";
import { isComplete } from "../schema/compiler";
import { useWidgetRegistry } from "../schema/widgetRegistry";
import {
  editorReducer,
  isReadOnly,
  type EditorSessionId,
  type EditorState,
} from "../state";
import type {
  DeliveryRecord,
  DeliverySummary,
  JsonObject,
  UiCatalog,
} from "../types";
import {
  applyAppearance,
  loadAppearance,
  saveAppearance,
  type Appearance,
} from "../ui/appearance";

const EMPTY_STATE: EditorState = {
  sessionId: "bootstrap",
  editing: true,
  localRevision: 0,
  name: "",
  description: "",
  config: {},
  validation: { state: "draft" },
  runtime: { state: "stopped" },
};

export function DeliveryApplication() {
  const api = useControlPlane();
  const widgets = useWidgetRegistry();
  const [appearance, setAppearance] = useState<Appearance>(() =>
    loadAppearance(window.localStorage),
  );
  const [catalog, setCatalog] = useState<UiCatalog>();
  const [deliveries, setDeliveries] = useState<DeliverySummary[]>([]);
  const [requiredErrorScope, setRequiredErrorScope] = useState<
    "none" | "source" | "all"
  >("none");
  const [schemaInspectorVisible, setSchemaInspectorVisible] = useState(false);
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
  const workspace = useRef<HTMLElement>(null);
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

  useEffect(() => {
    applyAppearance(document.documentElement, appearance);
    saveAppearance(window.localStorage, appearance);
  }, [appearance]);

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
    jobs.cancelRevisionJobs();
    dispatch(action);
  };

  useEffect(() => {
    jobs.cancelRevisionJobs();
  }, [editor.sessionId, editor.localRevision]);

  useEffect(() => {
    const requestId = beginOperation("bootstrap", "Loading control plane…");
    void Promise.all([api.catalog(), api.deliveries()])
      .then(([nextCatalog, nextDeliveries]) => {
        validateCatalogSchemas(nextCatalog, widgets);
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
  const handlePollingError = useCallback(
    (message: string | undefined) => {
      if (message === undefined) {
        clearOperation("poll");
        return;
      }
      const requestId = beginOperation("poll");
      finishOperation("poll", requestId, `Polling failed: ${message}`);
    },
    [beginOperation, clearOperation, finishOperation],
  );
  useDeliveryPolling({
    editor,
    listJob,
    pollJob,
    onDeliveries: setDeliveries,
    onRuntime: applyPolledRuntime,
    onError: handlePollingError,
  });

  const selection = useMemo(
    () =>
      catalog === undefined
        ? undefined
        : selectedEndpoints(catalog, editor.config),
    [catalog, editor.config],
  );
  const commonConfigComplete =
    catalog !== undefined &&
    selection !== undefined &&
    selection.error === undefined &&
    isComplete(compiledSchema(catalog.common_schema, widgets), editor.config);
  const sourceSchemaComplete =
    commonConfigComplete &&
    selection?.source !== undefined &&
    isComplete(
      compiledSchema(selection.source!.schema, widgets),
      endpointValue(editor.config, "source", selection!.sourceKey),
    );
  const structurallyComplete =
    sourceSchemaComplete &&
    selection?.sink !== undefined &&
    isComplete(
      compiledSchema(selection.sink!.schema, widgets),
      endpointValue(editor.config, "sink", selection!.sinkKey),
    );
  const requiredFieldsComplete =
    structurallyComplete && editor.name.trim() !== "";
  const { discovery, setDiscovery } = useDiscovery({
    editor,
    structurallyComplete: sourceSchemaComplete,
    job: discoveryJob,
    operations: { beginOperation, finishOperation, clearOperation },
    isCurrentContext,
  });
  const dataSchemaAvailable = (discovery?.datasets.length ?? 0) > 0;
  const dataSchemaUnavailableReason = (() => {
    if (dataSchemaAvailable) return undefined;
    if (operations.discovery?.label !== undefined)
      return "Discovering the data schema…";
    if (operations.discovery?.error !== undefined)
      return `Data schema discovery failed: ${operations.discovery.error}`;
    if (catalog === undefined) return "Loading the provider catalog…";
    if (selection?.error !== undefined) return selection.error;
    if (selection?.source === undefined) return "Choose a source first";
    if (!sourceSchemaComplete)
      return "Complete the required source and parser settings";
    if (discovery !== undefined)
      return "Discovery completed, but the selected parser produced no tables";
    return "Waiting for data schema discovery";
  })();
  useEffect(() => {
    if (!dataSchemaAvailable) {
      setSchemaInspectorVisible(false);
    } else if (appearance.autoShowSchemaWidget && editor.id === undefined) {
      setSchemaInspectorVisible(true);
    }
  }, [dataSchemaAvailable, appearance.autoShowSchemaWidget, editor.id]);
  const yamlEditor = useYamlEditor({
    enabled: catalog !== undefined,
    editable: !isReadOnly(editor),
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
  const {
    activeView,
    yamlDraft,
    editYaml,
    showYaml,
    applyYamlAndShowUi,
    showDataSchema,
    showLogs,
  } = yamlEditor;

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
  const revealMissingRequiredFields = (scope: "source" | "all" = "all") => {
    setRequiredErrorScope(scope);
    void applyYamlAndShowUi().then(() => {
      window.requestAnimationFrame(() => {
        const container = workspace.current;
        if (container === null) return;
        const excluded = scope === "source" ? ".endpoint-card-sink, .serializer-details-card" : undefined;
        requestRequiredGuidance(container, excluded);
        const missing = nextRequiredTarget(container, excluded);
        missing?.closest("details")?.setAttribute("open", "");
        missing?.scrollIntoView({ behavior: "smooth", block: "center" });
        missing
          ?.querySelector<HTMLElement>(REQUIRED_CONTROL_SELECTOR)
          ?.focus({ preventScroll: true });
      });
    });
  };
  const actionButtons = (
    <EditorActions
      editor={editor}
      blocked={blockingOperation}
      requiredFieldsComplete={requiredFieldsComplete}
      onMissingRequired={revealMissingRequiredFields}
      onEdit={() => {
        setRequiredErrorScope("none");
        dispatch({ type: "edit" });
      }}
      onDelete={() => {
        if (!window.confirm(`Delete delivery “${editor.name}”?`)) return;
        void mutations.remove().then((deleted) => {
          if (!deleted) return;
          jobs.cancelEditorJobs();
          resetOperations({});
          setRequiredErrorScope("none");
          dispatch({
            type: "new",
            sessionId: nextSession(),
            config: freshConfig(catalog),
          });
          yamlEditor.reset();
          setDiscovery(undefined);
        });
      }}
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
        appearance={appearance}
        onAppearance={setAppearance}
        dataWidgetAvailable={dataSchemaAvailable}
        dataWidgetUnavailableReason={dataSchemaUnavailableReason}
        dataWidgetVisible={schemaInspectorVisible}
        onToggleDataWidget={() =>
          setSchemaInspectorVisible((visible) => !visible)
        }
        onNew={() => {
          jobs.cancelEditorJobs();
          resetOperations({});
          setRequiredErrorScope("none");
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
              setRequiredErrorScope("none");
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

      <main ref={workspace} class="workspace">
        <RequiredFieldGuide
          root={workspace}
          enabled={!readOnly && activeView === "ui"}
          revision={editor.localRevision}
          tone={requiredErrorScope !== "none" ? "error" : "guided"}
        />
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
          dataSchemaAvailable={dataSchemaAvailable}
          dataSchemaUnavailableReason={dataSchemaUnavailableReason}
          onUi={() => void applyYamlAndShowUi()}
          onYaml={() => void showYaml()}
          onDataSchema={() => void showDataSchema()}
          onDataSchemaUnavailable={() => revealMissingRequiredFields("source")}
          onLogs={() => void showLogs()}
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
            readOnly={readOnly}
            requiredErrorScope={requiredErrorScope}
            onName={(name) => dispatchLocalChange({ type: "name", name })}
            onDescription={(description) =>
              dispatchLocalChange({ type: "description", description })
            }
            onConfig={updateConfig}
            onChooseEndpoint={chooseEndpoint}
          />
        ) : activeView === "yaml" ? (
          <YamlEditorPanel
            value={yamlDraft}
            disabled={readOnly}
            onChange={editYaml}
          />
        ) : activeView === "data_schema" && discovery !== undefined ? (
          <DataSchemaWorkspace result={discovery} />
        ) : activeView === "logs" ? (
          editor.id === undefined ? (
            <p class="data-schema-empty">
              Save the delivery to view worker logs.
            </p>
          ) : (
            <DeliveryLogs deliveryId={editor.id} />
          )
        ) : null}
        {schemaInspectorVisible && discovery !== undefined && (
          <DataSchemaInspector
            result={discovery}
            loading={operations.discovery?.label !== undefined}
            onHide={() => setSchemaInspectorVisible(false)}
          />
        )}
      </main>
    </div>
  );
}
