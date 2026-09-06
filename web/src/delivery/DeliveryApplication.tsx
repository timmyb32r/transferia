import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "preact/hooks";

import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import { DeliveryConfiguration } from "./DeliveryConfiguration";
import { tableConnectionIdentity } from "./useEndpointActions";
import { SourceMetadataContext, useSourceMetadata } from "./sourceMetadata";
import type { DeliveryType } from "../generated/apiContract";
import { DeliveryLogs } from "./DeliveryLogs";
import { PerformanceAdviceWorkspace } from "./PerformanceAdviceWorkspace";
import { SpeedtestWorkspace } from "./SpeedtestWorkspace";
import {
  DeliverySidebar,
  EditorActions,
  EditorTabs,
  OperationNotices,
} from "./EditorChrome";
import {
  isOperationPending,
  type OperationKey,
  type OperationState,
} from "../application/operations";
import {
  DataSchemaInspector,
  DataSchemaWorkspace,
  StatusPill,
} from "./EditorViews";
import { YamlEditorPanel } from "./YamlEditorPanel";
import {
  compiledSchema,
  completionIssueLabel,
  clonedConfig,
  clonedDeliveryName,
  configurationReadiness,
  endpointValue,
  isObject,
  errorMessage,
  freshConfig,
  validateCatalogSchemas,
} from "./editorConfig";
import { useDeliveryJobs, type EditorRequestContext } from "./useDeliveryJobs";
import { useDeliveryMutations } from "./useDeliveryMutations";
import { useDeliveryPolling } from "./useDeliveryPolling";
import { useDiscovery } from "./useDiscovery";
import { useOperations } from "./useOperations";
import { useYamlEditor } from "./useYamlEditor";
import { speedtestAvailability } from "./speedtestAvailability";
import { RequiredFieldGuide } from "./RequiredFieldGuide";
import { Button } from "../ui/Button";
import {
  nextRequiredTarget,
  requestRequiredGuidance,
  REQUIRED_CONTROL_SELECTOR,
} from "../ui/requiredGuidance";
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
  ValidationCommandResult,
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

type PostYamlIntent =
  | { kind: "validate" }
  | { kind: "reveal"; scope: "source" | "endpoints" | "all" };

interface PendingYamlIntent {
  intent: PostYamlIntent;
  context: EditorRequestContext;
}

interface ValidatedDiscoverySnapshot {
  context: EditorRequestContext;
  result: ValidationCommandResult;
}

type RuntimeActionIntent = "activate" | "pause" | "stop";

interface PendingRuntimeAction {
  sessionId: EditorSessionId;
  intent: RuntimeActionIntent;
}

function TransferIdentity({ id }: { id: string | undefined }) {
  const [copyState, setCopyState] = useState<
    "idle" | "copying" | "copied" | "error"
  >("idle");
  const copy = async () => {
    if (id === undefined || copyState === "copying") return;
    setCopyState("copying");
    try {
      await navigator.clipboard.writeText(id);
      setCopyState("copied");
    } catch {
      setCopyState("error");
    }
  };

  return (
    <div class="transfer-id-line">
      <small class="transfer-id-slot">
        {id === undefined ? "TRANSFER ID · assigned on save" : `TRANSFER ID · ${id}`}
      </small>
      {id !== undefined && (
        <Button
          shape="icon"
          class="transfer-id-copy"
          pending={copyState === "copying"}
          aria-label={copyState === "copied" ? "Transfer ID copied" : "Copy transfer ID"}
          title={copyState === "error" ? "Copy failed" : "Copy transfer ID"}
          onClick={() => void copy()}
        >
          <span class="ui-icon copy-icon" aria-hidden="true" />
        </Button>
      )}
    </div>
  );
}

export function DeliveryApplication() {
  const api = useControlPlane();
  const widgets = useWidgetRegistry();
  const [appearance, setAppearance] = useState<Appearance>(() =>
    loadAppearance(window.localStorage),
  );
  const [catalog, setCatalog] = useState<UiCatalog>();
  const [deliveries, setDeliveries] = useState<DeliverySummary[]>([]);
  const [requiredErrorScope, setRequiredErrorScope] = useState<
    "none" | "source" | "endpoints" | "all"
  >("none");
  const [pendingYamlIntent, setPendingYamlIntent] =
    useState<PendingYamlIntent>();
  const [validatedDiscoverySnapshot, setValidatedDiscoverySnapshot] =
    useState<ValidatedDiscoverySnapshot>();
  const [schemaInspectorVisible, setSchemaInspectorVisible] = useState(false);
  const [pendingRuntimeAction, setPendingRuntimeAction] =
    useState<PendingRuntimeAction>();
  const [editor, dispatch] = useReducer(editorReducer, EMPTY_STATE);
  const {
    operations,
    beginOperation,
    updateOperation,
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

  useLayoutEffect(() => {
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

  const readiness = useMemo(
    () =>
      catalog === undefined
        ? undefined
        : configurationReadiness(catalog, editor.config, widgets),
    [catalog, editor.config, widgets],
  );
  const selection = readiness?.selection;
  const selectedSourceConfig = selection?.sourceKey ? endpointValue(editor.config, "source", selection.sourceKey) : undefined;
  const requiredTableConnection = selection?.sourceKey && isObject(selectedSourceConfig)
    ? tableConnectionIdentity(selection.sourceKey, selectedSourceConfig) : undefined;
  const sourceMetadata = useSourceMetadata({
    connector: selection?.sourceKey ?? "", config: isObject(selectedSourceConfig) ? selectedSourceConfig : {},
    mode: requiredTableConnection !== undefined && typeof editor.config.delivery_type === "string"
      ? editor.config.delivery_type as DeliveryType : undefined,
    sessionKey: editor.sessionId, validating: isOperationPending(operations.validate),
  });
  const tableConnectionRequired = requiredTableConnection !== undefined &&
    !(sourceMetadata.check.state === "success" && sourceMetadata.check.status === "verified" && sourceMetadata.check.tables !== undefined);
  const metadataProgress = sourceMetadata.metadata?.validation;
  useEffect(() => {
    const operation = operations.validate;
    if (!operation || !isOperationPending(operation) || !metadataProgress ||
      metadataProgress.delivery_id !== editor.id || metadataProgress.revision !== editor.persistedRevision) return;
    updateOperation("validate", operation.requestId, metadataProgress.phase === "schemas"
      ? `Schemas checked ${metadataProgress.checked}/${metadataProgress.total}` : "Checking transforms and destination…");
  }, [metadataProgress, operations.validate?.requestId, editor.id, editor.persistedRevision, updateOperation]);
  const sourceSchemaComplete = readiness?.sourceSchemaReady ?? false;
  const structurallyComplete = readiness?.complete ?? false;
  const requiredFieldsComplete =
    structurallyComplete && editor.name.trim() !== "";
  const speedtest = useMemo(
    () =>
      catalog === undefined
        ? {
            available: false,
            reason: "Loading the connector catalog…",
          }
        : speedtestAvailability(catalog, editor.config, widgets),
    [catalog, editor.config, widgets],
  );
  const {
    discovery,
    setDiscovery,
    error: discoveryError,
  } = useDiscovery({
    editor,
    structurallyComplete: sourceSchemaComplete,
    metadataRequired: requiredTableConnection !== undefined,
    metadata: sourceMetadata.metadata,
    job: discoveryJob,
    operations: { beginOperation, finishOperation, clearOperation },
    isCurrentContext,
  });
  const dataSchemaAvailable = (discovery?.datasets.length ?? 0) > 0;
  const currentValidatedSnapshot =
    validatedDiscoverySnapshot?.context.sessionId === editor.sessionId &&
    validatedDiscoverySnapshot.context.localRevision === editor.localRevision &&
    validatedDiscoverySnapshot.result.delivery.id === editor.id &&
    validatedDiscoverySnapshot.result.delivery.revision ===
      editor.persistedRevision &&
    validatedDiscoverySnapshot.result.delivery.record_version ===
      editor.recordVersion &&
    validatedDiscoverySnapshot.result.delivery.validation.state === "ready" &&
    validatedDiscoverySnapshot.result.delivery.validation.revision ===
      editor.persistedRevision
      ? validatedDiscoverySnapshot
      : undefined;
  const validatedDiscovery = currentValidatedSnapshot?.result.discovery;
  const performanceAdviceCount =
    currentValidatedSnapshot === undefined
      ? undefined
      : (validatedDiscovery?.performance_advice.length ?? 0);
  const dataSchemaUnavailableReason = (() => {
    if (dataSchemaAvailable) return undefined;
    if (isOperationPending(operations.discovery))
      return "Discovering the data schema…";
    if (discoveryError !== undefined)
      return `Data schema discovery failed: ${discoveryError}`;
    if (catalog === undefined) return "Loading the connector catalog…";
    if (selection?.error !== undefined) return selection.error;
    if (selection?.source === undefined) return "Choose a source first";
    if (!sourceSchemaComplete) {
      const issue = readiness?.sourceSchemaIssue;
      if (
        issue !== undefined &&
        selection?.source !== undefined &&
        selection.sourceKey !== ""
      ) {
        const sourceNode = compiledSchema(selection.source.schema, widgets);
        const sourceValue = endpointValue(
          editor.config,
          "source",
          selection.sourceKey,
        );
        return `Fill required field: ${completionIssueLabel(
          sourceNode,
          sourceValue,
          issue,
          `#/source/${selection.sourceKey}`,
        )}`;
      }
      return "Complete the required source and parser settings";
    }
    if (discovery !== undefined)
      return "Discovery completed, but the selected parser produced no tables";
    return "Waiting for data schema discovery";
  })();
  useEffect(() => {
    if (!dataSchemaAvailable) setSchemaInspectorVisible(false);
  }, [dataSchemaAvailable]);
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
    ensureMetadata: async () => {
      if (requiredTableConnection === undefined) return undefined;
      if (sourceMetadata.metadata && !sourceMetadata.metadataError) return sourceMetadata.metadata.id;
      const metadata = await sourceMetadata.checkConnection();
      if (!metadata) throw new Error("Load source metadata before validation. See the connection status in Source.");
      return metadata.id;
    },
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
    onValidationResult: (context, result) =>
      setValidatedDiscoverySnapshot({ context, result }),
    isCurrentContext,
  });
  const {
    activeView,
    yamlDraft,
    editYaml,
    showYaml,
    applyYamlAndShowUi,
    showDataSchema,
    showSpeedtest,
    showPerformanceAdvice,
    showLogs,
  } = yamlEditor;

  const revealRequiredNow = useCallback(
    (scope: "source" | "endpoints" | "all") => {
      const reportUnrenderableIssue = () => {
        const issue =
          scope === "source"
            ? (readiness?.commonIssue ?? readiness?.sourceIssue)
            : scope === "endpoints"
              ? (readiness?.sourceIssue ?? readiness?.sinkIssue)
            : (readiness?.commonIssue ??
              readiness?.sourceIssue ??
              readiness?.sinkIssue);
        if (scope === "all") {
          void mutations.validate();
          return;
        }
        const requestId = beginOperation("discovery");
        finishOperation(
          "discovery",
          requestId,
          issue === undefined
            ? "The source configuration contains an issue that is not editable in the UI. Open the YAML view to correct it."
            : `The source configuration contains a non-editable issue at ${issue.path}. Open the YAML view to correct it.`,
        );
      };

      setRequiredErrorScope(scope);
      window.requestAnimationFrame(() => {
        const container = workspace.current;
        if (container === null) return;
        const excluded =
          scope === "source"
            ? ".identity-card, .endpoint-card-sink"
            : scope === "endpoints"
              ? ".identity-card, .pipeline-section"
              : undefined;
        requestRequiredGuidance(container, excluded);
        const missing = nextRequiredTarget(container, excluded);
        if (missing === undefined) {
          reportUnrenderableIssue();
          return;
        }
        const control = missing.matches(REQUIRED_CONTROL_SELECTOR)
          ? missing
          : missing.querySelector<HTMLElement>(REQUIRED_CONTROL_SELECTOR);
        if (control === null) {
          reportUnrenderableIssue();
          return;
        }
        missing.closest("details")?.setAttribute("open", "");
        missing.scrollIntoView({ behavior: "smooth", block: "center" });
        control.focus({ preventScroll: true });
      });
    },
    [beginOperation, finishOperation, mutations, readiness],
  );

  const executePostYamlIntent = useCallback(
    (intent: PostYamlIntent) => {
      if (intent.kind === "reveal") {
        revealRequiredNow(intent.scope);
        return;
      }
      if (requiredFieldsComplete) void mutations.validate();
      else revealRequiredNow("all");
    },
    [mutations, requiredFieldsComplete, revealRequiredNow],
  );

  useEffect(() => {
    if (pendingYamlIntent === undefined) return;
    if (
      pendingYamlIntent.context.sessionId !== editor.sessionId ||
      editor.localRevision > pendingYamlIntent.context.localRevision
    ) {
      setPendingYamlIntent(undefined);
      return;
    }
    if (
      editor.localRevision < pendingYamlIntent.context.localRevision ||
      activeView !== "ui"
    )
      return;
    setPendingYamlIntent(undefined);
    executePostYamlIntent(pendingYamlIntent.intent);
  }, [
    activeView,
    editor.localRevision,
    editor.sessionId,
    executePostYamlIntent,
    pendingYamlIntent,
  ]);

  const runAfterYaml = useCallback(
    (intent: PostYamlIntent) => {
      if (activeView === "ui") {
        executePostYamlIntent(intent);
        return;
      }
      void applyYamlAndShowUi().then((result) => {
        if (result.status === "failed") return;
        if (result.status === "applied") {
          setPendingYamlIntent({ intent, context: result.context });
          return;
        }
        executePostYamlIntent(intent);
      });
    },
    [activeView, applyYamlAndShowUi, executePostYamlIntent],
  );

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
    const connector = catalog.connectors.find(
      (candidate) => candidate.key === key,
    );
    const endpoint = connector?.[role];
    updateConfig({
      ...editor.config,
      [role]:
        endpoint === undefined
          ? {}
          : { [key]: structuredClone(endpoint.initial) },
    });
  };
  const operationPending = (key: OperationKey) => {
    return isOperationPending(operations[key]);
  };
  const blockingOperation = (
    ["bootstrap", "open", "save", "validate", "action", "parseYaml"] as const
  ).some(operationPending);
  const validatePending =
    operationPending("save") || operationPending("validate");
  const activatePending = operationPending("action");
  const runtimeActionIntent =
    pendingRuntimeAction?.sessionId === editor.sessionId
      ? pendingRuntimeAction.intent
      : undefined;
  const runRuntimeAction = (
    intent: RuntimeActionIntent,
    label: string,
    action: () => Promise<DeliveryRecord>,
  ) => {
    const sessionId = editor.sessionId;
    setPendingRuntimeAction({ sessionId, intent });
    void mutations.runAction(label, action).finally(() => {
      setPendingRuntimeAction((current) =>
        current?.sessionId === sessionId && current.intent === intent
          ? undefined
          : current,
      );
    });
  };
  const revealMissingRequiredFields = (scope: "source" | "all" = "all") =>
    runAfterYaml({ kind: "reveal", scope });
  const openSpeedtest = () => {
    void showSpeedtest(
      (config) => speedtestAvailability(catalog, config, widgets).available,
    ).then((result) => {
      if (result.status !== "unavailable") return;
      setPendingYamlIntent({
        intent: { kind: "reveal", scope: "endpoints" },
        context: result.context,
      });
    });
  };
  const actionButtons = (
    <EditorActions
      editor={editor}
      blocked={blockingOperation}
      validatePending={validatePending}
      activatePending={activatePending}
      runtimeActionIntent={runtimeActionIntent}
      requiredFieldsComplete={requiredFieldsComplete}
      connectionCheckRequired={tableConnectionRequired}
      onMissingRequired={revealMissingRequiredFields}
      onEdit={() => {
        setRequiredErrorScope("none");
        dispatch({ type: "edit" });
      }}
      onClone={() => {
        jobs.cancelEditorJobs();
        resetOperations({});
        setRequiredErrorScope("none");
        dispatch({
          type: "clone",
          sessionId: nextSession(),
          name: clonedDeliveryName(editor.name),
          description: editor.description,
          config: clonedConfig(catalog, editor.config),
        });
        yamlEditor.reset();
        setDiscovery(undefined);
        setSchemaInspectorVisible(false);
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
      onValidate={() => runAfterYaml({ kind: "validate" })}
      onActivate={() =>
        runRuntimeAction("activate", "Starting worker…", () =>
          api.activate(
            editor.id!,
            editor.persistedRevision!,
            editor.recordVersion!,
          ),
        )
      }
      onPause={(runId) =>
        runRuntimeAction("pause", "Pausing worker…", () =>
          api.stop(
            editor.id!,
            editor.persistedRevision!,
            editor.recordVersion!,
            runId,
          ),
        )
      }
      onStop={(runId) =>
        runRuntimeAction("stop", "Deactivating worker…", () =>
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
    <SourceMetadataContext.Provider value={sourceMetadata}><div class="shell">
      <DeliverySidebar
        deliveries={deliveries}
        selectedId={editor.id}
        appearance={appearance}
        catalog={catalog}
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
            <TransferIdentity id={editor.id} />
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
          speedtestAvailable={speedtest.available}
          speedtestUnavailableReason={speedtest.reason}
          performanceAdviceCount={performanceAdviceCount}
          onUi={() => void applyYamlAndShowUi()}
          onYaml={() => void showYaml()}
          onDataSchema={() => void showDataSchema()}
          onDataSchemaUnavailable={() => {
            if (discoveryError === undefined && selection?.source !== undefined && !sourceSchemaComplete) {
              revealMissingRequiredFields("source");
            } else if (!isOperationPending(operations.discovery) && dataSchemaUnavailableReason !== undefined) {
              const requestId = beginOperation("discovery");
              finishOperation("discovery", requestId, dataSchemaUnavailableReason);
            }
          }}
          onSpeedtest={openSpeedtest}
          onSpeedtestUnavailable={openSpeedtest}
          onPerformanceAdvice={() =>
            void showPerformanceAdvice(
              (config) =>
                config === editor.config && validatedDiscovery !== undefined,
            )
          }
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
        ) : activeView === "speedtest" ? (
          <SpeedtestWorkspace
            config={editor.config}
            estimate={(config, options, signal) =>
              api.speedtestEstimate({ config, ...options }, signal)
            }
            tune={(config, budget, options, signal) =>
              api.speedtestTune({ config, budget, ...options }, signal)
            }
          />
        ) : activeView === "performance_advice" ? (
          <PerformanceAdviceWorkspace result={validatedDiscovery} />
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
            loading={isOperationPending(operations.discovery)}
            onHide={() => setSchemaInspectorVisible(false)}
          />
        )}
      </main>
    </div></SourceMetadataContext.Provider>
  );
}
