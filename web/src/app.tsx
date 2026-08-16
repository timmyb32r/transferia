import { render } from "preact";
import { useEffect, useMemo, useReducer, useRef, useState } from "preact/hooks";

import { api } from "./api";
import {
  DeliverySidebar,
  EditorActions,
  EditorTabs,
  OperationNotices,
  type OperationKey,
  type OperationState,
} from "./delivery/EditorChrome";
import {
  CommonSettings,
  ContractView,
  EndpointCard,
  FieldLabel,
  StatusPill,
} from "./delivery/EditorViews";
import {
  compiledSchema,
  endpointValue,
  errorMessage,
  freshConfig,
  selectedEndpoints,
  stringValue,
} from "./delivery/editorConfig";
import { LatestJob } from "./effects";
import {
  ParserDetailsForm,
  SchemaForm,
  SelectControl,
} from "./schema/SchemaForm";
import { isComplete } from "./schema/compiler";
import {
  editorReducer,
  isDirty,
  isReadOnly,
  type EditorSessionId,
  type EditorState,
} from "./state";
import type {
  DeliveryRecord,
  DeliverySummary,
  DiscoveryResult,
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

interface EditorRequestContext {
  sessionId: EditorSessionId;
  localRevision: number;
}

export function App() {
  const [catalog, setCatalog] = useState<UiCatalog>();
  const [deliveries, setDeliveries] = useState<DeliverySummary[]>([]);
  const [editor, dispatch] = useReducer(editorReducer, EMPTY_STATE);
  const [yaml, setYaml] = useState("");
  const [yamlDraft, setYamlDraft] = useState("");
  const [activeView, setActiveView] = useState<"ui" | "yaml">("ui");
  const [discovery, setDiscovery] = useState<DiscoveryResult>();
  const [operations, setOperations] = useState<
    Partial<Record<OperationKey, OperationState>>
  >({});
  const operationSequence = useRef(0);
  const sessionSequence = useRef(0);
  const yamlJob = useRef(
    new LatestJob<EditorRequestContext, JsonObject, { yaml: string }>(),
  ).current;
  const discoveryJob = useRef(
    new LatestJob<EditorRequestContext, JsonObject, DiscoveryResult>(),
  ).current;
  const listJob = useRef(
    new LatestJob<void, undefined, DeliverySummary[]>(),
  ).current;
  const pollJob = useRef(
    new LatestJob<EditorSessionId, string, DeliveryRecord>(),
  ).current;
  const openJob = useRef(
    new LatestJob<EditorSessionId, string, DeliveryRecord>(),
  ).current;
  const saveJob = useRef(
    new LatestJob<EditorRequestContext, undefined, DeliveryRecord>(),
  ).current;
  const validateJob = useRef(
    new LatestJob<
      EditorRequestContext,
      undefined,
      {
        discovery?: DiscoveryResult;
        delivery: DeliveryRecord;
      }
    >(),
  ).current;
  const actionJob = useRef(
    new LatestJob<EditorRequestContext, undefined, DeliveryRecord>(),
  ).current;
  const parseYamlJob = useRef(
    new LatestJob<EditorRequestContext, string, { config: JsonObject }>(),
  ).current;
  const yamlEditing = useRef(false);
  const yamlContext = useRef<EditorRequestContext>();
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
  const beginOperation = (key: OperationKey, label?: string): number => {
    const requestId = ++operationSequence.current;
    setOperations((current) => ({
      ...current,
      [key]: { requestId, ...(label === undefined ? {} : { label }) },
    }));
    return requestId;
  };
  const finishOperation = (
    key: OperationKey,
    requestId: number,
    error?: string,
  ) =>
    setOperations((current) => {
      if (current[key]?.requestId !== requestId) return current;
      if (error !== undefined)
        return { ...current, [key]: { requestId, error } };
      const next = { ...current };
      delete next[key];
      return next;
    });
  const clearErrors = () =>
    setOperations((current) =>
      Object.fromEntries(
        Object.entries(current).filter(([, operation]) => !operation?.error),
      ),
    );
  const clearOperation = (key: OperationKey) =>
    setOperations((current) => {
      if (current[key] === undefined) return current;
      const next = { ...current };
      delete next[key];
      return next;
    });
  const cancelEditorJobs = () => {
    yamlJob.cancel();
    discoveryJob.cancel();
    pollJob.cancel();
    openJob.cancel();
    saveJob.cancel();
    validateJob.cancel();
    actionJob.cancel();
    parseYamlJob.cancel();
  };
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

  useEffect(() => {
    const timer = window.setInterval(() => {
      void listJob
        .run(undefined, undefined, () => api.deliveries())
        .then((result) => {
          if (result !== undefined) setDeliveries(result.value);
        })
        .catch(() => undefined);
      if (editor.id !== undefined && !isDirty(editor)) {
        const sessionId = editor.sessionId;
        void pollJob
          .run(sessionId, editor.id, (id) => api.delivery(id))
          .then((result) => {
            if (result !== undefined)
              dispatch({
                type: "runtime",
                sessionId: result.context,
                expectedLocalRevision: editor.localRevision,
                delivery: result.value,
              });
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
  ]);

  useEffect(() => {
    yamlJob.cancel();
    clearOperation("yaml");
    if (catalog === undefined) return;
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    const timer = window.setTimeout(() => {
      void yamlJob
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
          const requestId = beginOperation("yaml");
          finishOperation("yaml", requestId, errorMessage(reason));
        });
    }, 120);
    return () => {
      window.clearTimeout(timer);
      yamlJob.cancel();
    };
  }, [catalog, editor.config, editor.sessionId, editor.localRevision]);

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
  useEffect(() => {
    discoveryJob.cancel();
    clearOperation("discovery");
    setDiscovery(undefined);
    if (!structurallyComplete) return;
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    const timer = window.setTimeout(() => {
      const requestId = beginOperation(
        "discovery",
        "Discovering topology and schema…",
      );
      void discoveryJob
        .run(context, editor.config, (config, signal) =>
          api.discover(config, signal),
        )
        .then((result) => {
          if (result !== undefined && isCurrentContext(result.context)) {
            setDiscovery(result.value);
          }
          finishOperation("discovery", requestId);
        })
        .catch((reason: unknown) =>
          finishOperation("discovery", requestId, errorMessage(reason)),
        );
    }, 450);
    return () => {
      window.clearTimeout(timer);
      discoveryJob.cancel();
    };
  }, [
    editor.config,
    editor.sessionId,
    editor.localRevision,
    structurallyComplete,
  ]);

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
  const sourceProviders = catalog.providers.filter(
    (provider) => provider.source !== undefined,
  );
  const sinkProviders = catalog.providers.filter(
    (provider) => provider.sink !== undefined,
  );

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
  const refreshList = async () => {
    const requestId = beginOperation("list");
    try {
      const result = await listJob.run(undefined, undefined, () =>
        api.deliveries(),
      );
      if (result !== undefined) setDeliveries(result.value);
      finishOperation("list", requestId);
    } catch (reason) {
      finishOperation(
        "list",
        requestId,
        `Delivery list refresh failed: ${errorMessage(reason)}`,
      );
    }
  };
  const runAction = async (
    label: string,
    action: () => Promise<DeliveryRecord>,
  ) => {
    const requestId = beginOperation("action", label);
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    try {
      const result = await actionJob.run(context, undefined, action);
      if (result === undefined) {
        finishOperation("action", requestId);
        return undefined;
      }
      dispatch({
        type: "runtime",
        sessionId: result.context.sessionId,
        expectedLocalRevision: result.context.localRevision,
        delivery: result.value,
      });
      await refreshList();
      finishOperation("action", requestId);
      return result.value;
    } catch (reason) {
      finishOperation("action", requestId, errorMessage(reason));
      return undefined;
    }
  };
  const save = async (): Promise<DeliveryRecord | undefined> => {
    const requestId = beginOperation("save", "Saving draft…");
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    const snapshot = {
      id: editor.id,
      persistedRevision: editor.persistedRevision,
      recordVersion: editor.recordVersion,
      name: editor.name,
      description: editor.description,
      config: editor.config,
    };
    try {
      const result = await saveJob.run(context, undefined, () =>
        snapshot.id === undefined
          ? api.create(snapshot.name, snapshot.description, snapshot.config)
          : api.update(
              snapshot.id,
              snapshot.persistedRevision!,
              snapshot.recordVersion!,
              snapshot.name,
              snapshot.description,
              snapshot.config,
            ),
      );
      if (result === undefined) {
        finishOperation("save", requestId);
        return undefined;
      }
      dispatch({
        type: "persisted",
        sessionId: result.context.sessionId,
        savedLocalRevision: result.context.localRevision,
        delivery: result.value,
      });
      await refreshList();
      finishOperation("save", requestId);
      return result.value;
    } catch (reason) {
      finishOperation("save", requestId, errorMessage(reason));
      return undefined;
    }
  };
  const validate = async () => {
    const mustSave = isDirty(editor) || editor.id === undefined;
    const saved = mustSave ? await save() : undefined;
    if (mustSave && saved === undefined) return;
    const id = saved?.id ?? editor.id;
    const revision = saved?.revision ?? editor.persistedRevision;
    const recordVersion = saved?.record_version ?? editor.recordVersion;
    if (
      id === undefined ||
      revision === undefined ||
      recordVersion === undefined
    )
      return;
    const requestId = beginOperation(
      "validate",
      "Validating current revision…",
    );
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    try {
      const result = await validateJob.run(context, undefined, async () => {
        return api.validate(id, revision, recordVersion);
      });
      if (result === undefined) {
        finishOperation("validate", requestId);
        return;
      }
      if (
        result.value.discovery !== undefined &&
        isCurrentContext(result.context)
      )
        setDiscovery(result.value.discovery);
      dispatch({
        type: "runtime",
        sessionId: result.context.sessionId,
        expectedLocalRevision: result.context.localRevision,
        delivery: result.value.delivery,
      });
      await refreshList();
      finishOperation("validate", requestId);
    } catch (reason) {
      finishOperation("validate", requestId, errorMessage(reason));
    }
  };
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
      const requestId = beginOperation("yaml", "Rendering current YAML…");
      try {
        const result = await yamlJob.run(
          context,
          editor.config,
          (config, signal) => api.yaml(config, signal),
        );
        if (result === undefined || !isCurrentContext(result.context)) {
          finishOperation("yaml", requestId);
          return;
        }
        currentYaml = result.value.yaml;
        setYaml(currentYaml);
        yamlContext.current = result.context;
        finishOperation("yaml", requestId);
      } catch (reason) {
        finishOperation("yaml", requestId, errorMessage(reason));
        return;
      }
    }
    yamlEditing.current = true;
    setYamlDraft(currentYaml);
    setActiveView("yaml");
    clearErrors();
  };
  const applyYamlAndShowUi = async () => {
    if (activeView === "ui") return;
    const requestId = beginOperation("parseYaml", "Applying YAML…");
    const context = {
      sessionId: editor.sessionId,
      localRevision: editor.localRevision,
    };
    try {
      const result = await parseYamlJob.run(context, yamlDraft, (text) =>
        api.parseYaml(text),
      );
      if (result === undefined) {
        finishOperation("parseYaml", requestId);
        return;
      }
      if (!isCurrentContext(result.context)) {
        finishOperation("parseYaml", requestId);
        return;
      }
      dispatchLocalChange({ type: "config", config: result.value.config });
      yamlEditing.current = false;
      setActiveView("ui");
      finishOperation("parseYaml", requestId);
    } catch (reason) {
      finishOperation("parseYaml", requestId, errorMessage(reason));
    }
  };
  const blockingOperation = (
    ["bootstrap", "open", "save", "validate", "action", "parseYaml"] as const
  ).some((key) => operations[key]?.label !== undefined);
  const actionButtons = (
    <EditorActions
      editor={editor}
      blocked={blockingOperation}
      onSave={() => void save()}
      onValidate={() => void validate()}
      onActivate={() =>
        void runAction("Starting worker…", () =>
          api.activate(
            editor.id!,
            editor.persistedRevision!,
            editor.recordVersion!,
          ),
        )
      }
      onStop={(runId) =>
        void runAction("Stopping worker…", () =>
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
          cancelEditorJobs();
          setOperations({});
          dispatch({
            type: "new",
            sessionId: nextSession(),
            config: freshConfig(catalog),
          });
          yamlEditing.current = false;
          setActiveView("ui");
          setDiscovery(undefined);
        }}
        onOpen={(id) => {
          cancelEditorJobs();
          const sessionId = nextSession();
          const requestId = beginOperation("open", "Opening delivery…");
          void openJob
            .run(sessionId, id, (deliveryId) => api.delivery(deliveryId))
            .then((result) => {
              if (result === undefined) {
                finishOperation("open", requestId);
                return;
              }
              yamlEditing.current = false;
              setActiveView("ui");
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
          <div
            class="editor-view"
            role="tabpanel"
            key={`editor-${editor.sessionId}`}
          >
            <section class="card identity-card">
              <FieldLabel label="Delivery name" required>
                <input
                  type="text"
                  value={editor.name}
                  disabled={readOnly}
                  placeholder="e.g. Events to ClickHouse"
                  onInput={(event) =>
                    dispatchLocalChange({
                      type: "name",
                      name: event.currentTarget.value,
                    })
                  }
                />
              </FieldLabel>
              <FieldLabel label="Description">
                <input
                  type="text"
                  value={editor.description}
                  disabled={readOnly}
                  onInput={(event) =>
                    dispatchLocalChange({
                      type: "description",
                      description: event.currentTarget.value,
                    })
                  }
                />
              </FieldLabel>
              <FieldLabel label="Delivery type" required>
                <SelectControl
                  value={stringValue(editor.config.delivery_type)}
                  disabled={readOnly}
                  placeholder="Not selected"
                  options={[
                    { value: "batch", label: "Batch" },
                    { value: "stream", label: "Stream" },
                    { value: "batch_and_stream", label: "Batch + stream" },
                  ]}
                  onChange={(value) =>
                    updateConfig({ ...editor.config, delivery_type: value })
                  }
                />
              </FieldLabel>
            </section>

            <section class="route-composition">
              <EndpointCard
                title="Source"
                role="source"
                selectedKey={selection?.sourceKey ?? ""}
                providers={sourceProviders}
                {...(selection?.source === undefined ||
                selection.error !== undefined
                  ? {}
                  : { endpoint: selection.source })}
                config={editor.config}
                readOnly={readOnly}
                onChoose={chooseEndpoint}
                onConfig={updateConfig}
              />
              <div class="route-arrow">→</div>
              <EndpointCard
                title="Destination"
                role="sink"
                selectedKey={selection?.sinkKey ?? ""}
                providers={sinkProviders}
                {...(selection?.sink === undefined ||
                selection.error !== undefined
                  ? {}
                  : { endpoint: selection.sink })}
                config={editor.config}
                readOnly={readOnly}
                onChoose={chooseEndpoint}
                onConfig={updateConfig}
              />
              {selection?.error === undefined && selection?.source && (
                <ParserDetailsForm
                  node={compiledSchema(selection.source.schema)}
                  value={endpointValue(
                    editor.config,
                    "source",
                    selection.sourceKey,
                  )}
                  disabled={readOnly}
                  onChange={(next) =>
                    updateConfig({
                      ...editor.config,
                      source: { [selection.sourceKey]: next },
                    })
                  }
                />
              )}
            </section>
            {selection?.error && (
              <div class="compatibility-error">
                <strong>Incompatible route</strong>
                <span>{selection.error}</span>
              </div>
            )}

            <section class="pipeline-section">
              <h2>Pipeline settings</h2>
              <CommonSettings
                schema={catalog.common_schema}
                config={editor.config}
                disabled={readOnly}
                partitionedSource={selection?.source?.partitioned === true}
                onChange={updateConfig}
              />
            </section>

            {discovery && <ContractView result={discovery} />}
          </div>
        ) : (
          <section class="yaml-editor card" role="tabpanel">
            <div class="card-heading">
              <div>
                <small>RUNNABLE CONFIGURATION</small>
                <h2>YAML</h2>
              </div>
              <button
                type="button"
                onClick={() => void navigator.clipboard.writeText(yamlDraft)}
              >
                Copy
              </button>
            </div>
            <textarea
              aria-label="YAML configuration"
              spellcheck={false}
              value={yamlDraft}
              disabled={readOnly}
              onInput={(event) => {
                yamlEditing.current = true;
                setYamlDraft(event.currentTarget.value);
              }}
            />
            <p>
              Switch to UI to parse this YAML and continue editing it as a form.
            </p>
          </section>
        )}
      </main>
    </div>
  );
}

const appRoot = document.getElementById("app");
if (appRoot !== null) render(<App />, appRoot);
