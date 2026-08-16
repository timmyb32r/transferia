import { render } from "preact";
import { useEffect, useMemo, useReducer, useRef, useState } from "preact/hooks";

import { api } from "./api";
import { LatestJob } from "./effects";
import {
  ParserDetailsForm,
  SchemaForm,
  SelectControl,
} from "./schema/SchemaForm";
import {
  compileSchema,
  isComplete,
  type CompiledNode,
} from "./schema/compiler";
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
  EndpointDefinition,
  JsonObject,
  JsonValue,
  ProviderDefinition,
  UiCatalog,
} from "./types";

const compiledSchemaCache = new WeakMap<object, CompiledNode>();

function compiledSchema(schema: UiCatalog["common_schema"]): CompiledNode {
  const cached = compiledSchemaCache.get(schema);
  if (cached !== undefined) return cached;
  const compiled = compileSchema(schema);
  compiledSchemaCache.set(schema, compiled);
  return compiled;
}

const EMPTY_STATE: EditorState = {
  sessionId: "bootstrap",
  localRevision: 0,
  name: "",
  description: "",
  config: {},
  validation: { state: "draft" },
  runtime: { state: "stopped" },
};

type OperationKey =
  | "bootstrap"
  | "list"
  | "open"
  | "save"
  | "validate"
  | "action"
  | "yaml"
  | "parseYaml"
  | "discovery";

interface OperationState {
  requestId: number;
  label?: string;
  error?: string;
}

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
        discovery: DiscoveryResult;
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
    selection !== undefined &&
    selection.error === undefined &&
    selection.source !== undefined &&
    selection.sink !== undefined &&
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
    const result = await listJob.run(undefined, undefined, () =>
      api.deliveries(),
    );
    if (result !== undefined) setDeliveries(result.value);
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
    const saved =
      isDirty(editor) || editor.id === undefined ? await save() : undefined;
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
        const nextDiscovery = await api.validate(id, revision, recordVersion);
        const delivery = await api.delivery(id);
        return { discovery: nextDiscovery, delivery };
      });
      if (result === undefined) {
        finishOperation("validate", requestId);
        return;
      }
      if (isCurrentContext(result.context))
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
  const showYaml = () => {
    if (activeView === "yaml") return;
    yamlEditing.current = true;
    setYamlDraft(yaml);
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
  const runningRunId =
    editor.runtime.state === "running" ? editor.runtime.run_id : undefined;
  const actionButtons = (
    <div class="actions">
      {runningRunId !== undefined ? (
        <button
          class="danger-button"
          type="button"
          disabled={blockingOperation}
          onClick={() =>
            void runAction("Stopping worker…", () =>
              api.stop(
                editor.id!,
                editor.persistedRevision!,
                editor.recordVersion!,
                runningRunId,
              ),
            )
          }
        >
          Stop
        </button>
      ) : (
        <>
          <button
            type="button"
            disabled={blockingOperation || !isDirty(editor)}
            onClick={() => void save()}
          >
            Save draft
          </button>
          <button
            type="button"
            disabled={blockingOperation || editor.name.trim() === ""}
            onClick={() => void validate()}
          >
            Validate
          </button>
          <button
            class="primary"
            type="button"
            disabled={
              blockingOperation ||
              editor.id === undefined ||
              isDirty(editor) ||
              editor.validation.state !== "ready" ||
              editor.validation.revision !== editor.persistedRevision
            }
            onClick={() =>
              void runAction("Starting worker…", () =>
                api.activate(
                  editor.id!,
                  editor.persistedRevision!,
                  editor.recordVersion!,
                ),
              )
            }
          >
            Activate
          </button>
        </>
      )}
    </div>
  );

  return (
    <div class="shell">
      <aside class="sidebar">
        <div class="brand">
          <span class="brand-mark">T</span>
          <div>
            <strong>Transferia</strong>
            <small>Local control plane</small>
          </div>
        </div>
        <button
          class="primary new-button"
          type="button"
          onClick={() => {
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
        >
          + New delivery
        </button>
        <nav class="delivery-list">
          {deliveries.map((delivery) => (
            <button
              type="button"
              class={
                delivery.id === editor.id
                  ? "delivery-item active"
                  : "delivery-item"
              }
              onClick={() => {
                cancelEditorJobs();
                const sessionId = nextSession();
                const requestId = beginOperation("open", "Opening delivery…");
                void openJob
                  .run(sessionId, delivery.id, (id) => api.delivery(id))
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
            >
              <span>{delivery.name}</span>
              <StatusPill runtime={delivery.runtime.state} />
            </button>
          ))}
          {deliveries.length === 0 && (
            <p class="empty-list">No saved deliveries yet.</p>
          )}
        </nav>
      </aside>

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
        <div class="editor-tabs" role="tablist" aria-label="Configuration view">
          <button
            type="button"
            role="tab"
            aria-selected={activeView === "ui"}
            class={activeView === "ui" ? "active" : ""}
            disabled={blockingOperation}
            onClick={() => void applyYamlAndShowUi()}
          >
            UI
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={activeView === "yaml"}
            class={activeView === "yaml" ? "active" : ""}
            disabled={blockingOperation}
            onClick={showYaml}
          >
            YAML
          </button>
        </div>
        {Object.entries(operations).map(
          ([key, operation]) =>
            operation?.error && (
              <div class="notice error" key={key}>
                <span>{operation.error}</span>
                <button
                  type="button"
                  onClick={() =>
                    finishOperation(key as OperationKey, operation.requestId)
                  }
                >
                  ×
                </button>
              </div>
            ),
        )}
        {Object.values(operations).map(
          (operation) =>
            operation?.label && (
              <div class="notice progress" key={operation.requestId}>
                <span class="spinner" />
                {operation.label}
              </div>
            ),
        )}

        {activeView === "ui" ? (
          <div class="editor-view" role="tabpanel">
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

function EndpointCard(props: {
  title: string;
  role: "source" | "sink";
  selectedKey: string;
  providers: ProviderDefinition[];
  endpoint?: EndpointDefinition;
  config: JsonObject;
  readOnly: boolean;
  onChoose: (role: "source" | "sink", key: string) => void;
  onConfig: (config: JsonObject) => void;
}) {
  const value =
    props.endpoint === undefined
      ? {}
      : endpointValue(props.config, props.role, props.selectedKey);
  return (
    <article class={`card endpoint-card endpoint-card-${props.role}`}>
      <h2>{props.title}</h2>
      <SelectControl
        searchable
        value={props.selectedKey}
        disabled={props.readOnly}
        placeholder="Not selected"
        options={props.providers.map((provider) => ({
          value: provider.key,
          label: provider.title,
        }))}
        onChange={(key) => props.onChoose(props.role, key)}
      />
      {props.endpoint && (
        <div class="endpoint-fields">
          <SchemaForm
            node={compiledSchema(props.endpoint.schema)}
            value={value}
            disabled={props.readOnly}
            parserSelectionOnly={props.role === "source"}
            onChange={(next) =>
              props.onConfig({
                ...props.config,
                [props.role]: { [props.selectedKey]: next },
              })
            }
          />
        </div>
      )}
    </article>
  );
}

function CommonSettings({
  schema,
  config,
  disabled,
  partitionedSource,
  onChange,
}: {
  schema: UiCatalog["common_schema"];
  config: JsonObject;
  disabled: boolean;
  partitionedSource: boolean;
  onChange: (config: JsonObject) => void;
}) {
  const compiled = compiledSchema(schema);
  if (compiled.kind !== "object") return null;
  const excluded = new Set(["delivery_type"]);
  let properties = Object.fromEntries(
    Object.entries(compiled.properties).filter(([name]) => !excluded.has(name)),
  );
  if (!partitionedSource && properties.metrics !== undefined) {
    properties = {
      ...properties,
      metrics: withoutObjectProperty(properties.metrics, "per_partition"),
    };
  }
  const node: CompiledNode = {
    ...compiled,
    properties,
    required: new Set(
      [...compiled.required].filter((name) => !excluded.has(name)),
    ),
  };
  return (
    <SchemaForm
      node={node}
      value={config}
      disabled={disabled}
      onChange={(value) => {
        if (isObject(value)) onChange({ ...config, ...value });
      }}
    />
  );
}

function withoutObjectProperty(
  node: CompiledNode,
  property: string,
): CompiledNode {
  if (node.kind === "nullable")
    return { ...node, inner: withoutObjectProperty(node.inner, property) };
  if (node.kind !== "object") return node;
  const properties = { ...node.properties };
  delete properties[property];
  return {
    ...node,
    properties,
    required: new Set([...node.required].filter((name) => name !== property)),
  };
}

function ContractView({ result }: { result: DiscoveryResult }) {
  return (
    <section class="card contract">
      <div class="card-heading">
        <div>
          <small>DISCOVERED CONTRACT</small>
          <h2>Data schema</h2>
        </div>
        <span>
          {result.source} → {result.sink}
        </span>
      </div>
      {result.datasets.map((dataset) => (
        <div class="dataset">
          <h3>
            {dataset.name} <small>{dataset.role}</small>
          </h3>
          <div class="columns">
            {dataset.columns.map((column) => (
              <div class="column">
                <strong>{column.name}</strong>
                <span>{column.arrow_type}</span>
                <span>{column.nullable ? "nullable" : "not null"}</span>
                {column.primary_key && <em>key</em>}
                {column.low_cardinality && <em>low cardinality</em>}
              </div>
            ))}
          </div>
        </div>
      ))}
      <details class="foldout sink-limits">
        <summary>Destination limits</summary>
        <pre>{JSON.stringify(result.sink_limits, null, 2)}</pre>
      </details>
    </section>
  );
}

function FieldLabel({
  label,
  required = false,
  children,
}: {
  label: string;
  required?: boolean;
  children: preact.ComponentChildren;
}) {
  return (
    <label class="top-field">
      <span>
        {label}
        {!required && <small class="optional">(optional)</small>}
      </span>
      {children}
    </label>
  );
}
function StatusPill({ runtime }: { runtime: string }) {
  return <span class={`status ${runtime}`}>{runtime}</span>;
}

function freshConfig(catalog: UiCatalog): JsonObject {
  const id = crypto.randomUUID();
  return {
    ...structuredClone(catalog.initial),
    delivery_id: `delivery-${id}`,
    durable_storage: {
      type: "local_file",
      path: `.transferia-server/workers/${id}/state`,
    },
    delivery_type: null,
    source: {},
    sink: {},
  };
}

function selectedEndpoints(catalog: UiCatalog, config: JsonObject) {
  const sourceKey = singleKey(config.source);
  const sinkKey = singleKey(config.sink);
  const source = catalog.providers.find(
    (provider) => provider.key === sourceKey,
  )?.source;
  const sink = catalog.providers.find(
    (provider) => provider.key === sinkKey,
  )?.sink;
  const deliveryType = stringValue(config.delivery_type);
  let error: string | undefined;
  if (deliveryType !== "" && source !== undefined) {
    const required =
      deliveryType === "batch_and_stream"
        ? ["batch", "stream"]
        : [deliveryType];
    const missing = required.filter(
      (mode) => !source.delivery_modes?.includes(mode as "batch" | "stream"),
    );
    if (missing.length > 0)
      error = `${catalog.providers.find((provider) => provider.key === sourceKey)?.title ?? sourceKey} does not support ${deliveryType.replaceAll("_", " ")} delivery.`;
  }
  return {
    sourceKey,
    sinkKey,
    source,
    sink,
    ...(error === undefined ? {} : { error }),
  };
}

function endpointValue(
  config: JsonObject,
  role: "source" | "sink",
  key: string,
): JsonValue {
  const container = config[role];
  return isObject(container) ? (container[key] ?? {}) : {};
}
function singleKey(value: JsonValue | undefined): string {
  return isObject(value) ? (Object.keys(value)[0] ?? "") : "";
}
function stringValue(value: JsonValue | undefined): string {
  return typeof value === "string" ? value : "";
}
function isObject(value: JsonValue | undefined): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}

const appRoot = document.getElementById("app");
if (appRoot !== null) render(<App />, appRoot);
