import { render } from "preact";
import { useEffect, useMemo, useReducer, useRef, useState } from "preact/hooks";

import { api } from "./api";
import { LatestJob } from "./effects";
import { SchemaForm, SelectControl } from "./schema/SchemaForm";
import {
  compileSchema,
  isComplete,
  type CompiledNode,
} from "./schema/compiler";
import { editorReducer, isDirty, isReadOnly, type EditorState } from "./state";
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

const EMPTY_STATE: EditorState = {
  editRevision: 0,
  name: "",
  config: {},
  validation: { state: "draft" },
  runtime: { state: "stopped" },
};

function App() {
  const [catalog, setCatalog] = useState<UiCatalog>();
  const [deliveries, setDeliveries] = useState<DeliverySummary[]>([]);
  const [editor, dispatch] = useReducer(editorReducer, EMPTY_STATE);
  const [yaml, setYaml] = useState("");
  const [yamlDraft, setYamlDraft] = useState("");
  const [activeView, setActiveView] = useState<"ui" | "yaml">("ui");
  const [discovery, setDiscovery] = useState<DiscoveryResult>();
  const [busy, setBusy] = useState<string>();
  const [error, setError] = useState<string>();
  const yamlJob = useRef(new LatestJob<JsonObject, { yaml: string }>()).current;
  const discoveryJob = useRef(
    new LatestJob<JsonObject, DiscoveryResult>(),
  ).current;
  const yamlEditing = useRef(false);

  useEffect(() => {
    void Promise.all([api.catalog(), api.deliveries()])
      .then(([nextCatalog, nextDeliveries]) => {
        setCatalog(nextCatalog);
        setDeliveries(nextDeliveries);
        dispatch({ type: "new", config: freshConfig(nextCatalog) });
      })
      .catch(reportError(setError));
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => {
      void api
        .deliveries()
        .then(setDeliveries)
        .catch(() => undefined);
      if (editor.id !== undefined && !isDirty(editor)) {
        void api
          .delivery(editor.id)
          .then((delivery) => dispatch({ type: "runtime", delivery }))
          .catch(() => undefined);
      }
    }, 2000);
    return () => window.clearInterval(timer);
  }, [editor.id, editor.editRevision, editor.savedEditRevision]);

  useEffect(() => {
    if (catalog === undefined) return;
    const timer = window.setTimeout(() => {
      void yamlJob
        .run(editor.editRevision, editor.config, (config, signal) =>
          api.yaml(config, signal),
        )
        .then((result) => {
          if (result !== undefined && result.revision === editor.editRevision)
            setYaml(result.value.yaml);
          if (
            result !== undefined &&
            result.revision === editor.editRevision &&
            !yamlEditing.current
          )
            setYamlDraft(result.value.yaml);
        })
        .catch(ignoreAbort(setError));
    }, 120);
    return () => window.clearTimeout(timer);
  }, [catalog, editor.config, editor.editRevision]);

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
      compileSchema(selection.source.schema),
      endpointValue(editor.config, "source", selection.sourceKey),
    ) &&
    isComplete(
      compileSchema(selection.sink.schema),
      endpointValue(editor.config, "sink", selection.sinkKey),
    );
  const hasJsonParser =
    selection?.sourceKey !== undefined &&
    sourceHasJsonParser(editor.config, selection.sourceKey);
  useEffect(() => {
    discoveryJob.cancel();
    setDiscovery(undefined);
    if (!structurallyComplete) return;
    const timer = window.setTimeout(() => {
      setBusy("Discovering topology and schema…");
      void discoveryJob
        .run(editor.editRevision, editor.config, (config, signal) =>
          api.discover(config, signal),
        )
        .then((result) => {
          if (result !== undefined && result.revision === editor.editRevision) {
            setDiscovery(result.value);
            setError(undefined);
          }
        })
        .catch(ignoreAbort(setError))
        .finally(() =>
          setBusy((current) =>
            current?.startsWith("Discovering") ? undefined : current,
          ),
        );
    }, 450);
    return () => window.clearTimeout(timer);
  }, [editor.config, editor.editRevision, structurallyComplete]);

  if (catalog === undefined)
    return (
      <main class="loading-screen">
        <span class="spinner" /> Loading control plane…
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
    dispatch({ type: "config", config: next });
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
  const refreshList = async () => setDeliveries(await api.deliveries());
  const runAction = async (
    label: string,
    action: () => Promise<DeliveryRecord>,
  ) => {
    setBusy(label);
    setError(undefined);
    try {
      const delivery = await action();
      dispatch({ type: "runtime", delivery });
      await refreshList();
      return delivery;
    } catch (reason) {
      setError(errorMessage(reason));
      return undefined;
    } finally {
      setBusy(undefined);
    }
  };
  const save = async (): Promise<DeliveryRecord | undefined> => {
    setBusy("Saving draft…");
    setError(undefined);
    try {
      const saved =
        editor.id === undefined
          ? await api.create(editor.name, editor.config)
          : await api.update(
              editor.id,
              editor.persistedRevision!,
              editor.name,
              editor.config,
            );
      dispatch({ type: "persisted", delivery: saved });
      await refreshList();
      return saved;
    } catch (reason) {
      setError(errorMessage(reason));
      return undefined;
    } finally {
      setBusy(undefined);
    }
  };
  const validate = async () => {
    const saved =
      isDirty(editor) || editor.id === undefined
        ? await save()
        : await api.delivery(editor.id);
    if (saved === undefined) return;
    setBusy("Validating current revision…");
    setError(undefined);
    try {
      const result = await api.validate(saved.id, saved.revision);
      setDiscovery(result);
      const updated = await api.delivery(saved.id);
      dispatch({ type: "runtime", delivery: updated });
      await refreshList();
    } catch (reason) {
      setError(errorMessage(reason));
      const updated = await api.delivery(saved.id).catch(() => undefined);
      if (updated !== undefined)
        dispatch({ type: "runtime", delivery: updated });
    } finally {
      setBusy(undefined);
    }
  };
  const showYaml = () => {
    if (activeView === "yaml") return;
    yamlEditing.current = true;
    setYamlDraft(yaml);
    setActiveView("yaml");
    setError(undefined);
  };
  const applyYamlAndShowUi = async () => {
    if (activeView === "ui") return;
    setBusy("Applying YAML…");
    setError(undefined);
    try {
      const parsed = await api.parseYaml(yamlDraft);
      dispatch({ type: "config", config: parsed.config });
      yamlEditing.current = false;
      setActiveView("ui");
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setBusy(undefined);
    }
  };

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
            dispatch({ type: "new", config: freshConfig(catalog) });
            yamlEditing.current = false;
            setActiveView("ui");
            setError(undefined);
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
                setBusy("Opening delivery…");
                void api
                  .delivery(delivery.id)
                  .then((record) => {
                    yamlEditing.current = false;
                    setActiveView("ui");
                    dispatch({ type: "open", delivery: record });
                  })
                  .catch(reportError(setError))
                  .finally(() => setBusy(undefined));
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
          {editor.id !== undefined && (
            <StatusPill runtime={editor.runtime.state} />
          )}
        </header>
        <div class="editor-tabs" role="tablist" aria-label="Configuration view">
          <button
            type="button"
            role="tab"
            aria-selected={activeView === "ui"}
            class={activeView === "ui" ? "active" : ""}
            disabled={busy !== undefined}
            onClick={() => void applyYamlAndShowUi()}
          >
            UI
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={activeView === "yaml"}
            class={activeView === "yaml" ? "active" : ""}
            disabled={busy !== undefined}
            onClick={showYaml}
          >
            YAML
          </button>
        </div>
        {error && (
          <div class="notice error">
            <span>{error}</span>
            <button type="button" onClick={() => setError(undefined)}>
              ×
            </button>
          </div>
        )}
        {busy && (
          <div class="notice progress">
            <span class="spinner" />
            {busy}
          </div>
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
                    dispatch({ type: "name", name: event.currentTarget.value })
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

            <section
              class={`route-grid ${hasJsonParser ? "parser-layout" : ""}`}
            >
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
            </section>
            {selection?.error && (
              <div class="compatibility-error">
                <strong>Incompatible route</strong>
                <span>{selection.error}</span>
              </div>
            )}

            <section class="card pipeline-card">
              <h2>Pipeline settings</h2>
              <CommonSettings
                schema={catalog.common_schema}
                config={editor.config}
                disabled={readOnly}
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

        <footer class="actions">
          {editor.runtime.state === "running" ? (
            <button
              class="danger-button"
              type="button"
              disabled={busy !== undefined}
              onClick={() =>
                void runAction("Stopping worker…", () =>
                  api.stop(editor.id!, editor.persistedRevision!),
                )
              }
            >
              Stop
            </button>
          ) : (
            <>
              <button
                type="button"
                disabled={busy !== undefined || !isDirty(editor)}
                onClick={() => void save()}
              >
                Save draft
              </button>
              <button
                type="button"
                disabled={busy !== undefined || editor.name.trim() === ""}
                onClick={() => void validate()}
              >
                Validate
              </button>
              <button
                class="primary"
                type="button"
                disabled={
                  busy !== undefined ||
                  editor.id === undefined ||
                  isDirty(editor) ||
                  editor.validation.state !== "ready" ||
                  editor.validation.revision !== editor.persistedRevision
                }
                onClick={() =>
                  void runAction("Starting worker…", () =>
                    api.activate(editor.id!, editor.persistedRevision!),
                  )
                }
              >
                Activate
              </button>
            </>
          )}
        </footer>
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
    <article class="card endpoint-card">
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
            node={compileSchema(props.endpoint.schema)}
            value={value}
            disabled={props.readOnly}
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
  onChange,
}: {
  schema: UiCatalog["common_schema"];
  config: JsonObject;
  disabled: boolean;
  onChange: (config: JsonObject) => void;
}) {
  const compiled = compileSchema(schema);
  if (compiled.kind !== "object") return null;
  const excluded = new Set(["delivery_type"]);
  const node: CompiledNode = {
    ...compiled,
    properties: Object.fromEntries(
      Object.entries(compiled.properties).filter(
        ([name]) => !excluded.has(name),
      ),
    ),
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
function sourceHasJsonParser(config: JsonObject, sourceKey: string): boolean {
  const source = endpointValue(config, "source", sourceKey);
  if (!isObject(source) || !isObject(source.parser)) return false;
  return "json_parser" in source.parser;
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
function reportError(setter: (message: string) => void) {
  return (reason: unknown) => setter(errorMessage(reason));
}
function ignoreAbort(setter: (message: string) => void) {
  return (reason: unknown) => {
    if (!(reason instanceof DOMException && reason.name === "AbortError"))
      setter(errorMessage(reason));
  };
}

render(<App />, document.getElementById("app")!);
