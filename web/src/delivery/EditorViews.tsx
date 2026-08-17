import { useEffect, useRef, useState } from "preact/hooks";

import { api } from "../api";
import { SchemaForm, SelectControl } from "../schema/SchemaForm";
import type { CompiledNode } from "../schema/compiler";
import { Button } from "../ui/Button";
import { Disclosure } from "../ui/Disclosure";
import { EyeIcon } from "../ui/icons";
import type {
  DiscoveryResult,
  EndpointDefinition,
  JsonObject,
  ProviderDefinition,
  UiCatalog,
} from "../types";
import { compiledSchema, endpointValue, isObject } from "./editorConfig";
import { MessagePreviewDialog } from "./MessagePreviewDialog";

export function EndpointCard(props: {
  title: string;
  role: "source" | "sink";
  selectedKey: string;
  providers: ProviderDefinition[];
  endpoint?: EndpointDefinition;
  config: JsonObject;
  readOnly: boolean;
  showRequiredErrors: boolean;
  onChoose: (role: "source" | "sink", key: string) => void;
  onConfig: (config: JsonObject) => void;
}) {
  const value =
    props.endpoint === undefined
      ? {}
      : endpointValue(props.config, props.role, props.selectedKey);
  const [check, setCheck] = useState<
    | { state: "idle"; options: Record<string, string[]> }
    | { state: "checking"; options: Record<string, string[]> }
    | { state: "success"; options: Record<string, string[]> }
    | { state: "error"; message: string; options: Record<string, string[]> }
  >({ state: "idle", options: {} });
  const controller = useRef<AbortController>();
  const previewController = useRef<AbortController>();
  const [preview, setPreview] = useState<{
    open: boolean;
    loading: boolean;
    result?: import("../generated/apiContract").MessagePreviewResult;
    error?: string;
  }>({ open: false, loading: false });
  const configFingerprint = JSON.stringify(value);
  const endpointFingerprint = `${props.role}:${props.selectedKey}:${configFingerprint}`;
  const previousEndpointFingerprint = useRef(endpointFingerprint);
  useEffect(() => {
    if (previousEndpointFingerprint.current === endpointFingerprint) return;
    previousEndpointFingerprint.current = endpointFingerprint;
    controller.current?.abort();
    previewController.current?.abort();
    controller.current = undefined;
    previewController.current = undefined;
    setCheck({ state: "idle", options: {} });
    setPreview({ open: false, loading: false });
  }, [endpointFingerprint]);
  useEffect(
    () => () => {
      controller.current?.abort();
      previewController.current?.abort();
    },
    [],
  );

  const checkConnection = async () => {
    controller.current?.abort();
    const request = new AbortController();
    controller.current = request;
    setCheck((current) => ({ state: "checking", options: current.options }));
    try {
      const result = await api.checkConnection(
        {
          provider: props.selectedKey,
          role: props.role,
          config: isObject(value) ? value : {},
        },
        request.signal,
      );
      if (controller.current !== request) return;
      setCheck({ state: "success", options: result.options });
    } catch (error) {
      if (request.signal.aborted || controller.current !== request) return;
      setCheck({
        state: "error",
        message: error instanceof Error ? error.message : String(error),
        options: {},
      });
    } finally {
      if (controller.current === request) controller.current = undefined;
    }
  };
  const previewMessage = async () => {
    previewController.current?.abort();
    const request = new AbortController();
    previewController.current = request;
    setPreview({ open: true, loading: true });
    try {
      const result = await api.previewMessage(
        {
          provider: props.selectedKey,
          config: isObject(value) ? value : {},
          max_bytes: 16 * 1024 * 1024,
        },
        request.signal,
      );
      if (previewController.current === request) {
        setPreview({ open: true, loading: false, result });
      }
    } catch (error) {
      if (!request.signal.aborted && previewController.current === request) {
        setPreview({
          open: true,
          loading: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  };
  return (
    <article class={`card endpoint-card endpoint-card-${props.role}`}>
      <h2>{props.title}</h2>
      <div
        class={
          props.showRequiredErrors && props.selectedKey === ""
            ? "required-missing"
            : undefined
        }
      >
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
      </div>
      {props.endpoint && (
        <div class="endpoint-fields">
          <SchemaForm
            node={compiledSchema(props.endpoint.schema)}
            value={value}
            disabled={props.readOnly}
            showRequiredErrors={props.showRequiredErrors}
            parserSelectionOnly={props.role === "source"}
            optionOverrides={check.options}
            parserAction={
              props.role === "source" && props.selectedKey === "logbroker" ? (
                <Button
                  shape="icon"
                  class="parser-preview-button"
                  title="Preview one message"
                  aria-label="Preview one message"
                  disabled={preview.loading}
                  onClick={() => void previewMessage()}
                >
                  <EyeIcon />
                </Button>
              ) : undefined
            }
            connectionAction={
              props.endpoint.connection_check ? (
                <div class="connection-check">
                  <Button
                    class="connection-check-button"
                    disabled={check.state === "checking"}
                    onClick={() => void checkConnection()}
                  >
                    {check.state === "checking" && (
                      <span
                        class="connection-check-spinner"
                        aria-hidden="true"
                      />
                    )}
                    {check.state === "checking"
                      ? "Checking connection…"
                      : "Check connection"}
                  </Button>
                  <span
                    class={`connection-check-result connection-check-${check.state}`}
                    role={check.state === "error" ? "alert" : "status"}
                  >
                    {check.state === "success"
                      ? "Connection successful"
                      : check.state === "error"
                        ? check.message
                        : ""}
                  </span>
                </div>
              ) : undefined
            }
            onChange={(next) =>
              props.onConfig({
                ...props.config,
                [props.role]: { [props.selectedKey]: next },
              })
            }
          />
        </div>
      )}
      {preview.open && (
        <MessagePreviewDialog
          result={preview.result}
          error={preview.error}
          loading={preview.loading}
          allowApply={!props.readOnly}
          onClose={() => {
            previewController.current?.abort();
            setPreview({ open: false, loading: false });
          }}
          onApply={(detection) => {
            if (!isObject(detection.config)) return;
            props.onConfig({
              ...props.config,
              [props.role]: {
                [props.selectedKey]: {
                  ...(isObject(value) ? value : {}),
                  parser: detection.config,
                },
              },
            });
            setPreview({ open: false, loading: false });
          }}
        />
      )}
    </article>
  );
}

export function CommonSettings({
  schema,
  config,
  disabled,
  partitionedSource,
  showRequiredErrors,
  onChange,
}: {
  schema: UiCatalog["common_schema"];
  config: JsonObject;
  disabled: boolean;
  partitionedSource: boolean;
  showRequiredErrors: boolean;
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
      showRequiredErrors={showRequiredErrors}
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

export function ContractView({ result }: { result: DiscoveryResult }) {
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
      <Disclosure label="Destination limits" class="sink-limits">
        <pre>{JSON.stringify(result.sink_limits, null, 2)}</pre>
      </Disclosure>
    </section>
  );
}

export function StatusPill({ runtime }: { runtime: string }) {
  return <span class={`status ${runtime}`}>{runtime}</span>;
}
