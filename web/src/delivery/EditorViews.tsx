import { useEffect, useRef, useState } from "preact/hooks";

import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import { SchemaForm } from "../schema/SchemaForm";
import { useWidgetRegistry } from "../schema/widgetRegistry";
import type { CompiledNode } from "../schema/compiler";
import { Button } from "../ui/Button";
import { Disclosure } from "../ui/Disclosure";
import { SelectControl } from "../ui/SelectControl";
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
  const api = useControlPlane();
  const widgets = useWidgetRegistry();
  const value =
    props.endpoint === undefined
      ? {}
      : endpointValue(props.config, props.role, props.selectedKey);
  const [check, setCheck] = useState<
    | { state: "idle"; options: Record<string, string[]> }
    | { state: "checking"; options: Record<string, string[]> }
    | {
        state: "success";
        message?: string;
        status: import("../generated/apiContract").ConnectionCheckStatus;
        options: Record<string, string[]>;
      }
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
  const endpointIdentity = `${props.role}:${props.selectedKey}`;
  const configFingerprint = JSON.stringify(value);
  const previousEndpointIdentity = useRef(endpointIdentity);
  const previousConfigFingerprint = useRef(configFingerprint);
  useEffect(() => {
    if (previousEndpointIdentity.current === endpointIdentity) return;
    previousEndpointIdentity.current = endpointIdentity;
    previousConfigFingerprint.current = configFingerprint;
    controller.current?.abort();
    previewController.current?.abort();
    controller.current = undefined;
    previewController.current = undefined;
    setCheck({ state: "idle", options: {} });
    setPreview({ open: false, loading: false });
  }, [endpointIdentity]);
  useEffect(() => {
    if (previousConfigFingerprint.current === configFingerprint) return;
    previousConfigFingerprint.current = configFingerprint;
    controller.current?.abort();
    controller.current = undefined;
    setCheck((current) => ({ state: "idle", options: current.options }));
  }, [configFingerprint]);
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
      setCheck({
        state: "success",
        ...(result.message === null || result.message === undefined
          ? {}
          : { message: result.message }),
        status: result.status,
        options: result.options,
      });
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
          max_bytes: 32 * 1024 * 1024,
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
        class={[
          !props.readOnly && props.selectedKey === ""
            ? "required-incomplete"
            : "",
          props.showRequiredErrors && props.selectedKey === ""
            ? "required-missing"
            : "",
        ]
          .filter(Boolean)
          .join(" ")}
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
            node={compiledSchema(props.endpoint.schema, widgets)}
            value={value}
            disabled={props.readOnly}
            showRequiredErrors={props.showRequiredErrors}
            parserSelectionOnly={props.role === "source"}
            serializerSelectionOnly={props.role === "sink"}
            optionOverrides={check.options}
            parserAction={
              props.role === "source" && props.selectedKey === "logbroker" ? (
                <Button
                  class="parser-preview-button"
                  title="Preview one message"
                  aria-label="Preview one message"
                  disabled={props.readOnly || preview.loading}
                  onClick={() => void previewMessage()}
                >
                  Scan
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
                    class={`connection-check-result connection-check-${
                      check.state === "success" ? check.status : check.state
                    }`}
                    role={check.state === "error" ? "alert" : "status"}
                  >
                    {check.state === "success"
                      ? (check.message ??
                        "Connection verified, including access to the configured entities.")
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
            requestAnimationFrame(() =>
              requestAnimationFrame(() => {
                const parser = document.querySelector<HTMLElement>(
                  ".parser-details-card",
                );
                parser?.scrollIntoView({ behavior: "smooth", block: "start" });
                parser?.focus({ preventScroll: true });
              }),
            );
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
  const widgets = useWidgetRegistry();
  const compiled = compiledSchema(schema, widgets);
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
        <h2>Data schema</h2>
        <span>
          {result.source} → {result.sink}
          {result.pipeline_count > 1 && ` · ${result.pipeline_count} pipelines`}
        </span>
      </div>
      {result.datasets.map((dataset) => (
        <div class="dataset" key={`${dataset.role}:${dataset.name}`}>
          <h3>
            {dataset.name} <small>{dataset.role}</small>
          </h3>
          <div class="schema-flow">
            <SchemaStage
              title="Intermediate"
              subtitle="Arrow data after parsing and transforms"
              columns={dataset.intermediate_columns.map((column) => ({
                ...column,
                displayedType: column.arrow_type,
              }))}
            />
            <div class="schema-flow-arrow" aria-hidden="true">
              →
            </div>
            <SchemaStage
              title={`Final · ${result.sink}`}
              subtitle="Physical representation written by the destination"
              columns={dataset.final_columns.map((column) => ({
                ...column,
                displayedType: column.destination_type,
                secondaryType: column.arrow_type,
              }))}
            />
          </div>
        </div>
      ))}
      <Disclosure label="Destination limits" class="sink-limits">
        <pre>{JSON.stringify(result.sink_limits, null, 2)}</pre>
      </Disclosure>
    </section>
  );
}

export function DataSchemaWorkspace({ result }: { result: DiscoveryResult }) {
  return (
    <section class="data-schema-workspace" role="tabpanel">
      <header class="data-schema-toolbar">
        <div>
          <h2>Data schema</h2>
          <p>
            Final discovered table schemas update with the delivery
            configuration.
          </p>
        </div>
      </header>
      <ContractView result={result} />
    </section>
  );
}

export function DataSchemaInspector({
  result,
  loading,
  onHide,
}: {
  result: DiscoveryResult;
  loading?: boolean;
  onHide: () => void;
}) {
  const [selectedTable, setSelectedTable] = useState("");
  const [collapsed, setCollapsed] = useState(false);
  const [changedColumns, setChangedColumns] = useState<Set<string>>(
    () => new Set(),
  );
  const [position, setPosition] = useState(() => ({
    x: Math.max(0, window.innerWidth - 404),
    y: 24,
  }));
  const drag = useRef<{ pointer: number; dx: number; dy: number }>();
  const previousColumns = useRef(columnFingerprints(result));
  const highlightTimer = useRef<number>();
  const datasets = result.datasets;
  const selected =
    datasets.find((dataset) => dataset.name === selectedTable) ?? datasets[0];

  useEffect(() => {
    if (selected !== undefined && selected.name !== selectedTable)
      setSelectedTable(selected.name);
  }, [selected?.name, selectedTable]);
  useEffect(() => {
    const next = columnFingerprints(result);
    const changed = new Set(
      [...next].flatMap(([key, fingerprint]) =>
        previousColumns.current.get(key) === fingerprint ? [] : [key],
      ),
    );
    previousColumns.current = next;
    window.clearTimeout(highlightTimer.current);
    setChangedColumns(changed);
    if (changed.size > 0) {
      highlightTimer.current = window.setTimeout(
        () => setChangedColumns(new Set()),
        1000,
      );
    }
    return () => window.clearTimeout(highlightTimer.current);
  }, [result]);

  return (
    <aside
      class={`schema-inspector ${collapsed ? "collapsed" : ""}`}
      aria-label="Schema inspector"
      style={{ left: `${position.x}px`, top: `${position.y}px` }}
    >
      <header
        class="schema-inspector-drag-handle"
        onPointerDown={(event) => {
          if ((event.target as HTMLElement).closest("button")) return;
          drag.current = {
            pointer: event.pointerId,
            dx: event.clientX - position.x,
            dy: event.clientY - position.y,
          };
          event.currentTarget.setPointerCapture(event.pointerId);
        }}
        onPointerMove={(event) => {
          if (drag.current?.pointer !== event.pointerId) return;
          setPosition({
            x: Math.max(0, event.clientX - drag.current.dx),
            y: Math.max(0, event.clientY - drag.current.dy),
          });
        }}
        onPointerUp={(event) => {
          if (drag.current?.pointer === event.pointerId)
            drag.current = undefined;
        }}
      >
        <strong>Final schema</strong>
        <span>Drag to move</span>
        {loading && (
          <span
            class="schema-inspector-progress spinner"
            role="status"
            aria-label="Updating schema"
          />
        )}
        <Button
          shape="icon"
          aria-label={
            collapsed ? "Expand schema inspector" : "Collapse schema inspector"
          }
          title={collapsed ? "Expand" : "Collapse"}
          onClick={() => setCollapsed((value) => !value)}
        >
          {collapsed ? "□" : "—"}
        </Button>
        <Button
          shape="icon"
          aria-label="Hide schema inspector"
          onClick={onHide}
        >
          ×
        </Button>
      </header>
      {!collapsed &&
        (datasets.length === 0 ? (
          <p>No tables discovered.</p>
        ) : (
          <>
            <SelectControl
              searchable
              value={selected?.name ?? ""}
              placeholder="Select table"
              options={datasets.map((dataset) => ({
                value: dataset.name,
                label: dataset.name,
              }))}
              onChange={setSelectedTable}
            />
            <div
              class="schema-inspector-table"
              role="table"
              aria-label="Selected table schema"
            >
              <div
                class="schema-inspector-row schema-inspector-head"
                role="row"
              >
                <span>Column</span>
                <span>Arrow type</span>
                <span>PK</span>
                <span>Not null</span>
              </div>
              {selected?.final_columns.map((column) => {
                const key = columnKey(selected, column.name);
                return (
                  <div
                    class={`schema-inspector-row ${changedColumns.has(key) ? "schema-row-updated" : ""}`}
                    role="row"
                    key={column.name}
                  >
                    <strong>{column.name}</strong>
                    <code>{column.arrow_type}</code>
                    <span>{column.primary_key ? "Yes" : "—"}</span>
                    <span>{column.nullable ? "—" : "Yes"}</span>
                  </div>
                );
              })}
            </div>
          </>
        ))}
    </aside>
  );
}

function columnKey(
  dataset: DiscoveryResult["datasets"][number],
  column: string,
): string {
  return `${dataset.role}:${dataset.name}:${column}`;
}

function columnFingerprints(result: DiscoveryResult): Map<string, string> {
  return new Map(
    result.datasets.flatMap((dataset) =>
      dataset.final_columns.map((column) => [
        columnKey(dataset, column.name),
        JSON.stringify(column),
      ]),
    ),
  );
}

function SchemaStage({
  title,
  subtitle,
  columns,
}: {
  title: string;
  subtitle: string;
  columns: Array<
    DiscoveryResult["datasets"][number]["intermediate_columns"][number] & {
      displayedType: string;
      secondaryType?: string;
    }
  >;
}) {
  return (
    <section class="schema-stage">
      <header>
        <strong>{title}</strong>
        <span>{subtitle}</span>
      </header>
      <div
        class="schema-stage-table"
        role="table"
        aria-label={`${title} schema`}
      >
        <div class="schema-stage-row schema-stage-head" role="row">
          <span role="columnheader">Column</span>
          <span role="columnheader">Type</span>
          <span role="columnheader">Constraints</span>
        </div>
        {columns.map((column) => (
          <div class="schema-stage-row" role="row" key={column.name}>
            <strong role="cell">{column.name}</strong>
            <span role="cell" class="schema-stage-type">
              <code>{column.displayedType}</code>
              {column.secondaryType && (
                <small>from {column.secondaryType}</small>
              )}
            </span>
            <span role="cell" class="schema-stage-constraints">
              <span>{column.nullable ? "nullable" : "not null"}</span>
              {column.primary_key && <em>key</em>}
              {column.low_cardinality && <em>low cardinality</em>}
            </span>
          </div>
        ))}
      </div>
    </section>
  );
}

export function StatusPill({ runtime }: { runtime: string }) {
  return <span class={`status ${runtime}`}>{runtime}</span>;
}
