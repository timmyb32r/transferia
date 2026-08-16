import { SchemaForm, SelectControl } from "../schema/SchemaForm";
import type { CompiledNode } from "../schema/compiler";
import { Disclosure } from "../ui/Disclosure";
import type {
  DiscoveryResult,
  EndpointDefinition,
  JsonObject,
  ProviderDefinition,
  UiCatalog,
} from "../types";
import { compiledSchema, endpointValue, isObject } from "./editorConfig";

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
