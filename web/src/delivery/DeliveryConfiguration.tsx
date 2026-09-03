import {
  compiledSchema,
  endpointValue,
  isObject,
  orderedEndpointConnectors,
  selectedEndpoints,
  stringValue,
} from "./editorConfig";
import { CommonSettings, EndpointCard } from "./EditorViews";
import {
  ParserDetailsForm,
  SerializerDetailsForm,
} from "../features/variantDetails/VariantDetailsForms";
import { useWidgetRegistry } from "../schema/widgetRegistry";
import type { WidgetContracts } from "../schema/widgetDefinitions";
import type { EditorState } from "../state";
import type {
  ConnectorDefinition,
  JsonObject,
  UiCatalog,
} from "../types";
import { AutofillResistantInput } from "../ui/AutofillResistantField";
import { TopField } from "../ui/FormField";
import { SelectControl } from "../ui/SelectControl";
import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import { SourceSampleProvider } from "../features/middleware/SourceSampleContext";
import {
  configuredEndpointCapabilities,
  configuredSourceSupportsDeliveryType,
  DELIVERY_TYPES,
  routeSupportsDeliveryType,
  sourceRecordSemantics,
  sourceSupportsDeliveryType,
  type DeliveryType,
} from "../recordSemantics";

type EndpointSelection = ReturnType<typeof selectedEndpoints>;

export function DeliveryConfiguration({
  catalog,
  editor,
  selection,
  readOnly,
  requiredErrorScope,
  onName,
  onDescription,
  onConfig,
  onChooseEndpoint,
}: {
  catalog: UiCatalog;
  editor: EditorState;
  selection: EndpointSelection | undefined;
  readOnly: boolean;
  requiredErrorScope: "none" | "source" | "endpoints" | "all";
  onName: (name: string) => void;
  onDescription: (description: string) => void;
  onConfig: (config: JsonObject) => void;
  onChooseEndpoint: (role: "source" | "sink", key: string) => void;
}) {
  const api = useControlPlane();
  const widgets = useWidgetRegistry();
  const deliveryTypeSelected = stringValue(editor.config.delivery_type) !== "";
  const routeSelectionComplete =
    deliveryTypeSelected &&
    (selection?.sourceKey ?? "") !== "" &&
    (selection?.sinkKey ?? "") !== "";
  const allSourceConnectors = orderedEndpointConnectors(catalog, "source");
  const allSinkConnectors = orderedEndpointConnectors(catalog, "sink");
  const currentDeliveryType = deliveryType(editor.config);
  const sourceConnectors = allSourceConnectors.filter((connector) =>
    connectorAllowed(
      connector,
      "source",
      currentDeliveryType,
      selection,
      editor.config,
      widgets,
    ),
  );
  const sinkConnectors = allSinkConnectors.filter((connector) =>
    connectorAllowed(
      connector,
      "sink",
      currentDeliveryType,
      selection,
      editor.config,
      widgets,
    ),
  );
  const deliveryTypeOptions = DELIVERY_TYPES.filter((candidate) =>
    deliveryTypeAllowed(candidate, selection, editor.config, widgets),
  );
  const requiredSinkRecordSemantics =
    selection?.error === undefined &&
    selection?.source !== undefined &&
    (editor.config.delivery_type === "batch" ||
      editor.config.delivery_type === "stream" ||
      editor.config.delivery_type === "batch_and_stream")
      ? sourceRecordSemantics(
          selection.source,
          compiledSchema(selection.source.schema, widgets),
          endpointValue(editor.config, "source", selection.sourceKey),
          editor.config.delivery_type,
        )
      : undefined;
  const sourceSampleLoader =
    selection?.error === undefined && selection?.source !== undefined
      ? async () => {
          const sourceConfig = endpointValue(
            editor.config,
            "source",
            selection.sourceKey,
          );
          const result = await api.previewMessage({
            connector: selection.sourceKey,
            config: isObject(sourceConfig) ? sourceConfig : {},
            max_bytes: 10 * 1024 * 1024,
          });
          const detection = result.detections.find(
            (candidate) => candidate.sample_rows.length > 0,
          );
          if (detection === undefined)
            throw new Error("No configured parser could produce sample rows");
          return detection.sample_rows;
        }
      : undefined;
  return (
    <SourceSampleProvider loader={sourceSampleLoader}>
      <div
        class="editor-view"
        role="tabpanel"
        key={`editor-${editor.sessionId}`}
      >
        <section class="card identity-card">
          <TopField
            label="Delivery name"
            required
            incomplete={!readOnly && editor.name.trim() === ""}
            invalid={requiredErrorScope === "all" && editor.name.trim() === ""}
          >
            <AutofillResistantInput
              type="text"
              value={editor.name}
              disabled={readOnly}
              placeholder="e.g. Events to ClickHouse"
              onInput={(event) => onName(event.currentTarget.value)}
            />
          </TopField>
          <TopField label="Description">
            <AutofillResistantInput
              type="text"
              value={editor.description}
              disabled={readOnly}
              onInput={(event) => onDescription(event.currentTarget.value)}
            />
          </TopField>
          <TopField
            label="Delivery type"
            required
            incomplete={!readOnly && !deliveryTypeSelected}
            invalid={
              requiredErrorScope === "all" &&
              stringValue(editor.config.delivery_type) === ""
            }
          >
            <SelectControl
              value={stringValue(editor.config.delivery_type)}
              disabled={readOnly}
              placeholder="Not selected"
              options={deliveryTypeOptions.map((value) => ({
                value,
                label: deliveryTypeLabel(value),
              }))}
              onChange={(value) =>
                onConfig({ ...editor.config, delivery_type: value })
              }
            />
          </TopField>
        </section>

        <section class="route-composition">
          <EndpointCard
            title="Source"
            role="source"
            selectedKey={selection?.sourceKey ?? ""}
            connectors={sourceConnectors}
            {...(selection?.source === undefined
              ? {}
              : { endpoint: selection.source })}
            config={editor.config}
            readOnly={readOnly}
            showSettings={routeSelectionComplete}
            showRequiredErrors={requiredErrorScope !== "none"}
            onChoose={onChooseEndpoint}
            onConfig={onConfig}
          />
          <div class="route-arrow">→</div>
          <EndpointCard
            title="Destination"
            role="sink"
            selectedKey={selection?.sinkKey ?? ""}
            connectors={sinkConnectors}
            {...(selection?.sink === undefined
              ? {}
              : { endpoint: selection.sink })}
            config={editor.config}
            readOnly={readOnly}
            showSettings={routeSelectionComplete}
            showRequiredErrors={
              requiredErrorScope === "endpoints" ||
              requiredErrorScope === "all"
            }
            {...(requiredSinkRecordSemantics === undefined
              ? {}
              : { requiredRecordSemantics: requiredSinkRecordSemantics })}
            onChoose={onChooseEndpoint}
            onConfig={onConfig}
          />
          {routeSelectionComplete &&
            selection?.error === undefined &&
            selection?.source && (
              <ParserDetailsForm
                node={compiledSchema(selection.source.schema, widgets)}
                value={endpointValue(
                  editor.config,
                  "source",
                  selection.sourceKey,
                )}
                disabled={readOnly}
                showRequiredErrors={requiredErrorScope !== "none"}
                onChange={(next) =>
                  onConfig({
                    ...editor.config,
                    source: { [selection.sourceKey]: next },
                  })
                }
              />
            )}
          {routeSelectionComplete &&
            selection?.error === undefined &&
            selection?.sink && (
              <SerializerDetailsForm
                node={compiledSchema(selection.sink.schema, widgets)}
                value={endpointValue(editor.config, "sink", selection.sinkKey)}
                disabled={readOnly}
                showRequiredErrors={
                  requiredErrorScope === "endpoints" ||
                  requiredErrorScope === "all"
                }
                onChange={(next) =>
                  onConfig({
                    ...editor.config,
                    sink: { [selection.sinkKey]: next },
                  })
                }
              />
            )}
        </section>
        {routeSelectionComplete && selection?.error && (
          <div class="compatibility-error">
            <strong>Incompatible route</strong>
            <span>{selection.error}</span>
          </div>
        )}

        {routeSelectionComplete && (
          <section class="pipeline-section">
            <h2>Pipeline settings</h2>
            <CommonSettings
              schema={catalog.common_schema}
              config={editor.config}
              disabled={readOnly}
              showRequiredErrors={requiredErrorScope === "all"}
              partitionedSource={selection?.source?.partitioned === true}
              onChange={onConfig}
            />
          </section>
        )}
      </div>
    </SourceSampleProvider>
  );
}

function deliveryType(config: JsonObject): DeliveryType | undefined {
  const value = stringValue(config.delivery_type);
  return DELIVERY_TYPES.includes(value as DeliveryType)
    ? (value as DeliveryType)
    : undefined;
}

function deliveryTypeLabel(value: DeliveryType): string {
  if (value === "batch") return "Batch";
  if (value === "stream") return "Stream";
  return "Batch + stream";
}

function connectorAllowed(
  candidate: ConnectorDefinition,
  role: "source" | "sink",
  selectedDeliveryType: DeliveryType | undefined,
  selection: EndpointSelection | undefined,
  config: JsonObject,
  widgets: WidgetContracts,
): boolean {
  const currentKey =
    role === "source" ? selection?.sourceKey : selection?.sinkKey;
  if (candidate.key === currentKey) return true;
  if (role === "source") {
    const source = candidate.source;
    if (source === undefined) return false;
    if (selection?.sink === undefined)
      return (
        selectedDeliveryType === undefined ||
        sourceSupportsDeliveryType(source, selectedDeliveryType)
      );
    const sink = configuredEndpointCapabilities(
      selection.sink,
      compiledSchema(selection.sink.schema, widgets),
      endpointValue(config, "sink", selection.sinkKey),
      "destination",
    );
    return candidateDeliveryTypes(selectedDeliveryType).some((mode) =>
      routeSupportsDeliveryType(source, sink, mode),
    );
  }

  const sink = candidate.sink;
  if (sink === undefined) return false;
  if (selection?.source === undefined) return true;
  const sourceSchema = compiledSchema(selection.source.schema, widgets);
  const sourceValue = endpointValue(config, "source", selection.sourceKey);
  const source = configuredEndpointCapabilities(
    selection.source,
    sourceSchema,
    sourceValue,
    "source",
  );
  return candidateDeliveryTypes(selectedDeliveryType).some((mode) =>
    routeSupportsDeliveryType(source, sink, mode, (phase) =>
      sourceRecordSemantics(
        selection.source!,
        sourceSchema,
        sourceValue,
        phase,
      ),
    ),
  );
}

function deliveryTypeAllowed(
  candidate: DeliveryType,
  selection: EndpointSelection | undefined,
  config: JsonObject,
  widgets: WidgetContracts,
): boolean {
  if (selection?.source === undefined) return true;
  const sourceSchema = compiledSchema(selection.source.schema, widgets);
  const sourceValue = endpointValue(config, "source", selection.sourceKey);
  if (selection.sink === undefined)
    return configuredSourceSupportsDeliveryType(
      selection.source,
      sourceSchema,
      sourceValue,
      candidate,
    );
  return routeSupportsDeliveryType(
    configuredEndpointCapabilities(
      selection.source,
      sourceSchema,
      sourceValue,
      "source",
    ),
    configuredEndpointCapabilities(
      selection.sink,
      compiledSchema(selection.sink.schema, widgets),
      endpointValue(config, "sink", selection.sinkKey),
      "destination",
    ),
    candidate,
    (phase) =>
      sourceRecordSemantics(
        selection.source!,
        sourceSchema,
        sourceValue,
        phase,
      ),
  );
}

function candidateDeliveryTypes(
  selected: DeliveryType | undefined,
): readonly DeliveryType[] {
  return selected === undefined ? DELIVERY_TYPES : [selected];
}
