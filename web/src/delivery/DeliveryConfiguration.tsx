import {
  compiledSchema,
  endpointValue,
  isObject,
  selectedEndpoints,
  stringValue,
} from "./editorConfig";
import { orderedEndpointConnectors } from "../connectorCatalog";
import { CommonSettings, EndpointCard } from "./EditorViews";
import {
  ParserDetailsForm,
  SerializerDetailsForm,
} from "../features/variantDetails/VariantDetailsForms";
import { useWidgetRegistry } from "../schema/widgetRegistry";
import type { EditorState } from "../state";
import type {
  JsonObject,
  UiCatalog,
} from "../types";
import { AutofillResistantInput } from "../ui/AutofillResistantField";
import { TopField } from "../ui/FormField";
import { SelectControl } from "../ui/SelectControl";
import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import { SourceSampleProvider } from "../features/middleware/SourceSampleContext";
import {
  DELIVERY_TYPES,
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
  const routeSettingsAvailable = routeSelectionComplete && selection?.routeError === undefined;
  const allSinkConnectors = orderedEndpointConnectors(catalog, "sink");
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
              options={DELIVERY_TYPES.map((value) => ({
                value,
                label: deliveryTypeLabel(value),
              }))}
              onChange={(value) =>
                onConfig({ ...editor.config, delivery_type: value })
              }
            />
          </TopField>
        </section>

        <div class="route-feedback" role="status" aria-live="polite">
          {routeSelectionComplete && selection?.routeError && (
            <div class="compatibility-error">
              <strong>Incompatible route</strong>
              <span>{selection.routeError}</span>
            </div>
          )}
          {routeSettingsAvailable && selection?.error && (
            <div class="compatibility-error">
              <strong>{selection.incompatibleConfiguration ? "Incompatible configuration" : "Configuration required"}</strong>
              <span>{selection.error}</span>
            </div>
          )}
        </div>

        <section class="route-composition">
          <EndpointCard
            title="Source"
            role="source"
            selectedKey={selection?.sourceKey ?? ""}
            connectors={allSourceConnectors}
            {...(selection?.source === undefined
              ? {}
              : { endpoint: selection.source })}
            config={editor.config}
            readOnly={readOnly}
            showSettings={routeSettingsAvailable}
            showRequiredErrors={requiredErrorScope !== "none"}
            onChoose={onChooseEndpoint}
            onConfig={onConfig}
          />
          <div class="route-arrow">→</div>
          <EndpointCard
            title="Destination"
            role="sink"
            selectedKey={selection?.sinkKey ?? ""}
            connectors={allSinkConnectors}
            {...(selection?.sink === undefined
              ? {}
              : { endpoint: selection.sink })}
            config={editor.config}
            readOnly={readOnly}
            showSettings={routeSettingsAvailable}
            showRequiredErrors={
              requiredErrorScope === "endpoints" ||
              requiredErrorScope === "all"
            }
            onChoose={onChooseEndpoint}
            onConfig={onConfig}
          />
          {routeSettingsAvailable &&
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
                fieldLabelOverrides={
                  selection.sourceKey === "logbroker"
                    ? { preserve_key: "Add sourceID" }
                    : {}
                }
                onChange={(next) =>
                  onConfig({
                    ...editor.config,
                    source: { [selection.sourceKey]: next },
                  })
                }
              />
            )}
          {routeSettingsAvailable &&
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
        {routeSettingsAvailable && (
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

function deliveryTypeLabel(value: DeliveryType): string {
  if (value === "batch") return "Batch";
  if (value === "stream") return "Stream";
  return "Batch + stream";
}
