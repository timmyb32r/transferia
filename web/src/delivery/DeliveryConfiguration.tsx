import {
  compiledSchema,
  endpointValue,
  isObject,
  selectedEndpoints,
  stringValue,
} from "./editorConfig";
import { CommonSettings, EndpointCard } from "./EditorViews";
import { ParserDetailsForm, SerializerDetailsForm } from "../schema/SchemaForm";
import type { EditorState } from "../state";
import type { JsonObject, UiCatalog } from "../types";
import { TopField } from "../ui/FormField";
import { SelectControl } from "../ui/SelectControl";
import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import { SourceSampleProvider } from "../features/middleware/SourceSampleContext";

type EndpointSelection = ReturnType<typeof selectedEndpoints>;

export function DeliveryConfiguration({
  catalog,
  editor,
  selection,
  readOnly,
  showRequiredErrors,
  onName,
  onDescription,
  onConfig,
  onChooseEndpoint,
}: {
  catalog: UiCatalog;
  editor: EditorState;
  selection: EndpointSelection | undefined;
  readOnly: boolean;
  showRequiredErrors: boolean;
  onName: (name: string) => void;
  onDescription: (description: string) => void;
  onConfig: (config: JsonObject) => void;
  onChooseEndpoint: (role: "source" | "sink", key: string) => void;
}) {
  const api = useControlPlane();
  const deliveryTypeSelected = stringValue(editor.config.delivery_type) !== "";
  const sourceProviders = catalog.providers.filter(
    (provider) => provider.source !== undefined,
  );
  const sinkProviders = catalog.providers.filter(
    (provider) => provider.sink !== undefined,
  );
  const sourceSampleLoader =
    selection?.error === undefined && selection?.source !== undefined
      ? async () => {
          const sourceConfig = endpointValue(
            editor.config,
            "source",
            selection.sourceKey,
          );
          const result = await api.previewMessage({
            provider: selection.sourceKey,
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
            invalid={showRequiredErrors && editor.name.trim() === ""}
          >
            <input
              type="text"
              name="transferia-delivery-name"
              autoComplete="off"
              value={editor.name}
              disabled={readOnly}
              placeholder="e.g. Events to ClickHouse"
              onInput={(event) => onName(event.currentTarget.value)}
            />
          </TopField>
          <TopField label="Description">
            <input
              type="text"
              name="transferia-delivery-description"
              autoComplete="off"
              value={editor.description}
              disabled={readOnly}
              onInput={(event) => onDescription(event.currentTarget.value)}
            />
          </TopField>
          <TopField
            label="Delivery type"
            required
            invalid={
              showRequiredErrors &&
              stringValue(editor.config.delivery_type) === ""
            }
          >
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
                onConfig({ ...editor.config, delivery_type: value })
              }
            />
          </TopField>
        </section>

        {deliveryTypeSelected && (
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
              showRequiredErrors={showRequiredErrors}
              onChoose={onChooseEndpoint}
              onConfig={onConfig}
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
              showRequiredErrors={showRequiredErrors}
              onChoose={onChooseEndpoint}
              onConfig={onConfig}
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
                showRequiredErrors={showRequiredErrors}
                onChange={(next) =>
                  onConfig({
                    ...editor.config,
                    source: { [selection.sourceKey]: next },
                  })
                }
              />
            )}
            {selection?.error === undefined && selection?.sink && (
              <SerializerDetailsForm
                node={compiledSchema(selection.sink.schema)}
                value={endpointValue(editor.config, "sink", selection.sinkKey)}
                disabled={readOnly}
                showRequiredErrors={showRequiredErrors}
                onChange={(next) =>
                  onConfig({
                    ...editor.config,
                    sink: { [selection.sinkKey]: next },
                  })
                }
              />
            )}
          </section>
        )}
        {deliveryTypeSelected && selection?.error && (
          <div class="compatibility-error">
            <strong>Incompatible route</strong>
            <span>{selection.error}</span>
          </div>
        )}

        {deliveryTypeSelected && (
          <section class="pipeline-section">
            <h2>Pipeline settings</h2>
            <CommonSettings
              schema={catalog.common_schema}
              config={editor.config}
              disabled={readOnly}
              showRequiredErrors={showRequiredErrors}
              partitionedSource={selection?.source?.partitioned === true}
              onChange={onConfig}
            />
          </section>
        )}
      </div>
    </SourceSampleProvider>
  );
}
