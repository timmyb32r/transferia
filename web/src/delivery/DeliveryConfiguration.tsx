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
import { MiddlewareEditor } from "../features/middleware/MiddlewareEditor";
import { TableNamingProvider } from "../features/tableSelection/naming";
import { useMemo, useState } from "preact/hooks";
import { useSourceMetadataContext } from "./sourceMetadata";
import { tableConnectionIdentity } from "./useEndpointActions";
import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import { TableCatalogContext } from "../schema/tableCatalog";
import { useTransformCatalog, type VerifiedTableCatalog } from "../features/middleware/useTransformCatalog";
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
  onTableConnection,
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
  onTableConnection?: ((identity: string | undefined) => void) | undefined;
}) {
  const widgets = useWidgetRegistry();
  const api = useControlPlane();
  const [checkedTables, setCheckedTables] = useState<VerifiedTableCatalog>();
  const [tablesHost, setTablesHost] = useState<HTMLElement | null>(null);
  const sharedMetadata = useSourceMetadataContext();
  const deliveryTypeSelected = stringValue(editor.config.delivery_type) !== "";
  const routeSelectionComplete =
    deliveryTypeSelected &&
    (selection?.sourceKey ?? "") !== "" &&
    (selection?.sinkKey ?? "") !== "";
  const allSourceConnectors = orderedEndpointConnectors(catalog, "source");
  const routeSettingsAvailable = routeSelectionComplete && selection?.routeError === undefined;
  const allSinkConnectors = orderedEndpointConnectors(catalog, "sink");
  const sourceConfig = selection ? endpointValue(editor.config, "source", selection.sourceKey) : undefined;
  const sourceNode = selection?.source ? compiledSchema(selection.source.schema, widgets) : undefined;
  const hasTableSettings = routeSettingsAvailable && selection?.source?.connection_check === true
    && sourceNode?.kind === "object" && sourceNode.properties.tables?.xUi.widget === "table_selection";
  const previewSource = selection?.source?.table_preview && isObject(sourceConfig)
    ? { connector: selection.sourceKey, config: sourceConfig } : undefined;
  const sharedCheck = sharedMetadata?.discovery;
  const sharedTables = sharedCheck?.state === "success" ? sharedCheck.tables : undefined;
  const identity = previewSource ? tableConnectionIdentity(previewSource.connector, previewSource.config) : undefined;
  const sharedCatalog = useMemo(() => identity && sharedTables ? { identity, tables: sharedTables } : undefined, [identity, sharedTables]);
  const transformCatalog = useTransformCatalog(previewSource, sharedMetadata ? sharedCatalog : checkedTables, api);
  return (
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
          {(routeSettingsAvailable || (!selection?.routeError && selection?.incompatibleConfiguration)) && selection?.error && (
            <div class="compatibility-error">
              <strong>{selection.incompatibleConfiguration ? "Incompatible configuration" : "Configuration required"}</strong>
              <span>{selection.error}</span>
            </div>
          )}
        </div>

        <section class="route-composition">
          <EndpointCard
            title="Source"
            onTableConnection={onTableConnection}
            onTableCatalog={setCheckedTables}
            tablesHost={hasTableSettings ? tablesHost : undefined}
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
          {hasTableSettings && <section class="card source-tables-card" ref={setTablesHost} tabIndex={-1} aria-label="Source tables" />}
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
        </section>
        {routeSettingsAvailable && <section class="middleware-island">
          <TableNamingProvider connector={selection?.sourceKey ?? ""}>
            <TableCatalogContext.Provider value={transformCatalog}>
            <MiddlewareEditor value={editor.config.middlewares ?? []} disabled={readOnly} source={previewSource}
              onChange={middlewares => onConfig({ ...editor.config, middlewares })} />
            </TableCatalogContext.Provider>
          </TableNamingProvider>
        </section>}
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
  );
}

function deliveryTypeLabel(value: DeliveryType): string {
  if (value === "batch") return "Batch";
  if (value === "stream") return "Stream";
  return "Batch + stream";
}
