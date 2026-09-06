import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import { useEffect, useLayoutEffect, useMemo, useRef } from "preact/hooks";
import { createPortal } from "preact/compat";
import { visibleTableCatalog } from "../features/tableSelection/catalog";
import { TableNamingProvider } from "../features/tableSelection/naming";
import { SchemaForm } from "../schema/SchemaForm";
import { firstCompletionIssue } from "../schema/compiler";
import { revealDetails } from "../schema/revealDetails";
import { useWidgetRegistry } from "../schema/widgetRegistry";
import { Button } from "../ui/Button";
import { SelectControl } from "../ui/SelectControl";
import type {
  ConnectorDefinition,
  EndpointDefinition,
  JsonObject,
  JsonValue,
} from "../types";
import { compiledSchema, endpointValue, isObject } from "./editorConfig";
import { MessagePreviewDialog } from "./MessagePreviewDialog";
import { tableConnectionIdentity, useEndpointActions } from "./useEndpointActions";
import { ConnectionCheck, tableSettingsReady } from "./ConnectionCheck";
import { TableSelectionEditor } from "../features/tableSelection/TableSelectionEditor";
import { AutofillResistantInput } from "../ui/AutofillResistantField";
import type { VerifiedTableCatalog } from "../features/middleware/useTransformCatalog";
import { useSourceMetadataContext } from "./sourceMetadata";
import { beginMetadataReveal } from "./metadataReveal";

export function EndpointCard(props: {
  title: string;
  role: "source" | "sink";
  selectedKey: string;
  connectors: ConnectorDefinition[];
  endpoint?: EndpointDefinition;
  config: JsonObject;
  readOnly: boolean;
  showSettings?: boolean;
  showRequiredErrors: boolean;
  onChoose: (role: "source" | "sink", key: string) => void;
  onConfig: (config: JsonObject) => void;
  onTableConnection?: ((identity: string | undefined) => void) | undefined;
  onTableCatalog?: ((catalog: VerifiedTableCatalog | undefined) => void) | undefined;
  tablesHost?: HTMLElement | null | undefined;
}) {
  const showSettings = props.showSettings ?? true;
  const api = useControlPlane();
  const widgets = useWidgetRegistry();
  const value =
    props.endpoint === undefined
      ? {}
      : endpointValue(props.config, props.role, props.selectedKey);
  const node = props.endpoint ? compiledSchema(props.endpoint.schema, widgets) : undefined;
  const requiresTableCheck = props.role === "source" && props.endpoint?.connection_check === true
    && node?.kind === "object" && node.properties.tables?.xUi.widget === "table_selection";
  const localActions = useEndpointActions({
    api,
    connector: props.selectedKey,
    role: props.role,
    config: isObject(value) ? value : {},
  });
  const sharedMetadata = useSourceMetadataContext();
  const {
    check,
    preview,
    checkConnection,
    previewMessage,
    closePreview,
  } = requiresTableCheck && sharedMetadata ? sharedMetadata : localActions;
  const tableIdentity = isObject(value) ? tableConnectionIdentity(props.selectedKey, value) : undefined;
  const checkedTables = check.state === "success" ? check.tables : undefined;
  const hideSystemTables = isObject(value) && value.hide_system_tables !== false;
  const visibleTables = useMemo(() => checkedTables === undefined ? undefined
    : visibleTableCatalog(props.selectedKey, hideSystemTables, checkedTables),
  [props.selectedKey, hideSystemTables, checkedTables]);
  const tablesReady = tableSettingsReady(check);
  const currentTableState = useRef({ tableIdentity, tablesReady });
  currentTableState.current = { tableIdentity, tablesReady };
  const reveal = useRef<ReturnType<typeof beginMetadataReveal>>();
  useLayoutEffect(() => () => reveal.current?.cancel(), [tableIdentity]);
  const connectAndReveal = () => {
    if (check.state === "checking") return;
    reveal.current?.cancel();
    const intent = requiresTableCheck && props.tablesHost
      ? beginMetadataReveal(() => props.tablesHost ?? null, () => currentTableState.current.tableIdentity === tableIdentity && currentTableState.current.tablesReady)
      : undefined;
    reveal.current = intent;
    void checkConnection().then(() => intent?.complete());
  };
  const tableNode = node?.kind === "object" ? node.properties.tables : undefined;
  const tableIssue = tableNode ? firstCompletionIssue(tableNode, isObject(value) ? value.tables : undefined, true, "/tables") : undefined;
  const tablesIncomplete = tableIssue !== undefined && !tableIssue.hidden;
  const checkedTableIdentity = tablesReady ? tableIdentity : undefined;
  useEffect(() => {
    props.onTableConnection?.(checkedTableIdentity);
  }, [checkedTableIdentity, props.onTableConnection]);
  useEffect(() => {
    props.onTableCatalog?.(checkedTableIdentity !== undefined && checkedTables !== undefined
      ? { identity: checkedTableIdentity, tables: checkedTables } : undefined);
  }, [checkedTableIdentity, checkedTables, props.onTableCatalog]);
  const previewResult =
    props.selectedKey === "s3" && preview.result
      ? {
          ...preview.result,
          detections: preview.result.detections.filter(
            (detection) =>
              isObject(detection.config) &&
              isObject(detection.config["json_parser"]),
          ),
        }
      : preview.result;

  const applyDetection = (config: JsonObject) => {
    const parser =
      props.selectedKey === "s3" ? s3ParserFromDetection(config, value) : config;
    if (!parser) return;
    props.onConfig({
      ...props.config,
      [props.role]: {
        [props.selectedKey]: {
          ...(isObject(value) ? value : {}),
          parser,
        },
      },
    });
    closePreview();
    revealDetails(".parser-details-card");
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
          options={props.connectors.map((connector) => ({
            value: connector.key,
            label: connector.title,
          }))}
          onChange={(key) => props.onChoose(props.role, key)}
        />
      </div>
      {props.endpoint && node && showSettings && (
        <div class="endpoint-fields">
          <TableNamingProvider connector={props.selectedKey}>
          <SchemaForm
            node={node}
            value={value}
            disabled={props.readOnly}
            showRequiredErrors={props.showRequiredErrors}
            {...(typeof props.config.delivery_type === "string"
              ? { deliveryType: props.config.delivery_type }
              : {})}
            variantUi={{
              selectionOnly: props.role === "source" ? ["parser"] : [],
              actions:
                props.role === "source" && props.endpoint.message_preview
                  ? {
                      parser: (
                        <Button
                          class="parser-preview-button"
                          title="Preview one message"
                          aria-label="Preview one message"
                          disabled={props.readOnly || preview.loading}
                          onClick={() => void previewMessage()}
                        >
                          Scan
                        </Button>
                      ),
                    }
                  : {},
              onSelected: (widget) => {
                if (widget === "parser") revealDetails(".parser-details-card");
              },
            }}
            optionOverrides={check.options}
            tableCatalog={tablesReady && visibleTables !== undefined
              ? { tables: visibleTables, preview: api.previewTables,
                metadata: check.state === "success" ? check.metadata : undefined,
                metadataError: check.state === "success" ? check.metadataError : undefined } : undefined}
            connectionFields={requiresTableCheck ? {
              names: ["hide_system_tables", "tables", "new_tables"],
              label: "Table settings",
              disabled: !tablesReady,
              ...(props.tablesHost === undefined ? {} : { renderGroup: group => props.tablesHost ? createPortal(group, props.tablesHost) : null }),
              renderField: (name, field) => name === "hide_system_tables" ? null : name !== "tables" ? field
                : <div data-field-name="tables" key="tables"
                  class={[!props.readOnly && tablesReady && tablesIncomplete ? "required-incomplete" : "",
                    props.showRequiredErrors && tablesIncomplete ? "required-missing" : ""].filter(Boolean).join(" ")}>
                  <TableSelectionEditor value={isObject(value) ? value.tables ?? null : null}
                    disabled={props.readOnly || !tablesReady} fixed={node.properties.tables?.xUi.table_membership === "fixed"}
                    toolbar={node.properties.hide_system_tables && <span class="table-system-toggle">
                      <label><AutofillResistantInput type="checkbox" checked={hideSystemTables} disabled={props.readOnly || !tablesReady}
                        onChange={event => props.onConfig({ ...props.config, [props.role]: { [props.selectedKey]: {
                          ...(isObject(value) ? value : {}), hide_system_tables: event.currentTarget.checked,
                        } } })} />Hide system tables</label>
                      <span class="help" tabIndex={0} title="Hide system tables from the available catalog and table selection."
                        aria-label="About system table filtering">?</span>
                    </span>}
                    onChange={tables => props.onConfig({ ...props.config, [props.role]: { [props.selectedKey]: {
                      ...(isObject(value) ? value : {}), tables,
                    } } })} />
                </div>,
            } : undefined}
            connectionAction={
              props.endpoint.connection_check ? (
                <ConnectionCheck check={check} required={requiresTableCheck} onCheck={connectAndReveal} />
              ) : undefined
            }
            onChange={(next) =>
              props.onConfig({
                ...props.config,
                [props.role]: { [props.selectedKey]: next },
              })
            }
          />
          </TableNamingProvider>
        </div>
      )}
      {showSettings && preview.open && (
        <MessagePreviewDialog
          result={previewResult}
          error={preview.error}
          loading={preview.loading}
          allowApply={!props.readOnly}
          onClose={closePreview}
          onApply={(detection) => {
            if (isObject(detection.config)) applyDetection(detection.config);
          }}
        />
      )}
    </article>
  );
}

function s3ParserFromDetection(
  detection: JsonObject,
  sourceConfig: JsonValue | undefined,
): JsonObject | undefined {
  const jsonParser = detection["json_parser"];
  if (!isObject(jsonParser)) return undefined;
  const currentParser =
    isObject(sourceConfig) && isObject(sourceConfig["parser"])
      ? sourceConfig["parser"]
      : undefined;
  const currentCommon =
    isObject(currentParser) && isObject(currentParser["common"])
      ? currentParser["common"]
      : undefined;
  const detectedCommon = isObject(detection["common"])
    ? detection["common"]
    : undefined;
  const systemColumns =
    (isObject(currentCommon?.["system_columns"])
      ? currentCommon["system_columns"]
      : undefined) ??
    (isObject(detectedCommon?.["system_columns"])
      ? detectedCommon["system_columns"]
      : {});
  return {
    type: "json",
    common: { system_columns: systemColumns },
    json_parser: jsonParser,
  };
}
