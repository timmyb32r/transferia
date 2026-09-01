import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import { SchemaForm } from "../schema/SchemaForm";
import { revealDetails } from "../schema/revealDetails";
import { useWidgetRegistry } from "../schema/widgetRegistry";
import { Button } from "../ui/Button";
import { SelectControl } from "../ui/SelectControl";
import type {
  ConnectorDefinition,
  EndpointDefinition,
  JsonObject,
} from "../types";
import { compiledSchema, endpointValue, isObject } from "./editorConfig";
import { MessagePreviewDialog } from "./MessagePreviewDialog";
import { useEndpointActions } from "./useEndpointActions";

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
}) {
  const showSettings = props.showSettings ?? true;
  const api = useControlPlane();
  const widgets = useWidgetRegistry();
  const value =
    props.endpoint === undefined
      ? {}
      : endpointValue(props.config, props.role, props.selectedKey);
  const {
    check,
    preview,
    checkConnection,
    previewMessage,
    closePreview,
  } = useEndpointActions({
    api,
    connector: props.selectedKey,
    role: props.role,
    config: isObject(value) ? value : {},
  });

  const applyDetection = (config: JsonObject) => {
    props.onConfig({
      ...props.config,
      [props.role]: {
        [props.selectedKey]: {
          ...(isObject(value) ? value : {}),
          parser: config,
        },
      },
    });
    closePreview();
    requestAnimationFrame(() =>
      requestAnimationFrame(() => {
        const parser = document.querySelector<HTMLElement>(
          ".parser-details-card",
        );
        parser?.scrollIntoView({ behavior: "smooth", block: "start" });
        parser?.focus({ preventScroll: true });
      }),
    );
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
      {props.endpoint && showSettings && (
        <div class="endpoint-fields">
          <SchemaForm
            node={compiledSchema(props.endpoint.schema, widgets)}
            value={value}
            disabled={props.readOnly}
            showRequiredErrors={props.showRequiredErrors}
            {...(typeof props.config.delivery_type === "string"
              ? { deliveryType: props.config.delivery_type }
              : {})}
            variantUi={{
              selectionOnly: [
                props.role === "source" ? "parser" : "serializer",
              ],
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
              onSelected: (widget) =>
                revealDetails(
                  widget === "parser"
                    ? ".parser-details-card"
                    : ".serializer-details-card",
                ),
            }}
            optionOverrides={check.options}
            connectionAction={
              props.endpoint.connection_check ? (
                <div class="connection-check">
                  <Button
                    variant="primary"
                    class="connection-check-button"
                    aria-disabled={check.state === "checking"}
                    onClick={() => void checkConnection()}
                  >
                    Check connection
                  </Button>
                  <span
                    class="connection-check-spinner-slot"
                    aria-label={
                      check.state === "checking"
                        ? "Checking connection…"
                        : undefined
                    }
                    role={check.state === "checking" ? "status" : undefined}
                  >
                    {check.state === "checking" && (
                      <span
                        class="connection-check-spinner"
                        aria-hidden="true"
                      />
                    )}
                  </span>
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
      {showSettings && preview.open && (
        <MessagePreviewDialog
          result={preview.result}
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
