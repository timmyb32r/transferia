import { render } from "preact";
import { useMemo } from "preact/hooks";

import type { ControlPlanePort } from "./application/ports/controlPlane";
import { ApplicationServicesProvider } from "./bootstrap/ApplicationServicesProvider";
import { DeliveryApplication } from "./delivery/DeliveryApplication";
import { productionWidgetRegistry } from "./features/formWidgetRegistry";
import {
  httpControlPlane,
  OPTIONS_TRANSPORT_VERSION,
} from "./infrastructure/controlPlane/httpControlPlane";
import { SCHEMA_DIALECT_VERSION } from "./schema/compiler";
import { FormEnvironmentProvider } from "./schema/formEnvironment";
import { WidgetRegistryProvider } from "./schema/widgetRegistry";

export function App({
  controlPlane = httpControlPlane,
}: {
  controlPlane?: ControlPlanePort;
}) {
  const services = useMemo(() => ({ controlPlane }), [controlPlane]);
  const formEnvironment = useMemo(
    () => ({ options: controlPlane.options.bind(controlPlane) }),
    [controlPlane],
  );
  return (
    <ApplicationServicesProvider services={services}>
      <FormEnvironmentProvider environment={formEnvironment}>
        <WidgetRegistryProvider registry={productionWidgetRegistry}>
          <DeliveryApplication />
        </WidgetRegistryProvider>
      </FormEnvironmentProvider>
    </ApplicationServicesProvider>
  );
}

const appRoot = document.getElementById("app");
document.documentElement.dataset.schemaDialect = SCHEMA_DIALECT_VERSION;
document.documentElement.dataset.optionsTransport = OPTIONS_TRANSPORT_VERSION;
if (appRoot !== null) render(<App />, appRoot);
