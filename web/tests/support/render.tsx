import {
  render as testingLibraryRender,
  renderHook as testingLibraryRenderHook,
  type RenderHookResult,
  type RenderHookOptions,
  type RenderOptions,
  type RenderResult,
} from "@testing-library/preact";
import type { ComponentChild } from "preact";

import { httpControlPlane } from "../../src/infrastructure/controlPlane/httpControlPlane";
import { ApplicationServicesProvider } from "../../src/bootstrap/ApplicationServicesProvider";
import { productionWidgetRegistry } from "../../src/features/formWidgetRegistry";
import { WidgetRegistryProvider } from "../../src/schema/widgetRegistry";
import { FormEnvironmentProvider } from "../../src/schema/formEnvironment";

export function render(
  child: ComponentChild,
  options?: RenderOptions,
): RenderResult {
  return testingLibraryRender(child, { ...options, wrapper: ServicesWrapper });
}

function ServicesWrapper({ children }: { children: ComponentChild }) {
  return (
    <ApplicationServicesProvider services={{ controlPlane: httpControlPlane }}>
      <FormEnvironmentProvider
        environment={{
          options: httpControlPlane.options.bind(httpControlPlane),
        }}
      >
        <WidgetRegistryProvider registry={productionWidgetRegistry}>
          {children}
        </WidgetRegistryProvider>
      </FormEnvironmentProvider>
    </ApplicationServicesProvider>
  );
}

export function renderHook<Result, Props>(
  callback: (props: Props) => Result,
  options?: Omit<RenderHookOptions<Props>, "wrapper">,
): RenderHookResult<Result, Props> {
  return testingLibraryRenderHook(callback, {
    ...options,
    wrapper: ServicesWrapper,
  });
}
