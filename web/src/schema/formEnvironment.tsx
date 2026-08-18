import { createContext, type ComponentChildren } from "preact";
import { useContext } from "preact/hooks";

import type { ControlPlanePort } from "../application/ports/controlPlane";

export interface FormEnvironment {
  options: ControlPlanePort["options"];
}

const FormEnvironmentContext = createContext<FormEnvironment | undefined>(
  undefined,
);

export function FormEnvironmentProvider({
  environment,
  children,
}: {
  environment: FormEnvironment;
  children: ComponentChildren;
}) {
  return (
    <FormEnvironmentContext.Provider value={environment}>
      {children}
    </FormEnvironmentContext.Provider>
  );
}

export function useFormEnvironment(): FormEnvironment {
  const environment = useContext(FormEnvironmentContext);
  if (environment === undefined)
    throw new Error(
      "Form environment is unavailable: inject form capabilities at the composition root",
    );
  return environment;
}
