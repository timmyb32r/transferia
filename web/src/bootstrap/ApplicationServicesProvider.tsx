import { createContext, type ComponentChildren } from "preact";
import { useContext } from "preact/hooks";

import type { ControlPlanePort } from "../application/ports/controlPlane";

export interface ApplicationServices {
  controlPlane: ControlPlanePort;
}

const ServicesContext = createContext<ApplicationServices | undefined>(
  undefined,
);

export function ApplicationServicesProvider({
  services,
  children,
}: {
  services: ApplicationServices;
  children: ComponentChildren;
}) {
  return (
    <ServicesContext.Provider value={services}>
      {children}
    </ServicesContext.Provider>
  );
}

export function useApplicationServices(): ApplicationServices {
  const services = useContext(ServicesContext);
  if (services === undefined) {
    throw new Error(
      "Application services are unavailable: mount this feature below ApplicationServicesProvider",
    );
  }
  return services;
}

export function useControlPlane(): ControlPlanePort {
  return useApplicationServices().controlPlane;
}
