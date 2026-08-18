import { createContext, type ComponentChildren } from "preact";
import { useContext } from "preact/hooks";

import type { JsonValue } from "../../types";

export type SourceSampleLoader = () => Promise<JsonValue[]>;

const SourceSampleContext = createContext<SourceSampleLoader | undefined>(
  undefined,
);

export function SourceSampleProvider({
  loader,
  children,
}: {
  loader: SourceSampleLoader | undefined;
  children: ComponentChildren;
}) {
  return (
    <SourceSampleContext.Provider value={loader}>
      {children}
    </SourceSampleContext.Provider>
  );
}

export function useSourceSample(): SourceSampleLoader | undefined {
  return useContext(SourceSampleContext);
}
