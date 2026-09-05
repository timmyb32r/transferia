import type { ConnectorDefinition, UiCatalog } from "./generated/apiContract";

const SPECIAL_CONNECTOR_ORDER = new Map([
  ["data_generator", 0],
  ["discard", 1],
]);

export function orderedEndpointConnectors(
  catalog: UiCatalog,
  role: "source" | "sink",
): ConnectorDefinition[] {
  return catalog.connectors
    .filter((connector) => connector[role] !== undefined)
    .sort((left, right) => {
      const leftSpecial = SPECIAL_CONNECTOR_ORDER.get(left.key);
      const rightSpecial = SPECIAL_CONNECTOR_ORDER.get(right.key);
      if (leftSpecial !== undefined || rightSpecial !== undefined) {
        if (leftSpecial === undefined) return -1;
        if (rightSpecial === undefined) return 1;
        return leftSpecial - rightSpecial;
      }
      return (
        left.title.localeCompare(right.title, "en", { sensitivity: "base" }) ||
        left.key.localeCompare(right.key)
      );
    });
}
