import type { WidgetContracts } from "../schema/widgetDefinitions";
import type { JsonObject, UiCatalog } from "../types";
import {
  compiledSchema,
  completionIssueLabel,
  configurationReadiness,
  endpointValue,
} from "./editorConfig";

export interface SpeedtestAvailability {
  available: boolean;
  reason?: string;
}

export function speedtestAvailability(
  catalog: UiCatalog,
  config: JsonObject,
  widgets: WidgetContracts,
): SpeedtestAvailability {
  const readiness = configurationReadiness(catalog, config, widgets);
  const { selection } = readiness;
  if (selection.source === undefined)
    return { available: false, reason: "Choose a source first" };
  if (readiness.sourceIssue !== undefined) {
    const path = `#/source/${escapePointer(selection.sourceKey)}`;
    const label = completionIssueLabel(
      compiledSchema(selection.source.schema, widgets),
      endpointValue(config, "source", selection.sourceKey),
      readiness.sourceIssue,
      path,
    );
    return {
      available: false,
      reason: `Fill required source field: ${label} (${readiness.sourceIssue.path})`,
    };
  }
  if (selection.sink === undefined)
    return { available: false, reason: "Choose a destination first" };
  if (readiness.sinkIssue !== undefined) {
    const path = `#/sink/${escapePointer(selection.sinkKey)}`;
    const label = completionIssueLabel(
      compiledSchema(selection.sink.schema, widgets),
      endpointValue(config, "sink", selection.sinkKey),
      readiness.sinkIssue,
      path,
    );
    return {
      available: false,
      reason: `Fill required destination field: ${label} (${readiness.sinkIssue.path})`,
    };
  }
  return { available: true };
}

function escapePointer(value: string): string {
  return value.replaceAll("~", "~0").replaceAll("/", "~1");
}
