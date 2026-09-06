import { describe, expect, it } from "vitest";

import { speedtestAvailability } from "../src/delivery/speedtestAvailability";
import { NO_WIDGETS } from "../src/schema/widgetDefinitions";
import type { EndpointDefinition, JsonObject, UiCatalog } from "../src/types";

const endpoint = (title: string): EndpointDefinition => ({
  schema: {
    type: "object",
    properties: {
      database: { type: "string", title },
    },
    required: ["database"],
  },
  initial: {},
  delivery_modes: ["batch"],
  record_semantics: ["append_only"],
  partitioned: false,
  connection_check: false,
  message_preview: false, table_preview: false,
});

const CATALOG: UiCatalog = {
  common_schema: {
    type: "object",
    properties: {
      delivery_type: { type: "string" },
    },
    required: ["delivery_type"],
  },
  initial: {},
  connectors: [
    { key: "source-test", title: "Source", source: endpoint("Source DB") },
    {
      key: "sink/test",
      title: "Destination",
      sink: endpoint("Destination DB"),
    },
  ],
};

describe("speedtestAvailability", () => {
  it("requires a source before a destination", () => {
    expect(speedtestAvailability(CATALOG, {}, NO_WIDGETS)).toEqual({
      available: false,
      reason: "Choose a source first",
    });
  });

  it("names the exact missing source field and JSON pointer", () => {
    const config: JsonObject = {
      source: { "source-test": {} },
      sink: { "sink/test": { database: "ready" } },
    };

    expect(speedtestAvailability(CATALOG, config, NO_WIDGETS)).toEqual({
      available: false,
      reason:
        "Fill required source field: Source DB (#/source/source-test/database)",
    });
  });

  it("names the exact missing destination field and escapes its JSON pointer", () => {
    const config: JsonObject = {
      source: { "source-test": { database: "ready" } },
      sink: { "sink/test": {} },
    };

    expect(speedtestAvailability(CATALOG, config, NO_WIDGETS)).toEqual({
      available: false,
      reason:
        "Fill required destination field: Destination DB (#/sink/sink~1test/database)",
    });
  });

  it("ignores delivery name and common fields once both endpoints are complete", () => {
    const config: JsonObject = {
      source: { "source-test": { database: "ready" } },
      sink: { "sink/test": { database: "ready" } },
    };

    expect(speedtestAvailability(CATALOG, config, NO_WIDGETS)).toEqual({
      available: true,
    });
  });
});
