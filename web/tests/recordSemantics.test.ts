import { describe, expect, it } from "vitest";

import { compileSchema } from "../src/schema/compiler";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import {
  routeSupportsDeliveryType,
  sourceRecordSemantics,
} from "../src/recordSemantics";
import type { EndpointDefinition } from "../src/types";

const queueSource: EndpointDefinition = {
  schema: {
    type: "object",
    properties: {
      parser: {
        oneOf: [
          parser("json", "JSON", "append_only"),
          parser("debezium", "Debezium", "changelog"),
        ],
        "x-ui": { widget: "parser" },
      },
    },
    required: ["parser"],
  },
  initial: { parser: {} },
  delivery_modes: ["stream"],
  record_semantics: ["append_only", "changelog"],
  partitioned: false,
  connection_check: false,
  message_preview: false,
};

describe("record semantic selection", () => {
  it("uses the selected parser semantics instead of the endpoint union", () => {
    const schema = compileSchema(queueSource.schema, productionWidgetRegistry);
    expect(
      sourceRecordSemantics(
        queueSource,
        schema,
        { parser: { type: "json" } },
        "stream",
      ),
    ).toEqual(["append_only"]);
    expect(
      sourceRecordSemantics(
        queueSource,
        schema,
        { parser: { type: "debezium" } },
        "stream",
      ),
    ).toEqual(["changelog"]);
    expect(
      sourceRecordSemantics(queueSource, schema, { parser: {} }, "stream"),
    ).toEqual(["append_only", "changelog"]);
  });

  it("models batch, stream, and combined database deliveries", () => {
    const databaseSource: EndpointDefinition = {
      ...queueSource,
      schema: { type: "object", properties: {} },
      delivery_modes: ["batch", "stream"],
    };
    const schema = compileSchema(databaseSource.schema);
    expect(sourceRecordSemantics(databaseSource, schema, {}, "batch")).toEqual([
      "append_only",
    ]);
    expect(sourceRecordSemantics(databaseSource, schema, {}, "stream")).toEqual(
      ["changelog"],
    );
    expect(
      sourceRecordSemantics(databaseSource, schema, {}, "batch_and_stream"),
    ).toEqual(["append_only", "changelog"]);
  });

  it("requires a compatible sink semantic for every delivery phase", () => {
    const source: EndpointDefinition = {
      ...queueSource,
      schema: { type: "object", properties: {} },
      delivery_modes: ["batch", "stream"],
    };
    const appendSink: EndpointDefinition = {
      ...source,
      delivery_modes: [],
      record_semantics: ["append_only"],
    };
    const changelogSink: EndpointDefinition = {
      ...source,
      delivery_modes: [],
      record_semantics: ["changelog"],
    };
    const bothSink: EndpointDefinition = {
      ...source,
      delivery_modes: [],
      record_semantics: ["append_only", "changelog"],
    };

    expect(routeSupportsDeliveryType(source, appendSink, "batch")).toBe(true);
    expect(routeSupportsDeliveryType(source, changelogSink, "batch")).toBe(
      false,
    );
    expect(
      routeSupportsDeliveryType(source, appendSink, "batch_and_stream"),
    ).toBe(false);
    expect(
      routeSupportsDeliveryType(source, bothSink, "batch_and_stream"),
    ).toBe(true);
  });
});

function parser(type: string, title: string, semantics: string) {
  return {
    type: "object",
    title,
    properties: { type: { type: "string", const: type } },
    required: ["type"],
    "x-ui": {
      capabilities: {
        component: "parser",
        key: type,
        record_semantics: [semantics],
      },
    },
  };
}
