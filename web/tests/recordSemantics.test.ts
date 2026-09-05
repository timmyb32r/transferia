import { describe, expect, it } from "vitest";

import { compileSchema } from "../src/schema/compiler";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import {
  acceptsConfiguredRecordSemantics,
  configuredEndpointCapabilities,
  configuredSourceSupportsDeliveryType,
  routeSupportsDeliveryType,
  sourceRecordSemantics,
  sourceSupportsDeliveryType,
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
  it("requires every produced semantic to survive both destination and serializer", () => {
    const sink = { delivery_modes: [], record_semantics: ["append_only", "changelog"] } as const;
    const schema = compileSchema({ type: "object", properties: {
      encoder: { type: "object", properties: {}, "x-ui": { capabilities: {
        component: "serializer", key: "arbitrary-encoder", record_semantics: ["append_only"],
      } } },
    } }, productionWidgetRegistry);
    expect(acceptsConfiguredRecordSemantics(["append_only"], sink, schema, { encoder: {} })).toBe(true);
    expect(acceptsConfiguredRecordSemantics(["changelog"], sink, schema, { encoder: {} })).toBe(false);
    expect(acceptsConfiguredRecordSemantics(["append_only", "changelog"], sink, schema, { encoder: {} })).toBe(false);
  });

  it("does not turn parsed change events into append-only records merely because input is finite", () => {
    const schema = compileSchema(queueSource.schema, productionWidgetRegistry);
    expect(sourceRecordSemantics(queueSource, schema, { parser: { type: "debezium" } }, "batch"))
      .toEqual(["changelog"]);
  });
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
    ).toBeUndefined();
  });

  it("models batch, stream, and combined database deliveries", () => {
    const databaseSource: EndpointDefinition = {
      ...queueSource,
      schema: { type: "object", properties: {} },
      delivery_modes: ["batch", "stream", "batch_and_stream"],
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

  it("requires the explicit combined source capability", () => {
    const legacySeparateModes: EndpointDefinition = {
      ...queueSource,
      delivery_modes: ["batch", "stream"],
    };
    const combinedOnly: EndpointDefinition = {
      ...queueSource,
      delivery_modes: ["batch_and_stream"],
    };

    expect(
      sourceSupportsDeliveryType(legacySeparateModes, "batch_and_stream"),
    ).toBe(false);
    expect(sourceSupportsDeliveryType(combinedOnly, "batch_and_stream")).toBe(
      true,
    );
  });

  it("requires a compatible sink semantic for every delivery phase", () => {
    const source: EndpointDefinition = {
      ...queueSource,
      schema: { type: "object", properties: {} },
      delivery_modes: ["batch", "stream", "batch_and_stream"],
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
      routeSupportsDeliveryType(source, changelogSink, "batch_and_stream"),
    ).toBe(false);
    expect(
      routeSupportsDeliveryType(source, bothSink, "batch_and_stream"),
    ).toBe(true);
  });

  it("uses the deepest active endpoint capability override", () => {
    const source: EndpointDefinition = {
      ...queueSource,
      schema: conditionalSourceSchema(),
      initial: { replication: null },
      delivery_modes: ["batch", "stream", "batch_and_stream"],
    };
    const schema = compileSchema(source.schema);

    expect(
      configuredEndpointCapabilities(source, schema, {}, "source"),
    ).toEqual({
      delivery_modes: ["batch"],
      record_semantics: ["append_only"],
    });
    expect(
      configuredEndpointCapabilities(
        source,
        schema,
        { replication: null },
        "source",
      ),
    ).toEqual({
      delivery_modes: ["batch"],
      record_semantics: ["append_only"],
    });
    expect(
      configuredEndpointCapabilities(
        source,
        schema,
        { replication: {} },
        "source",
      ),
    ).toEqual({
      delivery_modes: ["stream", "batch_and_stream"],
      record_semantics: ["changelog"],
    });
    expect(
      configuredSourceSupportsDeliveryType(
        source,
        schema,
        { replication: {} },
        "batch",
      ),
    ).toBe(false);
    expect(
      sourceRecordSemantics(
        source,
        schema,
        { replication: {} },
        "batch_and_stream",
      ),
    ).toEqual(["append_only", "changelog"]);
  });

  it("fails closed for conflicting active endpoint capability overrides", () => {
    const source: EndpointDefinition = {
      ...queueSource,
      schema: {
        type: "object",
        properties: {
          left: endpointCapabilitySchema("left", ["stream"]),
          right: endpointCapabilitySchema("right", ["batch"]),
        },
      },
      delivery_modes: ["batch", "stream"],
    };
    const schema = compileSchema(source.schema);

    expect(() =>
      configuredEndpointCapabilities(
        source,
        schema,
        { left: {}, right: {} },
        "source",
      ),
    ).toThrow(/conflicting source capabilities/);
  });

  it("fails closed when a configured override exceeds the catalog aggregate", () => {
    const source: EndpointDefinition = {
      ...queueSource,
      schema: endpointCapabilitySchema("invalid", ["batch"]),
      delivery_modes: ["stream"],
    };

    expect(() =>
      configuredEndpointCapabilities(
        source,
        compileSchema(source.schema),
        {},
        "source",
      ),
    ).toThrow(/subset of the catalog aggregate/);
  });
});

function conditionalSourceSchema() {
  return {
    type: "object" as const,
    properties: {
      replication: {
        anyOf: [
          endpointCapabilitySchema("replication", [
            "stream",
            "batch_and_stream",
          ]),
          { type: "null" as const },
        ],
      },
    },
    "x-ui": {
      capabilities: {
        component: "source",
        key: "snapshot",
        delivery_modes: ["batch"],
        record_semantics: ["append_only"],
      },
    },
  };
}

function endpointCapabilitySchema(key: string, deliveryModes: string[]) {
  return {
    type: "object" as const,
    properties: {
    },
    "x-ui": {
      capabilities: {
        component: "source",
        key,
        delivery_modes: deliveryModes,
        record_semantics: [key === "replication" ? "changelog" : "append_only"],
      },
    },
  };
}

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
