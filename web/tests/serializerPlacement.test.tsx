// @vitest-environment jsdom
import { cleanup } from "@testing-library/preact";
import { afterEach, expect, it } from "vitest";
import { EndpointCard } from "../src/delivery/EndpointCard";
import type { EndpointDefinition } from "../src/types";
import { render } from "./support/render";

afterEach(cleanup);

it.each(["kafka", "logbroker"])("keeps %s serializer settings inside the endpoint", (key) => {
  const endpoint: EndpointDefinition = {
    connection_check: false, message_preview: false, table_preview: false, partitioned: false,
    delivery_modes: ["stream"], record_semantics: ["append_only"], initial: {},
    schema: {
      type: "object", required: ["serializer"], properties: {
        serializer: {
          title: "Serializer", "x-ui": { widget: "serializer" },
          oneOf: [{ title: "JSON", type: "object", required: ["type"], properties: {
            type: { type: "string", const: "json" },
          } }, { title: "Schema Registry", type: "object", required: ["type", "connection"], properties: {
            type: { type: "string", const: "schema_registry" },
            connection: { type: "string", title: "Registry URL" },
          } }],
        },
      },
    },
  };
  const view = render(<EndpointCard title="Destination" role="sink" selectedKey={key}
    connectors={[{ key, title: key, sink: endpoint }]} endpoint={endpoint}
    config={{ sink: { [key]: { serializer: { type: "schema_registry", connection: "https://registry" } } } }}
    readOnly={false} showRequiredErrors={false} onChoose={() => {}} onConfig={() => {}} />);
  const input = view.getByRole("textbox", { name: "Registry URL" });
  expect(input.closest(".endpoint-card-sink")).not.toBeNull();
  const field = input.closest(".serializer-inline-settings")!;
  expect(field).not.toBeNull();
  const selector = field.querySelector(".select-trigger")!;
  expect(selector.compareDocumentPosition(input) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
  expect(view.container.querySelector(".serializer-details-card")).toBeNull();
});
