// @vitest-environment jsdom

import { fireEvent, render } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { describe, expect, it } from "vitest";

import { SchemaForm } from "../src/schema/SchemaForm";
import type { CompiledNode } from "../src/schema/compiler";
import type { JsonValue } from "../src/types";

const stringNode = (title?: string): CompiledNode => ({
  kind: "string",
  ...(title === undefined ? {} : { title }),
  xUi: {},
});

describe("schema form", () => {
  it("does not render an empty nested panel for a discriminator-only branch", () => {
    const node: CompiledNode = {
      kind: "union",
      xUi: {},
      branches: [
        {
          label: "From topic name",
          discriminator: { key: "type", value: "from_topic_name" },
          requiredKeys: ["type"],
          node: {
            kind: "object",
            xUi: {},
            required: new Set(["type"]),
            properties: {
              type: {
                kind: "string",
                enumValues: ["from_topic_name"],
                xUi: {},
              },
            },
          },
        },
      ],
    };
    const { container } = render(
      <SchemaForm
        node={node}
        value={{ type: "from_topic_name" }}
        onChange={() => undefined}
      />,
    );
    expect(container.querySelector(".nested-section")).toBeNull();
  });

  it("hides tagged-union discriminator controls", () => {
    const node: CompiledNode = {
      kind: "union",
      xUi: {},
      branches: [
        {
          label: "Send to rest",
          discriminator: { key: "action", value: "rest" },
          requiredKeys: ["action", "column_name"],
          node: {
            kind: "object",
            xUi: {},
            required: new Set(["action", "column_name"]),
            properties: {
              action: {
                kind: "string",
                enumValues: ["rest"],
                xUi: {},
              },
              column_name: stringNode("Rest column name"),
            },
          },
        },
      ],
    };
    const { queryByText, getByText } = render(
      <SchemaForm
        node={node}
        value={{ action: "rest", column_name: "rest" }}
        onChange={() => undefined}
      />,
    );
    expect(getByText("Rest column name")).toBeTruthy();
    expect(queryByText("Action")).toBeNull();
  });

  it("builds compound key options from output and enabled system columns", () => {
    const columns: CompiledNode = {
      kind: "array",
      xUi: { widget: "column_mappings" },
      item: {
        kind: "object",
        xUi: {},
        required: new Set(["column_name", "jsonpath"]),
        properties: {
          column_name: stringNode("Column name"),
          jsonpath: stringNode("Path"),
        },
      },
    };
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["common", "json_parser"]),
      properties: {
        common: {
          kind: "object",
          xUi: {},
          required: new Set(["table_naming"]),
          properties: {
            table_naming: stringNode("Table name"),
            system_columns: {
              kind: "object",
              xUi: { widget: "system_columns" },
              required: new Set(),
              properties: { offset: stringNode() },
            },
          },
        },
        json_parser: {
          kind: "object",
          xUi: {},
          required: new Set(["columns"]),
          properties: {
            columns,
            keys: {
              kind: "array",
              xUi: { widget: "column_keys" },
              item: stringNode(),
            },
          },
        },
      },
    };
    const initial: JsonValue = {
      common: {
        table_naming: "events",
        system_columns: { offset: "source_offset" },
      },
      json_parser: {
        columns: [{ column_name: "id", jsonpath: "$.id" }],
        keys: [],
      },
    };
    function Harness() {
      const [value, setValue] = useState(initial);
      return <SchemaForm node={node} value={value} onChange={setValue} />;
    }
    const { container, getByRole } = render(<Harness />);
    const keys = container.querySelector<HTMLButtonElement>(
      ".column-keys .select-trigger",
    );
    expect(keys).not.toBeNull();
    fireEvent.click(keys!);
    fireEvent.click(getByRole("option", { name: /id/ }));
    fireEvent.click(getByRole("option", { name: /source_offset/ }));
    expect(keys!.textContent).toContain("id, source_offset");
  });
});
