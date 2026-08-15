// @vitest-environment jsdom

import { fireEvent, render, within } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { describe, expect, it } from "vitest";

import {
  ParserDetailsForm,
  SchemaForm,
} from "../src/schema/SchemaForm";
import type { CompiledNode } from "../src/schema/compiler";
import type { JsonValue } from "../src/types";

const stringNode = (title?: string): CompiledNode => ({
  kind: "string",
  ...(title === undefined ? {} : { title }),
  xUi: {},
});

describe("schema form", () => {
  it("renders a scalar enum union as one select", () => {
    const node: CompiledNode = {
      kind: "union",
      xUi: {},
      branches: [
        {
          label: "String",
          constant: "string",
          node: {
            kind: "string",
            enumValues: ["string"],
            xUi: {},
          },
        },
        {
          label: "Integer",
          constant: "integer",
          node: {
            kind: "string",
            enumValues: ["integer"],
            xUi: {},
          },
        },
      ],
    };
    const { container } = render(
      <SchemaForm node={node} value="string" onChange={() => undefined} />,
    );
    expect(container.querySelectorAll(".select-trigger")).toHaveLength(1);
    expect(container.querySelector(".nested-section")).toBeNull();
  });

  it("drops pointer focus from disclosure summaries", async () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(),
      properties: {
        timeout: {
          ...stringNode("Timeout"),
          xUi: { section: "advanced" },
        },
      },
    };
    const { getByText } = render(
      <SchemaForm
        node={node}
        value={{ timeout: "10s" }}
        onChange={() => undefined}
      />,
    );
    const summary = getByText("Advanced settings");
    summary.focus();
    expect(document.activeElement).toBe(summary);
    fireEvent.click(summary, { detail: 1 });
    await Promise.resolve();
    expect(document.activeElement).not.toBe(summary);

    summary.focus();
    fireEvent.click(summary, { detail: 0 });
    expect(document.activeElement).toBe(summary);
  });

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

  it("renders parser selection in the endpoint and details separately", () => {
    const parserContainer: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["common", "json_parser"]),
      properties: {
        common: {
          kind: "object",
          xUi: {},
          required: new Set(["table_naming"]),
          properties: { table_naming: stringNode("Table name") },
        },
        json_parser: {
          kind: "object",
          xUi: {},
          required: new Set(["columns"]),
          properties: {
            columns: {
              kind: "array",
              xUi: { widget: "column_mappings" },
              item: {
                kind: "object",
                xUi: {},
                required: new Set(["column_name"]),
                properties: { column_name: stringNode("Column name") },
              },
            },
          },
        },
      },
    };
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["parser"]),
      properties: {
        parser: {
          kind: "union",
          xUi: { widget: "parser" },
          branches: [
            {
              label: "JSON parser",
              requiredKeys: ["common", "json_parser"],
              node: parserContainer,
            },
          ],
        },
      },
    };
    const value = {
      parser: {
        common: { table_naming: "events" },
        json_parser: { columns: [{ column_name: "id" }] },
      },
    };
    const endpoint = render(
      <SchemaForm
        node={node}
        value={value}
        parserSelectionOnly
        onChange={() => undefined}
      />,
    );
    expect(endpoint.container.textContent).toContain("JSON parser");
    expect(endpoint.container.textContent).not.toContain("Output columns");

    const details = render(
      <ParserDetailsForm
        node={node}
        value={value}
        onChange={() => undefined}
      />,
    );
    expect(details.container.textContent).toContain("JSON parser configuration");
    expect(details.container.textContent).toContain("Output columns");
  });

  it("renders data schema with one shared table header", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["columns"]),
      properties: {
        columns: {
          kind: "array",
          xUi: { widget: "column_mappings" },
          item: {
            kind: "object",
            xUi: {},
            required: new Set(["column_name", "jsonpath"]),
            properties: {
              column_name: stringNode(),
              jsonpath: stringNode(),
            },
          },
        },
      },
    };
    const { container } = render(
      <SchemaForm
        node={node}
        value={{
          columns: [
            { column_name: "id", jsonpath: "$.id" },
            { column_name: "value", jsonpath: "$.value" },
          ],
        }}
        onChange={() => undefined}
      />,
    );
    expect(container.querySelectorAll("thead")).toHaveLength(1);
    expect(container.querySelectorAll("th")).toHaveLength(7);
    expect(container.querySelectorAll("tbody .config-table-row")).toHaveLength(
      2,
    );
    expect(container.querySelector(".add-row-button")?.textContent).toBe(
      "+ Add column",
    );
    expect(container.querySelectorAll(".table-details-row")).toHaveLength(0);
  });

  it("keeps column settings behind the row actions menu", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["columns"]),
      properties: {
        columns: {
          kind: "array",
          xUi: { widget: "column_mappings" },
          item: {
            kind: "object",
            xUi: {},
            required: new Set(["column_name", "jsonpath"]),
            properties: {
              column_name: stringNode(),
              jsonpath: stringNode(),
              expression: stringNode("Expression"),
            },
          },
        },
      },
    };
    function Harness() {
      const [value, setValue] = useState<JsonValue>({
        columns: [
          {
            column_name: "id",
            jsonpath: "$.id",
            expression: "custom expression",
          },
        ],
      });
      return <SchemaForm node={node} value={value} onChange={setValue} />;
    }
    const { container } = render(<Harness />);
    const table = within(container as HTMLElement);
    expect(table.queryByText("Column settings")).toBeNull();
    expect(table.queryByText("Advanced column settings")).toBeNull();
    expect(container.querySelector(".custom-settings-dot")).not.toBeNull();

    fireEvent.click(table.getByRole("button", { name: "Column 1 actions" }));
    expect(table.getByRole("menuitem", { name: "Column settings" })).toBeTruthy();
    expect(table.getByRole("menuitem", { name: "Duplicate" })).toBeTruthy();
    expect(table.getByRole("menuitem", { name: "Delete" })).toBeTruthy();
    fireEvent.click(table.getByRole("menuitem", { name: "Column settings" }));

    expect(container.querySelectorAll(".table-details-row")).toHaveLength(1);
    expect(table.queryByText("Advanced column settings")).toBeTruthy();

    fireEvent.click(
      table.getByRole("button", { name: "Close column 1 settings" }),
    );
    fireEvent.click(table.getByRole("button", { name: "Column 1 actions" }));
    fireEvent.click(table.getByRole("menuitem", { name: "Duplicate" }));
    expect(container.querySelectorAll(".config-table-row")).toHaveLength(2);

    fireEvent.click(table.getByRole("button", { name: "Column 2 actions" }));
    fireEvent.click(table.getByRole("menuitem", { name: "Delete" }));
    expect(container.querySelectorAll(".config-table-row")).toHaveLength(1);
  });

  it("selects output columns and deletes them as one bulk action", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["columns"]),
      properties: {
        columns: {
          kind: "array",
          xUi: { widget: "column_mappings" },
          item: {
            kind: "object",
            xUi: {},
            required: new Set(["column_name", "jsonpath"]),
            properties: {
              column_name: stringNode(),
              jsonpath: stringNode(),
            },
          },
        },
        keys: {
          kind: "array",
          xUi: { widget: "column_keys" },
          item: stringNode(),
        },
      },
    };
    function Harness() {
      const [value, setValue] = useState<JsonValue>({
        columns: [
          { column_name: "id", jsonpath: "$.id" },
          { column_name: "value", jsonpath: "$.value" },
        ],
        keys: ["id", "value"],
      });
      return (
        <>
          <SchemaForm node={node} value={value} onChange={setValue} />
          <output data-testid="config-value">{JSON.stringify(value)}</output>
        </>
      );
    }
    const { container } = render(<Harness />);
    const form = within(container as HTMLElement);
    const selectAll = form.getByRole("checkbox", {
      name: "Select all output columns",
    }) as HTMLInputElement;

    fireEvent.click(
      form.getByRole("checkbox", { name: "Select output column 1" }),
    );
    expect(selectAll.indeterminate).toBe(true);
    expect(form.getByText("1 selected")).toBeTruthy();
    expect(container.querySelectorAll(".config-table-row.selected")).toHaveLength(
      1,
    );

    fireEvent.click(selectAll);
    expect(selectAll.checked).toBe(true);
    expect(form.getByText("2 selected")).toBeTruthy();
    fireEvent.click(
      form.getByRole("button", { name: "Delete 2 selected columns" }),
    );

    expect(container.querySelectorAll(".config-table-row")).toHaveLength(0);
    expect(form.getByTestId("config-value").textContent).toBe(
      JSON.stringify({ columns: [], keys: [] }),
    );
  });
});
