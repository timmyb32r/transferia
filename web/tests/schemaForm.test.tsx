// @vitest-environment jsdom

import { fireEvent, render, waitFor, within } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { describe, expect, it, vi } from "vitest";

import { api } from "../src/api";
import {
  ParserDetailsForm,
  SchemaForm,
  SelectControl,
  SerializerDetailsForm,
} from "../src/schema/SchemaForm";
import type { CompiledNode } from "../src/schema/compiler";
import type { JsonValue } from "../src/types";

const stringNode = (title?: string): CompiledNode => ({
  kind: "string",
  ...(title === undefined ? {} : { title }),
  xUi: {},
});

describe("schema form", () => {
  it("renders a separate safely encoded external-console link", () => {
    const node: CompiledNode = {
      kind: "string",
      xUi: {
        external_link_template: "https://console.example/topics/{value}",
      },
    };
    const view = render(
      <SchemaForm
        node={node}
        value="account/topic name"
        onChange={() => undefined}
      />,
    );
    const link = view.getByRole("link", { name: "Open in external console" });
    expect(link.getAttribute("href")).toBe(
      "https://console.example/topics/account%2Ftopic%20name",
    );
    expect(link.getAttribute("target")).toBe("_blank");
    expect(link.getAttribute("rel")).toBe("noopener noreferrer");
  });

  it("does not render an external-console link for an empty value", () => {
    const node: CompiledNode = {
      kind: "string",
      xUi: {
        external_link_template: "https://console.example/items/{value}",
      },
    };
    const view = render(
      <SchemaForm node={node} value="" onChange={() => undefined} />,
    );
    expect(view.container.querySelector("a")).toBeNull();
  });

  it("keeps shard group compact and replaces it with checked options", () => {
    const node: CompiledNode = {
      kind: "object",
      required: new Set(),
      additionalProperties: false,
      xUi: {},
      properties: {
        shard_group: {
          kind: "string",
          title: "Shard group",
          xUi: { section: "shard_group" },
        },
      },
    };
    const view = render(
      <SchemaForm
        node={node}
        value={{ shard_group: "production" }}
        optionOverrides={{ "#/shard_group": ["production", "analytics"] }}
        onChange={() => undefined}
      />,
    );

    expect(view.container.querySelector("details")?.open).toBe(false);
    fireEvent.click(view.container.querySelector("summary")!);
    const control = view.container.querySelector("#field---shard_group")!;
    expect(control.tagName).toBe("BUTTON");
    expect(control.textContent).toContain("production");
    view.unmount();
  });

  it("associates schema labels with real controls using stable paths", () => {
    const node: CompiledNode = {
      kind: "object",
      required: new Set(["name", "mode", "secret", "hosts"]),
      additionalProperties: false,
      xUi: {},
      properties: {
        name: { kind: "string", title: "Name", xUi: {} },
        mode: {
          kind: "string",
          title: "Mode",
          enumValues: ["one", "two"],
          xUi: {},
        },
        secret: {
          kind: "string",
          title: "Secret",
          xUi: { widget: "password" },
        },
        hosts: {
          kind: "array",
          title: "Hosts",
          item: { kind: "string", xUi: {} },
          xUi: { widget: "compact_array", item_label: "host" },
        },
      },
    };
    const view = render(
      <SchemaForm
        node={node}
        value={{ name: "", mode: "", secret: "", hosts: [""] }}
        onChange={() => undefined}
      />,
    );

    expect(view.getByLabelText("Name").getAttribute("id")).toBe("field---name");
    expect(view.getByLabelText("Mode").tagName).toBe("BUTTON");
    expect(view.getByLabelText("Secret").getAttribute("type")).toBe("password");
    expect(view.getByLabelText("Host row 1").getAttribute("id")).toMatch(
      /^compact-row-\d+-value$/,
    );
    view.unmount();
  });

  it("does not turn a cleared number input into zero", () => {
    const onChange = vi.fn();
    const view = render(
      <SchemaForm
        node={{ kind: "number", integer: true, xUi: {} }}
        value={12}
        onChange={onChange}
      />,
    );

    fireEvent.input(view.getByRole("spinbutton"), { target: { value: "" } });

    expect(onChange).toHaveBeenCalledWith(null);
    expect(onChange).not.toHaveBeenCalledWith(0);
    view.unmount();
  });

  it("keeps a cleared byte-size input empty", () => {
    const onChange = vi.fn();
    const view = render(
      <SchemaForm
        node={{ kind: "number", integer: true, xUi: { widget: "byte_size" } }}
        value={1024}
        onChange={onChange}
      />,
    );

    fireEvent.input(view.getByRole("spinbutton"), { target: { value: "" } });

    expect(onChange).toHaveBeenCalledWith(null);
    expect(onChange).not.toHaveBeenCalledWith(0);
    view.unmount();
  });

  it("ignores dynamic options returned for an older source", async () => {
    const oldRequest = deferred<Awaited<ReturnType<typeof api.options>>>();
    const newRequest = deferred<Awaited<ReturnType<typeof api.options>>>();
    const options = vi
      .spyOn(api, "options")
      .mockImplementation(({ key }) =>
        key === "old-options" ? oldRequest.promise : newRequest.promise,
      );
    const dynamicNode = (source: string): CompiledNode => ({
      kind: "string",
      xUi: { dynamic_options: source },
    });
    const view = render(
      <SchemaForm
        node={dynamicNode("old-options")}
        value="selected"
        onChange={() => undefined}
      />,
    );
    await waitFor(() =>
      expect(options).toHaveBeenCalledWith({
        key: "old-options",
        dependencies: {},
        signal: expect.anything(),
      }),
    );

    view.rerender(
      <SchemaForm
        node={dynamicNode("new-options")}
        value="selected"
        onChange={() => undefined}
      />,
    );
    await waitFor(() =>
      expect(options).toHaveBeenCalledWith({
        key: "new-options",
        dependencies: {},
        signal: expect.anything(),
      }),
    );
    newRequest.resolve({
      options: [{ value: "selected", label: "New option" }],
    });
    oldRequest.resolve({
      options: [{ value: "old", label: "Old option" }],
    });
    await waitFor(() =>
      expect(view.getByRole("button", { name: "New option" })).toBeTruthy(),
    );

    fireEvent.pointerDown(view.getByRole("button", { name: "New option" }), {
      button: 0,
    });
    expect(view.getByRole("option", { name: "New option" })).toBeTruthy();
    expect(view.queryByRole("option", { name: "Old option" })).toBeNull();
    options.mockRestore();
    view.unmount();
  });

  it("dismisses stale text selection before dropdown interactions", () => {
    const { getByRole } = render(
      <>
        <input aria-label="Column name" defaultValue="id" />
        <SelectControl
          value=""
          placeholder="Not selected"
          options={[{ value: "string", label: "String" }]}
          onChange={() => undefined}
        />
      </>,
    );
    const input = getByRole("textbox", {
      name: "Column name",
    }) as HTMLInputElement;
    const trigger = getByRole("button", { name: "Not selected" });

    input.focus();
    input.setSelectionRange(0, input.value.length);
    fireEvent.pointerDown(trigger);
    expect(document.activeElement).not.toBe(input);
    expect(input.selectionStart).toBe(input.value.length);
    expect(input.selectionEnd).toBe(input.value.length);

    expect(getByRole("option", { name: "String" })).toBeTruthy();
    input.focus();
    input.setSelectionRange(0, input.value.length);
    const option = getByRole("option", { name: "String" });
    fireEvent.pointerDown(option);
    expect(document.activeElement).not.toBe(input);
    expect(input.selectionStart).toBe(input.value.length);
    expect(input.selectionEnd).toBe(input.value.length);
    fireEvent.click(option);
  });

  it("opens reliably on pointer down and clears a stale search", () => {
    const { container } = render(
      <SelectControl
        value=""
        placeholder="Not selected"
        searchable
        options={[
          { value: "string", label: "String" },
          { value: "integer", label: "Integer" },
        ]}
        onChange={() => undefined}
      />,
    );
    const form = within(container as HTMLElement);
    const trigger = form.getByRole("button", { name: "Not selected" });

    fireEvent.pointerDown(trigger, { button: 0, clientX: 1 });
    fireEvent.input(form.getByRole("searchbox"), {
      target: { value: "no such option" },
    });
    expect(
      form.getByRole("searchbox").closest(".select-menu")?.textContent,
    ).toContain("No matches");

    fireEvent.pointerDown(trigger, { button: 0, clientX: 219 });
    fireEvent.pointerDown(trigger, { button: 0, clientX: 1 });
    expect(form.getByRole("option", { name: "String" })).toBeTruthy();
    expect(form.getByRole("option", { name: "Integer" })).toBeTruthy();
  });

  it("closes an anchored select when the user clicks outside", () => {
    const view = render(
      <SelectControl
        value=""
        placeholder="Not selected"
        options={[{ value: "string", label: "String" }]}
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    const trigger = form.getByRole("button", { name: "Not selected" });
    fireEvent.pointerDown(trigger, { button: 0 });
    expect(form.getByRole("option", { name: "String" })).toBeTruthy();

    fireEvent.pointerDown(document.body);

    expect(form.queryByRole("option", { name: "String" })).toBeNull();
    view.unmount();
  });

  it("keeps only one dropdown open at a time", () => {
    const view = render(
      <>
        <SelectControl
          value=""
          placeholder="First"
          options={[{ value: "first", label: "First option" }]}
          onChange={() => undefined}
        />
        <SelectControl
          value=""
          placeholder="Second"
          options={[{ value: "second", label: "Second option" }]}
          onChange={() => undefined}
        />
      </>,
    );

    fireEvent.pointerDown(view.getByRole("button", { name: "First" }));
    expect(view.getByRole("option", { name: "First option" })).toBeTruthy();

    fireEvent.pointerDown(view.getByRole("button", { name: "Second" }));
    expect(view.queryByRole("option", { name: "First option" })).toBeNull();
    expect(view.getByRole("option", { name: "Second option" })).toBeTruthy();
  });

  it("loads dynamic options when opened from the keyboard", async () => {
    const options = vi.spyOn(api, "options").mockResolvedValue({
      options: [{ value: "cluster", label: "Cluster" }],
    });
    const view = render(
      <SchemaForm
        node={{
          kind: "string",
          xUi: { dynamic_options: "clusters" },
        }}
        value=""
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    const trigger = form.getByRole("button", { name: "Not selected" });
    trigger.focus();
    fireEvent.keyDown(trigger, { key: "ArrowDown" });

    expect(options).toHaveBeenCalledWith({
      key: "clusters",
      dependencies: {},
      signal: expect.anything(),
    });
    expect(await form.findByRole("option", { name: "Cluster" })).toBeTruthy();
    view.unmount();
    options.mockRestore();
  });

  it("passes current field dependencies to dynamic options and falls back to text without them", async () => {
    const options = vi.spyOn(api, "options").mockResolvedValue({
      options: [{ value: "db1", label: "db1" }],
    });
    const node: CompiledNode = {
      kind: "object",
      required: new Set(["installation", "database"]),
      xUi: {},
      properties: {
        installation: {
          kind: "object",
          required: new Set(["cluster_id"]),
          xUi: {},
          properties: {
            cluster_id: { kind: "string", xUi: {} },
          },
        },
        database: {
          kind: "string",
          xUi: {
            dynamic_options: "databases",
            dynamic_options_dependencies: {
              cluster_id: "/installation/cluster_id",
            },
          },
        },
      },
    };
    const view = render(
      <SchemaForm
        node={node}
        value={{ installation: { cluster_id: "mdb1" }, database: "" }}
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    fireEvent.pointerDown(form.getByRole("button", { name: "Database" }));
    await waitFor(() =>
      expect(options).toHaveBeenCalledWith({
        key: "databases",
        dependencies: { cluster_id: "mdb1" },
        signal: expect.anything(),
      }),
    );

    view.rerender(
      <SchemaForm
        node={node}
        value={{ installation: { cluster_id: "" }, database: "manual" }}
        onChange={() => undefined}
      />,
    );
    expect(form.getByDisplayValue("manual")).toBeTruthy();
    options.mockRestore();
    view.unmount();
  });

  it("shows progress while dynamic options are loading", async () => {
    const pending = deferred<Awaited<ReturnType<typeof api.options>>>();
    const options = vi.spyOn(api, "options").mockReturnValue(pending.promise);
    const view = render(
      <SchemaForm
        node={{ kind: "string", xUi: { dynamic_options: "clusters" } }}
        value=""
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);

    fireEvent.pointerDown(form.getByRole("button", { name: "Not selected" }));

    expect((await form.findByRole("status")).textContent).toContain("Loading…");
    expect(form.queryByText("No matches")).toBeNull();
    pending.resolve({ options: [{ value: "cluster", label: "Cluster" }] });
    expect(await form.findByRole("option", { name: "Cluster" })).toBeTruthy();
    view.unmount();
    options.mockRestore();
  });

  it("closes an open select when the form becomes read-only", () => {
    const onChange = vi.fn();
    const node: CompiledNode = {
      kind: "string",
      enumValues: ["first"],
      xUi: { labels: { first: "First option" } },
    };
    const view = render(
      <SchemaForm node={node} value="" onChange={onChange} />,
    );
    const form = within(view.container as HTMLElement);
    fireEvent.pointerDown(form.getByRole("button", { name: "Not selected" }));
    expect(form.getByRole("option", { name: "First option" })).toBeTruthy();

    view.rerender(
      <SchemaForm node={node} value="" disabled onChange={onChange} />,
    );

    expect(form.queryByRole("option", { name: "First option" })).toBeNull();
    expect(onChange).not.toHaveBeenCalled();
  });

  it("supports arrow navigation and Escape in dropdowns", async () => {
    const { container } = render(
      <SelectControl
        value=""
        placeholder="Not selected"
        options={[
          { value: "string", label: "String" },
          { value: "integer", label: "Integer" },
        ]}
        onChange={() => undefined}
      />,
    );
    const form = within(container as HTMLElement);
    const trigger = form.getByRole("button", { name: "Not selected" });
    trigger.focus();
    fireEvent.keyDown(trigger, { key: "ArrowDown" });
    await Promise.resolve();
    expect(document.activeElement).toBe(
      form.getByRole("option", { name: "String" }),
    );
    fireEvent.keyDown(document.activeElement!, { key: "ArrowDown" });
    expect(document.activeElement).toBe(
      form.getByRole("option", { name: "Integer" }),
    );
    fireEvent.keyDown(document.activeElement!, { key: "Escape" });
    expect(document.activeElement).toBe(trigger);
    expect(form.queryByRole("option")).toBeNull();
  });

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

  it("gives installation variants a full-width nested layout", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["installation"]),
      properties: {
        installation: {
          kind: "union",
          xUi: { control_width: "installation" },
          branches: [
            {
              label: "Instance",
              discriminator: { key: "type", value: "instance" },
              requiredKeys: ["type", "instance"],
              node: {
                kind: "object",
                xUi: {},
                required: new Set(["type", "instance"]),
                properties: {
                  type: {
                    kind: "string",
                    enumValues: ["instance"],
                    xUi: {},
                  },
                  instance: {
                    kind: "string",
                    enumValues: [
                      "sas.logbroker-prestable.example.net",
                      "vla.logbroker-prestable.example.net",
                    ],
                    xUi: {},
                  },
                },
              },
            },
          ],
        },
      },
    };
    const { container } = render(
      <SchemaForm
        node={node}
        value={{ installation: { type: "instance", instance: "" } }}
        onChange={() => undefined}
      />,
    );

    const row = container.querySelector(".form-row-installation");
    expect(row).toBeTruthy();
    expect(row?.querySelector(".nested-section .select")).toBeTruthy();
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
          label: "Send to a column",
          discriminator: { key: "action", value: "send_to_column" },
          requiredKeys: ["action", "column_name"],
          node: {
            kind: "object",
            xUi: {},
            required: new Set(["action", "column_name"]),
            properties: {
              action: {
                kind: "string",
                enumValues: ["send_to_column"],
                xUi: {},
              },
              column_name: {
                ...stringNode("Column name"),
                defaultValue: "additional_properties",
              },
            },
          },
        },
      ],
    };
    const { queryByText, getByText } = render(
      <SchemaForm
        node={node}
        value={{
          action: "send_to_column",
          column_name: "additional_properties",
        }}
        onChange={() => undefined}
      />,
    );
    expect(getByText("Column name")).toBeTruthy();
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
      xUi: { widget: "json_parser" },
      required: new Set(["common", "json_parser"]),
      properties: {
        common: {
          kind: "object",
          xUi: { widget: "parser_common" },
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
    const systemColumns = getByRole("button", { name: "Add system columns" });
    expect(systemColumns.closest(".column-editor-heading")).not.toBeNull();
    expect(container.querySelector(".schema-system-columns-panel")).toBeNull();
    fireEvent.click(systemColumns);
    expect(
      container.querySelector(".schema-system-columns-panel"),
    ).not.toBeNull();

    const keys = container.querySelector<HTMLButtonElement>(
      ".column-keys .select-trigger",
    );
    expect(keys).not.toBeNull();
    fireEvent.click(keys!);
    const search = container.querySelector<HTMLInputElement>(
      ".column-keys .select-search",
    );
    expect(search).not.toBeNull();
    const id = getByRole("option", { name: /id/ });
    expect(id.closest(".select-menu-floating")).not.toBeNull();
    fireEvent.pointerDown(id, { button: 0, clientX: 1 });
    expect(keys!.textContent).toContain("id");
    fireEvent.input(search!, { target: { value: "source" } });
    expect(container.querySelector('[role="option"]')?.textContent).toBe(
      "source_offset",
    );
    fireEvent.click(getByRole("option", { name: /source_offset/ }));
    expect(keys!.textContent).toContain("id, source_offset");
  });

  it("renders parser selection in the endpoint and details separately", () => {
    const parserContainer: CompiledNode = {
      kind: "object",
      xUi: { widget: "json_parser" },
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
        connectionAction={<button>Check connection</button>}
        parserAction={<button aria-label="Preview one message">eye</button>}
        onChange={() => undefined}
      />,
    );
    expect(endpoint.container.textContent).toContain("JSON parser");
    expect(endpoint.container.textContent).not.toContain("Output columns");
    const connectionAction = endpoint.getByRole("button", {
      name: "Check connection",
    });
    const parserSelector = endpoint.container.querySelector(".union-editor")!;
    expect(connectionAction.compareDocumentPosition(parserSelector)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );
    expect(
      endpoint.getByRole("button", { name: "Preview one message" }),
    ).toBeTruthy();

    const details = render(
      <ParserDetailsForm
        node={node}
        value={value}
        onChange={() => undefined}
      />,
    );
    expect(details.container.textContent).toContain("JSON parser settings");
    expect(details.container.textContent).not.toContain("Parser settings");
    expect(details.container.textContent).toContain("Output columns");
    expect(
      details.container.querySelector(".source-parser-bridge"),
    ).not.toBeNull();
  });

  it("renders serializer selection and settings separately", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["serializer"]),
      properties: {
        serializer: {
          kind: "union",
          xUi: { widget: "serializer" },
          branches: [
            {
              label: "JSON",
              requiredKeys: ["type"],
              discriminator: { key: "type", value: "json" },
              node: {
                kind: "object",
                xUi: {},
                required: new Set(["type"]),
                properties: {
                  type: {
                    kind: "string",
                    xUi: {},
                    enumValues: ["json"],
                  },
                },
              },
            },
            {
              label: "Schema Registry",
              requiredKeys: ["type", "connection"],
              discriminator: { key: "type", value: "schema_registry" },
              node: {
                kind: "object",
                xUi: {},
                required: new Set(["type", "connection"]),
                properties: {
                  type: {
                    kind: "string",
                    xUi: {},
                    enumValues: ["schema_registry"],
                  },
                  connection: stringNode("Registry URL"),
                },
              },
            },
          ],
        },
      },
    };
    const changes: JsonValue[] = [];
    const endpoint = render(
      <SchemaForm
        node={node}
        value={{ serializer: { type: "json" } }}
        serializerSelectionOnly
        onChange={(next) => changes.push(next)}
      />,
    );
    expect(endpoint.container.textContent).toContain("JSON");
    expect(endpoint.container.textContent).not.toContain("Registry URL");
    fireEvent.click(endpoint.container.querySelector(".select-trigger")!);
    fireEvent.click(endpoint.getByRole("option", { name: "Schema Registry" }));
    expect(changes.at(-1)).toEqual({
      serializer: { type: "schema_registry", connection: "" },
    });

    const details = render(
      <SerializerDetailsForm
        node={node}
        value={{
          serializer: {
            type: "schema_registry",
            connection: "https://registry",
          },
        }}
        onChange={() => undefined}
      />,
    );
    expect(details.container.textContent).toContain("Schema Registry settings");
    expect(details.getByDisplayValue("https://registry")).toBeTruthy();
    expect(
      details.container.querySelector(".sink-serializer-bridge"),
    ).not.toBeNull();
  });

  it("shows partition IDs only when explicit partition selection is enabled", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["topics", "driver"]),
      properties: {
        topics: {
          kind: "array",
          xUi: { widget: "compact_array", item_label: "topic" },
          item: {
            kind: "object",
            xUi: {},
            required: new Set(["path", "partitions"]),
            properties: {
              path: stringNode("Topic path"),
              partitions: {
                kind: "array",
                xUi: { widget: "partition_ranges" },
                title: "Partition IDs",
                item: {
                  kind: "number",
                  integer: true,
                  xUi: {},
                },
              },
            },
          },
        },
        driver: {
          kind: "string",
          enumValues: ["ydb", "pqv1"],
          xUi: { section: "advanced" },
        },
      },
    };
    function Harness() {
      const [value, setValue] = useState<JsonValue>({
        topics: [{ path: "/events", partitions: [] }],
        driver: "ydb",
      });
      return (
        <>
          <SchemaForm node={node} value={value} onChange={setValue} />
          <output data-testid="partition-config">
            {JSON.stringify(value)}
          </output>
        </>
      );
    }
    const { container } = render(<Harness />);
    const form = within(container as HTMLElement);

    expect(form.queryByText(/Partition IDs/)).toBeNull();
    fireEvent.click(form.getByText("Advanced settings"));
    const toggle = form.getByRole("checkbox", { name: "Specify partitions" });
    fireEvent.click(toggle);
    const partitions = form.getByPlaceholderText("e.g. 1-5,7");
    fireEvent.input(partitions, { target: { value: "1-3" } });
    expect(form.getByTestId("partition-config").textContent).toContain(
      '"partitions":[1,2,3]',
    );

    fireEvent.click(toggle);
    expect(container.textContent).not.toContain("Partition IDs");
    expect(form.getByTestId("partition-config").textContent).toContain(
      '"partitions":[]',
    );
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
    expect(container.querySelectorAll("th")).toHaveLength(8);
    expect(container.querySelectorAll("tbody .config-table-row")).toHaveLength(
      2,
    );
    expect(container.querySelector(".add-row-button")?.textContent).toBe(
      "+ Add column",
    );
    expect(container.querySelectorAll(".table-details-row")).toHaveLength(0);
  });

  it("reorders output columns with the drag handle", () => {
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
    function Harness() {
      const [value, setValue] = useState<JsonValue>({
        columns: [
          { column_name: "id", jsonpath: "$.id" },
          { column_name: "value", jsonpath: "$.value" },
        ],
      });
      return <SchemaForm node={node} value={value} onChange={setValue} />;
    }
    const { container } = render(<Harness />);
    const form = within(container as HTMLElement);
    const rows = container.querySelectorAll(".config-table-row");
    const dataTransfer = {
      dropEffect: "none",
      effectAllowed: "none",
      setData: () => undefined,
      setDragImage: (image: Element) => {
        expect(image.classList.contains("column-drag-preview")).toBe(true);
        expect(
          image.querySelector<HTMLInputElement>('input[type="text"]')?.value,
        ).toBe("id");
      },
    };
    fireEvent.dragStart(
      form.getByRole("button", { name: "Move output column 1" }),
      { dataTransfer },
    );
    fireEvent.dragOver(rows[1]!, { dataTransfer, clientY: 9 });
    const dragTarget = container.querySelectorAll(".config-table-row")[1]!;
    fireEvent.drop(dragTarget, { dataTransfer, clientY: 9 });

    const values = [
      ...container.querySelectorAll<HTMLInputElement>(
        '.column-table tbody .config-table-row input[type="text"]',
      ),
    ].map((input) => input.value);
    expect(values).toEqual(["value", "$.value", "id", "$.id"]);

    fireEvent.click(form.getByRole("button", { name: "Column 1 actions" }));
    fireEvent.click(form.getByRole("menuitem", { name: "Move down" }));
    const movedWithMenu = [
      ...container.querySelectorAll<HTMLInputElement>(
        '.column-table tbody .config-table-row input[type="text"]',
      ),
    ].map((input) => input.value);
    expect(movedWithMenu).toEqual(["id", "$.id", "value", "$.value"]);
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
    const actionsMenu = table.getByRole("menu");
    expect(actionsMenu.classList.contains("row-actions-menu-floating")).toBe(
      true,
    );
    expect(actionsMenu.style.left).not.toBe("");
    expect(
      table.getByRole("menuitem", { name: "Column settings" }),
    ).toBeTruthy();
    expect(table.getByRole("menuitem", { name: "Duplicate" })).toBeTruthy();
    expect(table.getByRole("menuitem", { name: "Delete" })).toBeTruthy();
    fireEvent.pointerDown(document.body);
    expect(table.queryByRole("menu")).toBeNull();
    fireEvent.click(table.getByRole("button", { name: "Column 1 actions" }));
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
    expect(
      container.querySelectorAll(".config-table-row.selected"),
    ).toHaveLength(1);

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

  it("preserves the remaining column row identity after deleting a sibling", () => {
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
    function Harness() {
      const [value, setValue] = useState<JsonValue>({
        columns: [
          { column_name: "id", jsonpath: "$.id" },
          { column_name: "value", jsonpath: "$.value" },
        ],
      });
      return <SchemaForm node={node} value={value} onChange={setValue} />;
    }
    const view = render(<Harness />);
    const form = within(view.container as HTMLElement);
    const remainingInput = form.getByDisplayValue("value");
    remainingInput.focus();
    fireEvent.click(
      form.getByRole("checkbox", { name: "Select output column 1" }),
    );
    fireEvent.click(
      form.getByRole("button", { name: "Delete 1 selected column" }),
    );

    expect(form.getByDisplayValue("value")).toBe(remainingInput);
    expect(document.activeElement).toBe(remainingInput);
    view.unmount();
  });
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}
