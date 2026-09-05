// @vitest-environment jsdom

import { cleanup, fireEvent, waitFor, within } from "@testing-library/preact";
import { useRef, useState } from "preact/hooks";
import { afterEach, describe, expect, it, vi } from "vitest";

import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";
import { RequiredFieldGuide } from "../src/delivery/RequiredFieldGuide";
import { SchemaForm } from "../src/schema/SchemaForm";
import { revealDetails } from "../src/schema/revealDetails";
import {
  ParserDetailsForm,
} from "../src/features/variantDetails/VariantDetailsForms";
import { SelectControl } from "../src/ui/SelectControl";
import type { CompiledNode } from "../src/schema/compiler";
import type { JsonValue } from "../src/types";
import { render } from "./support/render";

afterEach(cleanup);

const stringNode = (title?: string): CompiledNode => ({
  kind: "string",
  ...(title === undefined ? {} : { title }),
  xUi: {},
});

describe("schema form", () => {
  it("reveals the table name and focuses its input instead of the parser card", async () => {
    const view = render(<div class="parser-details-card" tabIndex={-1}>
      <div data-field-name="table_naming"><input aria-label="Table name" /></div>
      <input aria-label="Another setting" />
    </div>);
    const card = view.container.querySelector<HTMLElement>(".parser-details-card")!;
    const field = view.container.querySelector<HTMLElement>('[data-field-name="table_naming"]')!;
    card.scrollIntoView = vi.fn();
    field.scrollIntoView = vi.fn();
    revealDetails(".parser-details-card");
    await waitFor(() => expect(document.activeElement).toBe(view.getByRole("textbox", { name: "Table name" })));
    expect(field.scrollIntoView).toHaveBeenCalledWith({ behavior: "smooth", block: "start" });
    expect(card.scrollIntoView).not.toHaveBeenCalled();
  });
  it("marks only the missing output cell, not filled rows or table checkboxes", () => {
    const node: CompiledNode = {
      kind: "object", xUi: {}, required: new Set(["columns"]),
      properties: { columns: {
        kind: "array", xUi: { widget: "column_mappings" },
        item: { kind: "object", xUi: {}, required: new Set(["column_name", "arrow_type"]), properties: {
          column_name: stringNode(), arrow_type: { kind: "string", xUi: {}, enumValues: ["Utf8"] },
        } },
      } },
    };
    const view = render(<SchemaForm node={node} showRequiredErrors value={{ columns: [
      { column_name: "id", arrow_type: "Utf8" }, { column_name: "value", arrow_type: "" },
    ] }} onChange={() => undefined} />);
    const cells = view.container.querySelectorAll(".column-table td.required-incomplete");
    expect(cells.length).toBe(1);
    expect(cells[0]?.classList.contains("arrow-type-cell")).toBe(true);
    expect(cells[0]?.querySelector('input[type="checkbox"]')).toBeNull();
    expect(view.container.querySelector("tr.required-incomplete")).toBeNull();
  });
  it("breaks timestamp display after the type name without changing its value", () => {
    const node: CompiledNode = {
      kind: "object", xUi: {}, required: new Set(),
      properties: { columns: {
        kind: "array", xUi: { widget: "column_mappings" },
        item: { kind: "object", xUi: {}, required: new Set(), properties: {
          column_name: stringNode(),
          arrow_type: { kind: "string", xUi: {}, enumValues: ["Utf8", "Timestamp(Microsecond, UTC)"] },
        } },
      } },
    };
    const onChange = vi.fn();
    const view = render(<SchemaForm node={node} value={{ columns: [{ column_name: "time", arrow_type: "Timestamp(Microsecond, UTC)" }] }} onChange={onChange} />);
    expect(view.container.querySelector(".arrow-type-cell .select-trigger > span")?.textContent).toBe("Timestamp\n(Microsecond, UTC)");
    expect(onChange).not.toHaveBeenCalled();
  });
  it("does not offer decimal in the JSON type column", () => {
    const node: CompiledNode = {
      kind: "object", xUi: {}, required: new Set(),
      properties: {
        columns: {
          kind: "array", xUi: { widget: "column_mappings" },
          item: {
            kind: "object", xUi: {}, required: new Set(),
            properties: {
              column_name: stringNode(),
              json_data_type: {
                kind: "string", xUi: {},
                enumValues: ["string", "number", "boolean", "json", "decimal"],
              },
            },
          },
        },
      },
    };
    const view = render(<SchemaForm node={node} value={{ columns: [{ column_name: "id", json_data_type: "string" }] }} onChange={() => undefined} />);
    fireEvent.click(view.container.querySelector<HTMLButtonElement>(".column-table .select-trigger")!);
    expect(view.queryByRole("option", { name: /decimal/i })).toBeNull();
    expect(view.getByRole("option", { name: /number/i })).toBeTruthy();
  });
  it("omits the redundant JSON parser label while retaining its fields", () => {
    const node: CompiledNode = {
      kind: "object", xUi: {}, required: new Set(),
      properties: {
        common: {
          kind: "object", xUi: { widget: "parser_common" },
          required: new Set(), properties: {},
        },
        json_parser: {
          kind: "object", title: "JSON parser", xUi: {},
          required: new Set(),
          properties: { json_framing: stringNode("JSON framing") },
        },
      },
    };
    const view = render(<SchemaForm node={node} value={{}} onChange={() => undefined} />);
    expect(view.queryByText("JSON parser", { exact: true })).toBeNull();
    expect(view.getByText("JSON framing", { exact: true })).toBeTruthy();
    expect(view.getByText("Parser settings", { exact: true })).toBeTruthy();
  });
  it("supports connector-specific field labels without changing shared schemas", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["preserve_key"]),
      properties: {
        preserve_key: {
          kind: "boolean",
          title: "Add message key",
          xUi: {},
        },
      },
    };
    const view = render(
      <SchemaForm
        node={node}
        value={{ preserve_key: true }}
        fieldLabelOverrides={{ preserve_key: "Add sourceID" }}
        onChange={() => undefined}
      />,
    );
    expect(view.getByText("Add sourceID")).toBeTruthy();
    expect(view.queryByText("Add message key")).toBeNull();
  });

  it("keeps parse and unknown-field policies on one responsive row", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(),
      properties: {
        conversion_error: stringNode("On Parse Error"),
        unknown_fields: stringNode("On Unknown Field"),
      },
    };
    const view = render(
      <SchemaForm
        node={node}
        value={{ conversion_error: "fail", unknown_fields: "fail" }}
        onChange={() => undefined}
      />,
    );
    const row = view.container.querySelector(".parse-policy-row");
    expect(row).not.toBeNull();
    expect(row?.querySelectorAll(":scope > .form-row")).toHaveLength(2);
    expect(row?.textContent).toContain("On Parse Error");
    expect(row?.textContent).toContain("On Unknown Field");
  });

  it("keeps editable unions wide and protects their numeric fields from autofill", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["amount"]),
      properties: {
        amount: {
          kind: "union",
          title: "Amount",
          xUi: { control_width: "wide" },
          branches: [
            {
              label: "Rows",
              requiredKeys: ["type", "row_count"],
              discriminator: { key: "type", value: "rows" },
              node: {
                kind: "object",
                xUi: {},
                required: new Set(["type", "row_count"]),
                properties: {
                  type: {
                    kind: "string",
                    xUi: {},
                    enumValues: ["rows"],
                  },
                  row_count: {
                    kind: "number",
                    title: "Row count",
                    xUi: { widget: "grouped_integer" },
                    integer: true,
                    minimum: 1,
                  },
                },
              },
            },
          ],
        },
      },
    };

    const view = render(
      <SchemaForm
        node={node}
        value={{ amount: { type: "rows", row_count: 50_000_000 } }}
        onChange={() => undefined}
      />,
    );
    const amountRow = view.container.querySelector(".control-width-wide");
    expect(amountRow).not.toBeNull();
    expect(amountRow?.classList.contains("form-row-wide")).toBe(false);
    expect(amountRow?.classList.contains("control-width-enum")).toBe(false);
    const rowCount = within(amountRow as HTMLElement).getByLabelText(
      "Row count",
    ) as HTMLInputElement;
    expect(rowCount.type).toBe("text");
    expect(rowCount.inputMode).toBe("numeric");
    expect(rowCount.value).toBe("50 000 000");
    expect(rowCount.autocomplete).toBe("none");
    expect(rowCount.name).toMatch(/^tf-/);
    expect(rowCount.getAttribute("data-form-type")).toBe("other");

    fireEvent.focus(rowCount);
    expect(rowCount.value).toBe("50 000 000");
    fireEvent.input(rowCount, { target: { value: "500000001" } });
    expect(rowCount.value).toBe("500 000 001");
  });

  it("keeps routing unions wide instead of applying the enum width", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["topic"]),
      properties: {
        topic: {
          kind: "union",
          title: "Topic",
          xUi: { control_width: "routing" },
          branches: [
            {
              label: "Topic",
              requiredKeys: ["type", "topic"],
              discriminator: { key: "type", value: "topic" },
              node: {
                kind: "object",
                xUi: {},
                required: new Set(["type", "topic"]),
                properties: {
                  type: {
                    kind: "string",
                    xUi: {},
                    enumValues: ["topic"],
                  },
                  topic: stringNode("Topic"),
                },
              },
            },
          ],
        },
      },
    };

    const view = render(
      <SchemaForm
        node={node}
        value={{ topic: { type: "topic", topic: "" } }}
        onChange={() => undefined}
      />,
    );
    const routingRow = view.container.querySelector(".control-width-routing");
    expect(routingRow).not.toBeNull();
    expect(routingRow?.classList.contains("control-width-enum")).toBe(false);
  });

  it("guides the first required parser setting after selecting a parser", async () => {
    const fromConfig: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(["type", "name"]),
      properties: {
        type: { kind: "string", xUi: {}, enumValues: ["from_config"] },
        name: stringNode("Name"),
      },
    };
    const parserContainer: CompiledNode = {
      kind: "object",
      xUi: { widget: "json_parser" },
      required: new Set(["common", "json_parser"]),
      properties: {
        common: {
          kind: "object",
          xUi: { widget: "parser_common" },
          required: new Set(["table_naming"]),
          properties: {
            table_naming: {
              kind: "union",
              title: "Table name",
              xUi: { control_width: "table_name" },
              branches: [
                {
                  label: "From config",
                  requiredKeys: ["type", "name"],
                  discriminator: { key: "type", value: "from_config" },
                  node: fromConfig,
                },
                {
                  label: "From topic name",
                  requiredKeys: ["type"],
                  discriminator: { key: "type", value: "from_topic_name" },
                  node: {
                    kind: "object",
                    xUi: {},
                    required: new Set(["type"]),
                    properties: {
                      type: {
                        kind: "string",
                        xUi: {},
                        enumValues: ["from_topic_name"],
                      },
                    },
                  },
                },
              ],
            },
          },
        },
        json_parser: {
          kind: "object",
          xUi: {},
          required: new Set(["columns"]),
          properties: {
            columns: {
              kind: "array",
              xUi: { widget: "column_mappings", initial_items: 1 },
              minItems: 1,
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
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
      configurable: true,
      value: vi.fn(),
    });

    function Harness() {
      const root = useRef<HTMLDivElement>(null);
      const [value, setValue] = useState<JsonValue>({ parser: null });
      return (
        <div ref={root} class="route-composition">
          <RequiredFieldGuide root={root} enabled revision={value} />
          <div class="required-incomplete">
            <input aria-label="Earlier required field" />
          </div>
          <SchemaForm
            node={node}
            value={value}
            variantUi={{
              selectionOnly: ["parser"],
              onSelected: () => revealDetails(".parser-details-card"),
            }}
            onChange={setValue}
          />
          <ParserDetailsForm node={node} value={value} onChange={setValue} />
        </div>
      );
    }

    const view = render(<Harness />);
    fireEvent.click(view.getByRole("button", { name: "Parser" }));
    fireEvent.click(view.getByRole("option", { name: "JSON parser" }));

    const tableName = await view.findByText("Table name");
    await waitFor(() =>
      expect(
        tableName.closest(".form-row")?.classList.contains("required-next"),
      ).toBe(true),
    );
    expect(
      view
        .getByLabelText("Earlier required field")
        .classList.contains("required-next-control"),
    ).toBe(false);
    expect(
      view.container
        .querySelector(".route-composition")
        ?.classList.contains("route-selection-flash"),
    ).toBe(false);
  });

  it("disables implicit browser autofill for ordinary and secret fields", () => {
    const node: CompiledNode = {
      kind: "object",
      required: new Set(),
      additionalProperties: false,
      xUi: {},
      properties: {
        consumer_name: { kind: "string", xUi: {} },
        token: { kind: "string", xUi: { widget: "password" } },
      },
    };
    const view = render(
      <SchemaForm
        node={node}
        value={{ consumer_name: "consumer", token: "secret" }}
        onChange={() => undefined}
      />,
    );

    const consumer = view.container.querySelector<HTMLInputElement>(
      "#field---consumer_name",
    )!;
    const token =
      view.container.querySelector<HTMLInputElement>("#field---token")!;
    expect(consumer.autocomplete).toBe("none");
    expect(consumer.name).toMatch(/^tf-/);
    expect(consumer.name).not.toContain("consumer_name");
    expect(token.autocomplete).toBe("none");
    expect(token.name).toMatch(/^tf-/);
    expect(token.name).not.toContain("token");
  });

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
      "https://console.example/topics/account/topic%20name",
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

  it("builds dependency-aware external links and hides them until dependencies exist", () => {
    const node: CompiledNode = {
      kind: "object",
      required: new Set(["installation", "path"]),
      additionalProperties: false,
      xUi: {},
      properties: {
        installation: {
          kind: "object",
          required: new Set(["cluster"]),
          additionalProperties: false,
          xUi: {},
          properties: {
            cluster: { kind: "string", xUi: {} },
          },
        },
        path: {
          kind: "string",
          xUi: {
            external_link_template:
              "https://storage.example/{cluster}/navigation?path=//{value}",
            external_link_dependencies: {
              cluster: "/installation/cluster",
            },
          },
        },
      },
    };
    const view = render(
      <SchemaForm
        node={node}
        value={{
          installation: { cluster: "test-cluster" },
          path: "//home/example/benchmarks/",
        }}
        onChange={() => undefined}
      />,
    );
    expect(
      view
        .getByRole("link", { name: "Open in external console" })
        .getAttribute("href"),
    ).toBe(
      "https://storage.example/test-cluster/navigation?path=//home/example/benchmarks/",
    );

    view.rerender(
      <SchemaForm
        node={node}
        value={{ installation: { cluster: "" }, path: "//home/table" }}
        onChange={() => undefined}
      />,
    );
    expect(view.queryByRole("link", { name: "Open in external console" })).toBeNull();
  });

  it("keeps focus when an external-console link appears after typing", () => {
    const node: CompiledNode = {
      kind: "string",
      xUi: {
        external_link_template: "https://console.example/topics/{value}",
      },
    };
    function Harness() {
      const [value, setValue] = useState<JsonValue>("");
      return <SchemaForm node={node} value={value} onChange={setValue} />;
    }
    const view = render(<Harness />);
    const input = view.getByRole("textbox") as HTMLInputElement;
    input.focus();

    fireEvent.input(input, { target: { value: "c" } });

    expect(document.activeElement).toBe(input);
    expect(
      view.getByRole("link", { name: "Open in external console" }),
    ).toBeTruthy();
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
      required: new Set(["name", "mode", "secret", "descriptor", "hosts"]),
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
        descriptor: {
          kind: "string",
          title: "Descriptor",
          xUi: { widget: "textarea" },
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
        value={{
          name: "",
          mode: "",
          secret: "",
          descriptor: "",
          hosts: [""],
        }}
        onChange={() => undefined}
      />,
    );

    expect(view.getByLabelText("Name").getAttribute("id")).toBe("field---name");
    expect(view.getByLabelText("Mode").tagName).toBe("BUTTON");
    expect(view.getByLabelText("Secret").getAttribute("type")).toBe("password");
    expect(view.getByLabelText("Descriptor").tagName).toBe("TEXTAREA");
    expect(view.getByLabelText("Descriptor").getAttribute("autocomplete")).toBe("none");
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
    fireEvent.click(view.getByRole("button", { name: "MiB" }));
    expect(view.queryByText("B")).toBeNull();
    expect(view.queryByText("KiB")).toBeNull();
    expect(view.getAllByText("MiB")).toHaveLength(2);
    expect(view.getByText("GiB")).toBeTruthy();
    view.unmount();
  });

  it("edits optional long durations with explicit calendar-scale units", () => {
    const onChange = vi.fn();
    const view = render(
      <SchemaForm
        node={{
          kind: "nullable",
          inner: { kind: "number", integer: true, xUi: {} },
          xUi: { widget: "duration_scale" },
        }}
        value={null}
        onChange={onChange}
      />,
    );

    fireEvent.input(view.getByRole("spinbutton"), {
      target: { value: "2" },
    });
    expect(onChange).toHaveBeenLastCalledWith(120_000);
    fireEvent.click(view.getByRole("button", { name: "Minutes" }));
    fireEvent.click(view.getByText("Years"));
    fireEvent.input(view.getByRole("spinbutton"), {
      target: { value: "1" },
    });
    expect(onChange).toHaveBeenLastCalledWith(31_536_000_000);
    view.unmount();
  });

  it("can render deferred variant details flush with their parent form", () => {
    const view = render(
      <SchemaForm
        node={{
          kind: "object",
          properties: {
            tables: {
              kind: "union",
              branches: [
                {
                  label: "Dynamic tables",
                  discriminator: { key: "type", value: "dynamic" },
                  node: {
                    kind: "object",
                    properties: {
                      path: { kind: "string", title: "Path", xUi: {} },
                    },
                    required: new Set(["path"]),
                    xUi: {},
                  },
                },
              ],
              xUi: {
                defer_variant_details: true,
                indent_variant_details: false,
              },
            },
          },
          required: new Set(["tables"]),
          xUi: {},
        }}
        value={{ tables: { type: "dynamic", path: "//tmp/output" } }}
        onChange={() => undefined}
      />,
    );

    const details = view.container.querySelector(".deferred-variant-details");
    expect(details).toBeTruthy();
    expect(details?.classList.contains("nested-section")).toBe(false);
    expect(view.getByDisplayValue("//tmp/output")).toBeTruthy();
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
    ).toContain("Not selected");

    fireEvent.pointerDown(trigger, { button: 0, clientX: 219 });
    fireEvent.pointerDown(trigger, { button: 0, clientX: 1 });
    expect(form.getByRole("option", { name: "String" })).toBeTruthy();
    expect(form.getByRole("option", { name: "Integer" })).toBeTruthy();
  });

  it("makes every select searchable by default", () => {
    const view = render(
      <SelectControl
        value=""
        placeholder="Arrow type"
        options={[
          { value: "Utf8", label: "Utf8" },
          { value: "Int64", label: "Int64" },
        ]}
        onChange={() => undefined}
      />,
    );

    fireEvent.pointerDown(view.getByRole("button", { name: "Not selected" }), {
      button: 0,
    });
    const search = view.getByRole("searchbox") as HTMLInputElement;
    expect(search.autocomplete).toBe("none");
    fireEvent.input(search, { target: { value: "int" } });
    expect(view.queryByRole("option", { name: "Utf8" })).toBeNull();
    expect(view.getByRole("option", { name: "Int64" })).toBeTruthy();
  });

  it("lets a selected dropdown return to Not selected", () => {
    const onChange = vi.fn();
    const view = render(
      <SelectControl
        value="json"
        placeholder="Not selected"
        options={[{ value: "json", label: "JSON" }]}
        onChange={onChange}
      />,
    );

    fireEvent.pointerDown(view.getByRole("button", { name: "JSON" }), {
      button: 0,
    });
    const options = view.getAllByRole("option");
    expect(options[0]!.textContent).toBe("Not selected");
    fireEvent.pointerDown(options[0]!, { button: 0 });

    expect(onChange).toHaveBeenCalledWith("");
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

    const triggers = view.getAllByRole("button", { name: "Not selected" });
    fireEvent.pointerDown(triggers[0]!);
    expect(view.getByRole("option", { name: "First option" })).toBeTruthy();

    fireEvent.pointerDown(triggers[1]!);
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

    expect(
      form
        .getByRole("button", { name: "Not selected" })
        .querySelector(".spinner"),
    ).toBeTruthy();
    expect((await form.findByRole("status")).textContent).toContain("Loading…");
    expect(form.queryByText("No matches")).toBeNull();
    pending.resolve({ options: [{ value: "cluster", label: "Cluster" }] });
    expect(await form.findByRole("option", { name: "Cluster" })).toBeTruthy();
    view.unmount();
    options.mockRestore();
  });

  it("keeps a stable status slot when dynamic option loading fails", async () => {
    const options = vi
      .spyOn(api, "options")
      .mockRejectedValue(
        new Error(
          "No ready managed-kafka clusters were found in the configured folder.",
        ),
      );
    const view = render(
      <SchemaForm
        node={{ kind: "string", xUi: { dynamic_options: "clusters" } }}
        value=""
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    const slot = view.container.querySelector(".dynamic-select-status");
    expect(slot).toBeTruthy();
    expect(slot?.textContent).toBe("");

    fireEvent.pointerDown(form.getByRole("button", { name: "Not selected" }));

    const alert = await form.findByRole("alert");
    expect(alert).toBe(slot);
    expect(alert.textContent).toContain("No ready managed-kafka clusters");
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
      form.getByRole("option", { name: "Not selected" }),
    );
    fireEvent.keyDown(document.activeElement!, { key: "ArrowDown" });
    expect(document.activeElement).toBe(
      form.getByRole("option", { name: "String" }),
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

  it("groups Parquet-only controls under their dedicated disclosure", () => {
    const node: CompiledNode = {
      kind: "object",
      xUi: {},
      required: new Set(),
      properties: {
        compression: {
          ...stringNode("Compression"),
          xUi: { section: "advanced_parquet" },
        },
      },
    };
    const view = render(
      <SchemaForm
        node={node}
        value={{ compression: "zstd" }}
        onChange={() => undefined}
      />,
    );

    const details = view.getByText("Advanced Parquet settings").closest("details")!;
    expect(details.open).toBe(false);
    expect(details.classList.contains("advanced-parquet-settings")).toBe(true);
    fireEvent.click(view.getByText("Advanced Parquet settings"));
    expect(details.open).toBe(true);
    expect(view.container.querySelector("#field---compression")).toBeTruthy();
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

    expect(container.querySelector(".column-keys")).toBeNull();
    expect(getByRole("columnheader", { name: "Key", exact: true })).toBeTruthy();
    const id = getByRole("checkbox", { name: "Key id", exact: true }) as HTMLInputElement;
    const offset = getByRole("checkbox", { name: "Key source_offset", exact: true }) as HTMLInputElement;
    expect(id.getAttribute("autocomplete")).toBe("none");
    fireEvent.click(id);
    fireEvent.click(offset);
    expect(id.checked).toBe(true);
    expect(offset.checked).toBe(true);
    fireEvent.click(id);
    expect(id.checked).toBe(false);
    expect(offset.checked).toBe(true);
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
              minItems: 1,
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
        variantUi={{
          selectionOnly: ["parser"],
          actions: {
            parser: <button aria-label="Preview one message">eye</button>,
          },
        }}
        connectionAction={<button>Check connection</button>}
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
    expect(details.container.textContent).not.toContain("JSON path");
    expect(details.container.textContent).not.toContain("JSON type");
    expect(
      details.container.querySelector(".source-parser-bridge"),
    ).not.toBeNull();

    const incompleteValue = {
      parser: {
        common: { table_naming: "events" },
        json_parser: { columns: [{ column_name: "" }, { column_name: "" }] },
      },
    };
    const incompleteEndpoint = render(
      <SchemaForm
        node={node}
        value={incompleteValue}
        variantUi={{ selectionOnly: ["parser"] }}
        showRequiredErrors
        onChange={() => undefined}
      />,
    );
    expect(
      incompleteEndpoint.container.querySelector(".required-missing"),
    ).toBeNull();

    const incompleteDetails = render(
      <ParserDetailsForm
        node={node}
        value={incompleteValue}
        showRequiredErrors
        onChange={() => undefined}
      />,
    );
    expect(
      incompleteDetails.container.querySelector(
        ".column-table tr.required-incomplete",
      ),
    ).not.toBeNull();
    expect(
      incompleteDetails.container.querySelector(
        ".column-table td.required-missing",
      ),
    ).toBeNull();

    const emptyDetails = render(
      <ParserDetailsForm
        node={node}
        value={{
          parser: {
            common: { table_naming: "events" },
            json_parser: { columns: [] },
          },
        }}
        showRequiredErrors
        onChange={() => undefined}
      />,
    );
    expect(
      emptyDetails.container
        .querySelector<HTMLButtonElement>(".add-row-button")
        ?.closest(".column-editor")
        ?.classList.contains("required-incomplete"),
    ).toBe(true);
  });

  it("removes parser configuration when parser selection is cleared", () => {
    const onChange = vi.fn();
    const parser: CompiledNode = {
      kind: "union",
      xUi: { widget: "parser" },
      branches: [
        {
          label: "JSON parser",
          requiredKeys: ["json_parser"],
          node: {
            kind: "object",
            xUi: {},
            required: new Set(["json_parser"]),
            properties: {
              json_parser: {
                kind: "object",
                xUi: {},
                required: new Set(),
                properties: {},
              },
            },
          },
        },
      ],
    };
    const view = render(
      <SchemaForm
        node={{
          kind: "object",
          xUi: {},
          required: new Set(["parser"]),
          properties: { parser },
        }}
        value={{ parser: { json_parser: {} } }}
        onChange={onChange}
      />,
    );

    fireEvent.pointerDown(view.getByRole("button", { name: "Parser" }), {
      button: 0,
    });
    fireEvent.pointerDown(view.getByRole("option", { name: "Not selected" }), {
      button: 0,
    });

    expect(onChange).toHaveBeenCalledWith({ parser: null });
  });

  it("renders serializer settings directly below its stable selector", () => {
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
    function Harness() {
      const [value, setValue] = useState<JsonValue>({ serializer: { type: "json" } });
      return <SchemaForm node={node} value={value} onChange={setValue} />;
    }
    const view = render(<Harness />);
    const trigger = view.container.querySelector(".select-trigger")!;
    fireEvent.click(trigger);
    fireEvent.click(view.getByRole("option", { name: "Schema Registry" }));
    const input = view.getByRole("textbox", { name: "Registry URL" });
    const field = trigger.closest(".serializer-inline-settings")!;
    expect(field.contains(input)).toBe(true);
    expect(trigger.compareDocumentPosition(input) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(view.container.querySelector(".serializer-details-card")).toBeNull();
    expect(view.container.querySelector(".sink-serializer-bridge")).toBeNull();
    input.focus();
    fireEvent.input(input, { target: { value: "https://registry" } });
    expect(document.activeElement).toBe(input);
    expect(view.getByDisplayValue("https://registry")).toBe(input);
    expect(view.container.querySelector(".select-trigger")).toBe(trigger);
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
    expect(container.querySelectorAll("th")).toHaveLength(6);
    expect(container.querySelector("thead")?.textContent).not.toContain(
      "JSON type",
    );
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

  it("adds output columns with explicit string and Utf8 defaults", () => {
    const onChange = vi.fn();
    const stringEnum = (values: string[]): CompiledNode => ({
      kind: "string",
      enumValues: values,
      xUi: {},
    });
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
            required: new Set(),
            properties: {
              column_name: stringNode(),
              jsonpath: stringNode(),
              json_data_type: stringEnum(["string", "number", "boolean"]),
              arrow_type: stringEnum(["Utf8", "Int64", "Boolean"]),
            },
          },
        },
      },
    };
    const view = render(
      <SchemaForm node={node} value={{ columns: [] }} onChange={onChange} />,
    );

    fireEvent.click(view.getByRole("button", { name: "+ Add column" }));

    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        columns: [
          expect.objectContaining({
            json_data_type: "string",
            arrow_type: "Utf8",
          }),
        ],
      }),
    );
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

  it("sets and clears not null for every output column from the header", () => {
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
            required: new Set(["column_name", "nullable"]),
            properties: {
              column_name: stringNode(),
              nullable: { kind: "boolean", xUi: {} },
            },
          },
        },
      },
    };
    function Harness() {
      const [value, setValue] = useState<JsonValue>({
        columns: [
          { column_name: "id", nullable: false },
          { column_name: "value", nullable: true },
        ],
      });
      return (
        <>
          <SchemaForm node={node} value={value} onChange={setValue} />
          <output data-testid="config-value">{JSON.stringify(value)}</output>
        </>
      );
    }
    const view = render(<Harness />);
    const form = within(view.container as HTMLElement);
    const toggle = form.getByRole("checkbox", {
      name: "Set not null for all output columns",
    }) as HTMLInputElement;
    expect(toggle.indeterminate).toBe(true);
    fireEvent.click(toggle);
    expect(form.getByTestId("config-value").textContent).toContain(
      '"nullable":false',
    );
    expect(toggle.checked).toBe(true);
    fireEvent.click(toggle);
    expect(form.getByTestId("config-value").textContent).toBe(
      JSON.stringify({
        columns: [
          { column_name: "id", nullable: true },
          { column_name: "value", nullable: true },
        ],
        keys: [],
      }),
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
