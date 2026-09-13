// @vitest-environment jsdom

import { cleanup, fireEvent, within } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, describe, expect, it } from "vitest";

import { SchemaForm } from "../src/schema/SchemaForm";
import { isComplete, type CompiledNode } from "../src/schema/compiler";
import type { JsonObject, JsonValue } from "../src/json";
import { render } from "./support/render";

afterEach(cleanup);

const string: CompiledNode = { kind: "string", xUi: {} };
const node: CompiledNode = {
  kind: "object", xUi: {}, required: new Set(["columns"]),
  properties: {
    columns: { kind: "array", xUi: { widget: "column_mappings" }, item: {
      kind: "object", xUi: {}, required: new Set(["column_name"]),
      properties: { column_name: string, arrow_type: { ...string, enumValues: ["Utf8"] } },
    } },
    keys: { kind: "array", xUi: { widget: "column_keys" }, item: string },
  },
};

function editor(names = ["", ""], keys: string[] = [], disabled = false) {
  let current: JsonValue = { columns: names.map(column_name => ({ column_name, arrow_type: "Utf8" })), keys };
  function Harness() {
    const [value, setValue] = useState(current);
    return <SchemaForm node={node} value={value} disabled={disabled} onChange={next => { current = next; setValue(next); }} />;
  }
  const view = render(<Harness />);
  const row = (index: number) => within(view.container.querySelectorAll<HTMLElement>(".config-table-row")[index]!);
  const key = (index: number) => row(index).getByRole("checkbox", { name: /^Key / }) as HTMLInputElement;
  const name = (index: number, value: string) => fireEvent.input(row(index).getByRole("textbox"), { target: { value } });
  const action = (index: number, label: string) => {
    fireEvent.click(row(index).getByRole("button", { name: /actions$/ }));
    fireEvent.click(view.getByRole("menuitem", { name: label, exact: true }));
  };
  return { ...view, key, name, action, config: () => current as JsonObject };
}

describe("output column keys", () => {
  it("allows Key before a name without selecting other unnamed rows or emitting an empty key", () => {
    const view = editor();
    expect(view.key(0).disabled).toBe(false);
    fireEvent.click(view.key(0));
    expect(view.key(0).checked).toBe(true);
    expect(view.key(1).checked).toBe(false);
    expect(view.config().keys).toEqual([]);
    expect(isComplete(node, view.config())).toBe(false);
    view.name(0, "id");
    expect(view.key(0).checked).toBe(true);
    expect(view.config().keys).toEqual(["id"]);
    view.name(1, "value");
    expect(view.key(1).checked).toBe(false);
    expect(isComplete(node, view.config())).toBe(true);
  });

  it("can uncheck an unnamed Key before entering the name", () => {
    const view = editor();
    fireEvent.click(view.key(0));
    fireEvent.click(view.key(0));
    view.name(0, "id");
    expect(view.key(0).checked).toBe(false);
    expect(view.config().keys).toEqual([]);
  });

  it("preserves Key while clearing and retyping the name", () => {
    const view = editor(["id"], ["id"]);
    view.name(0, "");
    expect(view.key(0).checked).toBe(true);
    expect(view.config().keys).toEqual([]);
    view.name(0, "renamed");
    expect(view.key(0).checked).toBe(true);
    expect(view.config().keys).toEqual(["renamed"]);
  });

  it("moves the unnamed Key with its row", () => {
    const view = editor();
    fireEvent.click(view.key(0));
    view.action(0, "Move down");
    expect(view.key(0).checked).toBe(false);
    expect(view.key(1).checked).toBe(true);
    view.name(1, "id");
    expect(view.config().keys).toEqual(["id"]);
  });

  it("duplicates an unnamed Key independently of the original row", () => {
    const view = editor([""]);
    fireEvent.click(view.key(0));
    view.action(0, "Duplicate");
    expect(view.key(1).checked).toBe(true);
    fireEvent.click(view.key(0));
    view.name(0, "original");
    view.name(1, "copy");
    expect(view.config().keys).toEqual(["copy"]);
  });

  it.each(["single", "bulk"])("does not transfer an unnamed Key to another row after %s deletion", mode => {
    const view = editor();
    fireEvent.click(view.key(0));
    if (mode === "single") view.action(0, "Delete");
    else {
      fireEvent.click(view.getByRole("checkbox", { name: "Select output column 1" }));
      fireEvent.click(view.getByRole("button", { name: "Delete 1 selected column" }));
    }
    fireEvent.click(view.getByRole("button", { name: "+ Add column" }));
    expect(view.key(0).checked).toBe(false);
    expect(view.key(1).checked).toBe(false);
    view.name(0, "remaining");
    view.name(1, "new");
    expect(view.config().keys).toEqual([]);
  });

  it("still disables Key when the editor is read-only", () => {
    const view = editor([""], [], true);
    expect(view.key(0).disabled).toBe(true);
  });
});
