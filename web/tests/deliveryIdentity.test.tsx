// @vitest-environment jsdom
import { cleanup, fireEvent } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, expect, it, vi } from "vitest";
import catalogFixture from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { decodeApi } from "../src/api/contractDecoder";
import { DeliveryConfiguration } from "../src/delivery/DeliveryConfiguration";
import { render } from "./support/render";

afterEach(cleanup);
const catalog = decodeApi("catalog_response", catalogFixture, "catalog");

function IdentityForm({ description = "", readOnly = false, revision = 0, onDescription = () => {} }: {
  description?: string; readOnly?: boolean; revision?: number; onDescription?: (value: string) => void;
}) {
  const [value, setValue] = useState(description);
  return <DeliveryConfiguration catalog={catalog} selection={undefined}
    editor={{ sessionId: "identity", editing: !readOnly, localRevision: revision, name: "pg-2-discard",
      description: value, config: {}, validation: { state: "draft" }, runtime: { state: "stopped" } }}
    readOnly={readOnly} requiredErrorScope="none" onName={() => {}} onConfig={() => {}} onChooseEndpoint={() => {}}
    onDescription={next => { setValue(next); onDescription(next); }} />;
}

it("puts delivery identity fields in the same compact left column as endpoint forms", () => {
  const view = render(<IdentityForm />);
  const column = view.container.querySelector(".identity-card > .island-form.identity-form")!;
  expect(column).not.toBeNull();
  expect(column.querySelectorAll(".top-field")).toHaveLength(3);
  expect(column.contains(view.getByRole("textbox", { name: "Delivery name" }))).toBe(true);
  expect(column.contains(view.getByRole("textbox", { name: /Description/ }))).toBe(true);
  expect(column.querySelector(".island-form")).toBeNull();
});

it("uses an autofill-resistant multiline description starting at one row", () => {
  const view = render(<IdentityForm />);
  const field = view.getByRole("textbox", { name: /Description/ }) as HTMLTextAreaElement;
  expect(field.tagName).toBe("TEXTAREA");
  expect(field.rows).toBe(1);
  expect(field.getAttribute("autocomplete")).toBe("none");
  expect(field.getAttribute("name")).toMatch(/^tf-/);
  expect(field.getAttribute("data-1p-ignore")).toBe("true");
  expect(field.getAttribute("data-lpignore")).toBe("true");
  field.focus();
  expect(fireEvent.keyDown(field, { key: "Enter" })).toBe(true);
  expect(document.activeElement).toBe(field);
});

it("updates the sizing mirror synchronously for wrapping, newlines and deletion without changing user text", () => {
  const onDescription = vi.fn();
  const view = render(<IdentityForm onDescription={onDescription} />);
  const field = view.getByRole("textbox", { name: /Description/ }) as HTMLTextAreaElement;
  const mirror = field.parentElement!.querySelector("[aria-hidden='true']")!;
  expect(mirror).not.toBeNull();
  field.focus();
  for (const value of ["Long description ".repeat(30), "x".repeat(400), "  First line\n\nВторая строка 🙂\n", "Short", ""]) {
    fireEvent.input(field, { target: { value } });
    expect(field.value).toBe(value);
    expect(onDescription).toHaveBeenLastCalledWith(value);
    // The trailing measuring space preserves the height of a final empty line;
    // it must never become part of the actual description.
    expect(mirror.textContent).toBe(`${value} `);
    expect(view.getByRole("textbox", { name: /Description/ })).toBe(field);
    expect(document.activeElement).toBe(field);
  }
});

it("keeps identity controls, caret and sizing content unchanged on unrelated editor updates", () => {
  const view = render(<IdentityForm description="First line\nSecond line" />);
  const name = view.getByRole("textbox", { name: "Delivery name" });
  const field = view.getByRole("textbox", { name: /Description/ }) as HTMLTextAreaElement;
  const type = view.container.querySelector(".identity-form .select")!;
  const mirror = field.parentElement!.querySelector("[aria-hidden='true']")!;
  field.focus();
  field.setSelectionRange(4, 8);
  view.rerender(<IdentityForm description="First line\nSecond line" revision={1} />);
  expect(view.getByRole("textbox", { name: "Delivery name" })).toBe(name);
  expect(view.getByRole("textbox", { name: /Description/ })).toBe(field);
  expect(view.container.querySelector(".identity-form .select")).toBe(type);
  expect(field.parentElement!.querySelector("[aria-hidden='true']")).toBe(mirror);
  expect(mirror.textContent).toBe("First line\nSecond line ");
  expect(document.activeElement).toBe(field);
  expect([field.selectionStart, field.selectionEnd]).toEqual([4, 8]);
});

it("renders saved multiline descriptions at content height while keeping read-only fields disabled", () => {
  const description = "  Saved description\nWith another line\n";
  const view = render(<IdentityForm description={description} readOnly />);
  const field = view.getByRole("textbox", { name: /Description/ }) as HTMLTextAreaElement;
  expect(field.disabled).toBe(true);
  expect(field.value).toBe(description);
  expect(field.parentElement!.querySelector("[aria-hidden='true']")!.textContent).toBe(`${description} `);
  expect((view.getByRole("textbox", { name: "Delivery name" }) as HTMLInputElement).disabled).toBe(true);
});
