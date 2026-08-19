// @vitest-environment jsdom

import { cleanup, fireEvent, render, within } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";

import { FormField } from "../src/ui/FormField";
import { Button } from "../src/ui/Button";
import { MultiSelectControl, SelectControl } from "../src/ui/SelectControl";

afterEach(cleanup);

describe("UI primitives", () => {
  it("gives pending actions immediate feedback without changing their label", () => {
    const view = render(<Button pending>Save</Button>);
    const button = view.getByRole("button", { name: "Save" });

    expect(button.getAttribute("aria-busy")).toBe("true");
    expect((button as HTMLButtonElement).disabled).toBe(true);
    expect(button.classList.contains("interaction-pending")).toBe(true);
    expect(button.textContent).toBe("Save");
  });

  it("exposes field help as an accessible tooltip", () => {
    const view = render(
      <FormField
        label="Native port"
        optional={false}
        description="ClickHouse native protocol port"
      >
        <input />
      </FormField>,
    );

    const help = view.getByText("?").closest(".help");
    const tooltip = view.getByRole("tooltip");
    expect(help?.getAttribute("aria-describedby")).toBe(tooltip.id);
    expect(tooltip.textContent).toBe("ClickHouse native protocol port");
  });

  it("navigates listbox options from the search input", () => {
    const view = render(
      <SelectControl
        value=""
        placeholder="Not selected"
        options={[
          { value: "one", label: "One" },
          { value: "two", label: "Two" },
        ]}
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    const trigger = form.getByRole("button", { name: "Not selected" });
    fireEvent.pointerDown(trigger, { button: 0 });
    const search = form.getByRole("searchbox");
    search.focus();

    fireEvent.keyDown(search, { key: "ArrowDown" });
    expect(document.activeElement).toBe(
      form.getByRole("option", { name: "Not selected" }),
    );
    fireEvent.keyDown(document.activeElement!, { key: "End" });
    expect(document.activeElement).toBe(
      form.getByRole("option", { name: "Two" }),
    );
    expect(trigger.getAttribute("aria-controls")).toBeTruthy();
  });

  it("shares keyboard dismissal and focus restoration across listboxes", () => {
    const view = render(
      <MultiSelectControl
        values={[]}
        placeholder="Not selected"
        options={[{ value: "one", label: "One" }]}
        disabled={false}
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    const trigger = form.getByRole("button", { name: "Not selected" });
    fireEvent.pointerDown(trigger, { button: 0 });
    const search = form.getByRole("searchbox");
    search.focus();

    fireEvent.keyDown(search, { key: "Escape" });

    expect(form.queryByRole("listbox")).toBeNull();
    expect(document.activeElement).toBe(trigger);
  });

  it("matches substrings and text typed with the Russian keyboard layout", () => {
    const view = render(
      <SelectControl
        value=""
        placeholder="Not selected"
        options={[{ value: "consumer", label: "timmy-test-consumer-00" }]}
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    fireEvent.pointerDown(form.getByRole("button", { name: "Not selected" }), {
      button: 0,
    });
    const search = form.getByRole("searchbox");

    fireEvent.input(search, { target: { value: "cons" } });
    expect(
      form.getByRole("option", { name: "timmy-test-consumer-00" }),
    ).toBeTruthy();

    fireEvent.input(search, { target: { value: "сщты" } });
    expect(
      form.getByRole("option", { name: "timmy-test-consumer-00" }),
    ).toBeTruthy();
  });
});
