// @vitest-environment jsdom

import { cleanup, fireEvent, render, within } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { FormField } from "../src/ui/FormField";
import { Button } from "../src/ui/Button";
import { Disclosure } from "../src/ui/Disclosure";
import { MultiSelectControl, SelectControl } from "../src/ui/SelectControl";

afterEach(cleanup);

describe("UI primitives", () => {
  it("uses the native disclosure hit area for the entire summary", () => {
    const view = render(
      <Disclosure
        label={<span data-testid="summary-edge">Advanced settings</span>}
      >
        <p>Advanced value</p>
      </Disclosure>,
    );
    const details = view.container.querySelector("details")!;

    fireEvent.click(view.getByTestId("summary-edge"), { detail: 1 });

    expect(details.open).toBe(true);
  });

  it("gives pending actions immediate feedback without changing their label", () => {
    const onClick = vi.fn();
    const view = render(
      <Button pending onClick={onClick}>
        Save
      </Button>,
    );
    const button = view.getByRole("button", { name: "Save" });

    expect(button.getAttribute("aria-busy")).toBe("true");
    expect(button.getAttribute("aria-disabled")).toBe("true");
    expect((button as HTMLButtonElement).disabled).toBe(false);
    expect(button.classList.contains("interaction-pending")).toBe(true);
    expect(button.textContent).toBe("Save");
    fireEvent.click(button);
    expect(onClick).not.toHaveBeenCalled();
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

  it("releases focus after selecting a single value", () => {
    const view = render(
      <SelectControl
        value=""
        placeholder="Not selected"
        options={[{ value: "one", label: "One" }]}
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    const trigger = form.getByRole("button", { name: "Not selected" });
    fireEvent.pointerDown(trigger, { button: 0 });
    expect(document.activeElement).toBe(trigger);

    fireEvent.pointerDown(form.getByRole("option", { name: "One" }), {
      button: 0,
    });

    expect(document.activeElement).not.toBe(trigger);
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

  it("ranks prefix, substring, and subsequence matches for dc and вс", () => {
    const view = render(
      <SelectControl
        value=""
        placeholder="Not selected"
        options={[
          { value: "adbc", label: "adbc" },
          { value: "dcb", label: "dcb" },
          { value: "adcb", label: "adcb" },
          { value: "dca", label: "dca" },
        ]}
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    fireEvent.pointerDown(form.getByRole("button", { name: "Not selected" }), {
      button: 0,
    });

    fireEvent.input(form.getByRole("searchbox"), {
      target: { value: "dc" },
    });

    expect(
      form.getAllByRole("option").map((option) => option.textContent),
    ).toEqual(["Not selected", "dca", "dcb", "adcb", "adbc"]);

    fireEvent.input(form.getByRole("searchbox"), {
      target: { value: "вс" },
    });
    expect(
      form.getAllByRole("option").map((option) => option.textContent),
    ).toEqual(["Not selected", "dca", "dcb", "adcb", "adbc"]);
  });

  it("always keeps Not selected as the first option", () => {
    const view = render(
      <SelectControl
        value=""
        placeholder="Not selected"
        options={[
          { value: "stream", label: "Stream" },
          { value: "batch", label: "Batch" },
        ]}
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    fireEvent.pointerDown(form.getByRole("button", { name: "Not selected" }), {
      button: 0,
    });

    expect(
      form.getAllByRole("option").map((option) => option.textContent),
    ).toEqual(["Not selected", "Stream", "Batch"]);

    fireEvent.input(form.getByRole("searchbox"), {
      target: { value: "str" },
    });
    expect(
      form.getAllByRole("option").map((option) => option.textContent),
    ).toEqual(["Not selected", "Stream"]);
  });

  it("always keeps Not selected first in multi-selects", () => {
    const view = render(
      <MultiSelectControl
        values={[]}
        placeholder="Not selected"
        options={[
          { value: "stream", label: "Stream" },
          { value: "batch", label: "Batch" },
        ]}
        disabled={false}
        onChange={() => undefined}
      />,
    );
    const form = within(view.container as HTMLElement);
    fireEvent.pointerDown(form.getByRole("button", { name: "Not selected" }), {
      button: 0,
    });
    fireEvent.input(form.getByRole("searchbox"), {
      target: { value: "str" },
    });

    expect(
      form.getAllByRole("option").map((option) => option.textContent),
    ).toEqual(["✓Not selected", "Stream"]);
  });
});
