// @vitest-environment jsdom

import {
  act,
  cleanup,
  fireEvent,
  render,
  waitFor,
} from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { DynamicOptionsQuery } from "../src/application/ports/controlPlane";
import { DynamicPathControl } from "../src/schema/DynamicPathControl";
import { FormEnvironmentProvider } from "../src/schema/formEnvironment";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("dynamic path control", () => {
  it("queries the current hierarchy prefix and keeps directory navigation open", async () => {
    vi.useFakeTimers();
    const requests: DynamicOptionsQuery[] = [];
    const options = vi.fn(async (request: DynamicOptionsQuery) => {
      requests.push(request);
      return {
        options:
          request.query === "aaa/"
            ? [{ value: "aaa/bb/", label: "aaa/bb/" }]
            : [{ value: "aaa/", label: "aaa/" }],
      };
    });
    const view = render(<Harness options={options} initialValue="a" />);
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    expect(requests.at(-1)).toMatchObject({
      key: "endpoint.paths",
      query: "a",
      dependencies: { installation: "cluster-a" },
    });

    fireEvent.click(await view.findByRole("option", { name: "aaa/" }));
    expect(input).toHaveProperty("value", "aaa/");
    expect(input.getAttribute("aria-expanded")).toBe("true");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    await waitFor(() => expect(requests.at(-1)?.query).toBe("aaa/"));
    expect(await view.findByRole("option", { name: "aaa/bb/" })).toBeTruthy();
  });

  it("commits a leaf and closes the suggestion list", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({
      options: [{ value: "aaa/topic", label: "aaa/topic" }],
    }));
    const view = render(<Harness options={options} initialValue="aaa/t" />);
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    fireEvent.click(await view.findByRole("option", { name: "aaa/topic" }));

    expect(input).toHaveProperty("value", "aaa/topic");
    expect(input.getAttribute("aria-expanded")).toBe("false");
  });

  it("does not fetch or edit while read-only", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({ options: [] }));
    const view = render(
      <Harness options={options} initialValue="aaa/topic" disabled />,
    );
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    fireEvent.input(input, { target: { value: "changed" } });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });

    expect(options).not.toHaveBeenCalled();
    expect(input).toHaveProperty("disabled", true);
  });

  it("shows a prerequisite warning instead of claiming there are no paths", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({
      options: [],
      warning: "Select credentials to load path suggestions.",
    }));
    const view = render(<Harness options={options} initialValue="cdc" />);

    fireEvent.focus(view.getByRole("combobox"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });

    expect(
      await view.findByText("Select credentials to load path suggestions."),
    ).toBeTruthy();
    expect(view.queryByText("No matching paths")).toBeNull();
  });

  it("queries the backend with text corrected from the Russian keyboard layout", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({ options: [] }));
    const view = render(<Harness options={options} initialValue="свс" />);

    fireEvent.focus(view.getByRole("combobox"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });

    expect(options).toHaveBeenCalledWith(
      expect.objectContaining({ query: "cdc" }),
    );
  });
});

function Harness({
  options,
  initialValue,
  disabled = false,
}: {
  options: (request: DynamicOptionsQuery) => Promise<{
    options: Array<{ value: string; label: string }>;
  }>;
  initialValue: string;
  disabled?: boolean;
}) {
  const [value, setValue] = useState(initialValue);
  return (
    <FormEnvironmentProvider environment={{ options }}>
      <DynamicPathControl
        source="endpoint.paths"
        dependencies={{ installation: "cluster-a" }}
        value={value}
        disabled={disabled}
        onChange={setValue}
      />
    </FormEnvironmentProvider>
  );
}
