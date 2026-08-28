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
    expect(input.getAttribute("autocomplete")).toBe("none");
    expect(input.getAttribute("data-form-type")).toBe("other");

    fireEvent.focus(input);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    expect(requests.at(-1)).toMatchObject({
      key: "endpoint.paths",
      query: "",
      dependencies: { installation: "cluster-a" },
    });

    fireEvent.click(await view.findByRole("option", { name: "aaa/" }));
    expect(input).toHaveProperty("value", "aaa/");
    expect(input.getAttribute("aria-expanded")).toBe("true");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    await waitFor(() => expect(requests.at(-1)?.query).toBe("aaa/"));
    expect(
      (await view.findByRole("option", { name: "aaa/bb/" })).querySelector(
        ".ytsaurus-folder-icon",
      ),
    ).toBeTruthy();
  });

  it("uses the standard spinner without changing the input layout", async () => {
    vi.useFakeTimers();
    let resolveOptions!: (value: {
      options: Array<{ value: string; label: string }>;
    }) => void;
    const pending = new Promise<{
      options: Array<{ value: string; label: string }>;
    }>((resolve) => {
      resolveOptions = resolve;
    });
    const view = render(
      <Harness options={() => pending} initialValue="//home/log" />,
    );

    fireEvent.focus(view.getByRole("combobox"));
    const slot = view.container.querySelector(".dynamic-path-spinner-slot");
    expect(slot).toBeTruthy();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    expect(slot?.querySelector(".spinner")).toBeTruthy();

    await act(async () => {
      resolveOptions({ options: [] });
      await pending;
    });
    expect(slot?.querySelector(".spinner")).toBeNull();
    expect(view.container.querySelector(".dynamic-path-spinner-slot")).toBe(
      slot,
    );
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
    const option = await view.findByRole("option", { name: "aaa/topic" });
    expect(option.querySelector(".ytsaurus-table-icon")).toBeTruthy();
    fireEvent.click(option);

    expect(input).toHaveProperty("value", "aaa/topic");
    expect(input.getAttribute("aria-expanded")).toBe("false");
  });

  it("uses Logbroker directory and topic glyphs for topic suggestions", async () => {
    vi.useFakeTimers();
    const view = render(
      <Harness
        source="yandex.logbroker.topics"
        options={async () => ({
          options: [
            {
              value: "cdc/prod/control-plane/",
              label: "cdc/prod/control-plane/",
            },
            { value: "cdc/prod/logs", label: "cdc/prod/logs" },
          ],
        })}
        initialValue="cdc/prod/"
      />,
    );

    fireEvent.focus(view.getByRole("combobox"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });

    expect(
      view.container.querySelector(
        ".dynamic-path-directory .logbroker-directory-icon",
      ),
    ).toBeTruthy();
    expect(
      (
        await view.findByRole("option", { name: "cdc/prod/control-plane/" })
      ).querySelector(".logbroker-directory-icon"),
    ).toBeTruthy();
    expect(
      (await view.findByRole("option", { name: "cdc/prod/logs" })).querySelector(
        ".logbroker-topic-icon",
      ),
    ).toBeTruthy();
  });

  it("uses the Logbroker consumer glyph for consumer suggestions", async () => {
    vi.useFakeTimers();
    const view = render(
      <Harness
        source="yandex.logbroker.consumers"
        options={async () => ({
          options: [
            {
              value: "cdc/prod/logfeller-important",
              label: "cdc/prod/logfeller-important",
            },
          ],
        })}
        initialValue="cdc/prod/"
      />,
    );

    fireEvent.focus(view.getByRole("combobox"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });

    expect(
      (await view.findByRole("option", {
        name: "cdc/prod/logfeller-important",
      })).querySelector(".logbroker-consumer-icon"),
    ).toBeTruthy();
  });

  it("keeps long path suggestions on one stable row and exposes the full label", async () => {
    vi.useFakeTimers();
    const longPath =
      "//home/logfeller/tmp/TM-10373/yt-read-throughput-direct-count-20260826/";
    const view = render(
      <Harness
        options={async () => ({
          options: [{ value: longPath, label: longPath }],
        })}
        initialValue="//home/logfeller/tmp/TM-10373/"
      />,
    );

    fireEvent.focus(view.getByRole("combobox"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });

    const option = await view.findByRole("option", { name: longPath });
    const label = option.querySelector(".dynamic-path-label");
    expect(label?.getAttribute("title")).toBe(longPath);
    expect(label?.textContent).toBe(
      "yt-read-throughput-direct-count-20260826/",
    );
    expect(label?.classList.contains("dynamic-path-label")).toBe(true);
    expect(
      view.container.querySelector(".dynamic-path-directory-path")?.textContent,
    ).toBe("//home/logfeller/tmp/TM-10373/");
  });

  it("navigates suggestions with arrows and accepts the active option with Tab", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({
      options: ["aaa/first", "aaa/second", "aaa/third"].map((value) => ({
        value,
        label: value,
      })),
    }));
    const view = render(<Harness options={options} initialValue="aaa/" />);
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    const suggestions = await view.findAllByRole("option");
    expect(suggestions.every((option) => option.getAttribute("tabindex") === "-1")).toBe(true);
    expect(suggestions[0]?.getAttribute("aria-selected")).toBe("true");

    fireEvent.keyDown(input, { key: "ArrowDown" });
    expect(suggestions[1]?.getAttribute("aria-selected")).toBe("true");
    fireEvent.keyDown(input, { key: "ArrowUp" });
    expect(suggestions[0]?.getAttribute("aria-selected")).toBe("true");

    await act(async () => {
      fireEvent.keyDown(input, { key: "Tab" });
      await Promise.resolve();
    });
    expect(input).toHaveProperty("value", "aaa/first");
    expect(input.getAttribute("aria-expanded")).toBe("false");
    expect(document.activeElement).toBe(input);
    expect(input).toHaveProperty("selectionStart", "aaa/first".length);
    expect(input).toHaveProperty("selectionEnd", "aaa/first".length);
  });

  it("highlights and accepts the first suggestion with Tab by default", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({
      options: ["aaa/first", "aaa/second"].map((value) => ({
        value,
        label: value,
      })),
    }));
    const view = render(<Harness options={options} initialValue="aaa/f" />);
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    expect(
      (await view.findAllByRole("option"))[0]?.getAttribute("aria-selected"),
    ).toBe("true");

    await act(async () => {
      fireEvent.keyDown(input, { key: "Tab" });
      await Promise.resolve();
    });
    expect(input).toHaveProperty("value", "aaa/first");
    expect(document.activeElement).toBe(input);
    expect(input).toHaveProperty("selectionEnd", "aaa/first".length);
  });

  it("accepts an arrow-selected suggestion with ArrowRight", async () => {
    vi.useFakeTimers();
    const view = render(
      <Harness
        options={async () => ({
          options: ["aaa/first", "aaa/second"].map((path) => ({
            value: path,
            label: path,
          })),
        })}
        initialValue="aaa/"
      />,
    );
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    fireEvent.keyDown(input, { key: "ArrowDown" });
    expect((await view.findAllByRole("option"))[1]?.getAttribute("aria-selected")).toBe(
      "true",
    );

    await act(async () => {
      fireEvent.keyDown(input, { key: "ArrowRight" });
      await Promise.resolve();
    });
    expect(input).toHaveProperty("value", "aaa/second");
    expect(document.activeElement).toBe(input);
    expect(input).toHaveProperty("selectionEnd", "aaa/second".length);
  });

  it("keeps ArrowRight as caret navigation before arrow-selecting a suggestion", async () => {
    vi.useFakeTimers();
    const view = render(
      <Harness
        options={async () => ({
          options: [{ value: "aaa/first", label: "aaa/first" }],
        })}
        initialValue="aaa/f"
      />,
    );
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    fireEvent.keyDown(input, { key: "ArrowRight" });

    expect(input).toHaveProperty("value", "aaa/f");
    expect(input.getAttribute("aria-expanded")).toBe("true");
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

  it("keeps empty and single-slash YTsaurus paths quiet until the root is complete", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({ options: [] }));
    const view = render(
      <Harness
        options={options}
        source="yandex.ytsaurus.tables"
        initialValue=""
      />,
    );
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(options).not.toHaveBeenCalled();
    expect(view.queryByRole("alert")).toBeNull();

    fireEvent.input(input, { target: { value: "/" } });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(options).not.toHaveBeenCalled();
    expect(view.queryByRole("alert")).toBeNull();

    fireEvent.input(input, { target: { value: "//" } });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    expect(options).toHaveBeenCalledWith(
      expect.objectContaining({ query: "//" }),
    );
  });

  it("adds the canonical YTsaurus root when typing or pasting a path", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({
      options: [{ value: "//home/", label: "//home/" }],
    }));
    const view = render(
      <Harness
        options={options}
        source="yandex.ytsaurus.tables"
        initialValue=""
      />,
    );
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    await act(async () => {
      fireEvent.input(input, { target: { value: "home" } });
      await Promise.resolve();
    });
    expect(input).toHaveProperty("value", "//home");
    expect(input).toHaveProperty("selectionEnd", "//home".length);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    expect(options).toHaveBeenLastCalledWith(
      expect.objectContaining({ query: "//" }),
    );

    await act(async () => {
      fireEvent.input(input, { target: { value: "/tmp" } });
      await Promise.resolve();
    });
    expect(input).toHaveProperty("value", "//tmp");

    fireEvent.input(input, { target: { value: "" } });
    expect(input).toHaveProperty("value", "");
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

  it("finds cdc for Russian вс and ranks prefix matches first", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({
      options: ["cdc/", "dcc_logbroker/", "dca/"].map((value) => ({
        value,
        label: value,
      })),
    }));
    const view = render(<Harness options={options} initialValue="вс" />);

    fireEvent.focus(view.getByRole("combobox"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });

    expect(options).toHaveBeenCalledWith(
      expect.objectContaining({ query: "" }),
    );
    expect(
      view
        .getAllByRole("option")
        .map(
          (option) =>
            option.querySelector(".dynamic-path-label")?.textContent,
        ),
    ).toEqual(["dca/", "dcc_logbroker/", "cdc/"]);
  });

  it("searches and highlights only inside the current directory", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async () => ({
      options: ["//home/logfeller/logforwarder/", "//home/logfeller/logs/"].map(
        (value) => ({ value, label: value }),
      ),
    }));
    const view = render(
      <Harness options={options} initialValue="//home/logfeller/log" />,
    );

    fireEvent.focus(view.getByRole("combobox"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });

    expect(options).toHaveBeenCalledWith(
      expect.objectContaining({ query: "//home/logfeller/" }),
    );
    for (const option of await view.findAllByRole("option")) {
      expect(option.querySelector(".dynamic-path-label")?.textContent).toMatch(
        /^log(?:forwarder|s)\/$/,
      );
      expect(
        [...option.querySelectorAll("strong")]
          .map((character) => character.textContent)
          .join(""),
      ).toBe("log");
    }
    expect(
      view.container.querySelector(".dynamic-path-directory-path")?.textContent,
    ).toBe("//home/logfeller/");
  });

  it("loads each directory once and filters subsequent input from its LRU cache", async () => {
    vi.useFakeTimers();
    const options = vi.fn(async (request: DynamicOptionsQuery) => ({
      options:
        request.query === "//home/"
          ? [{ value: "//home/logfeller/", label: "//home/logfeller/" }]
          : [
              { value: "//logs/", label: "//logs/" },
              { value: "//logfeller/", label: "//logfeller/" },
            ],
    }));
    const view = render(
      <Harness
        options={options}
        source="yandex.ytsaurus.tables"
        initialValue="//l"
      />,
    );
    const input = view.getByRole("combobox");

    fireEvent.focus(input);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    expect(options).toHaveBeenCalledTimes(1);
    expect(options).toHaveBeenLastCalledWith(
      expect.objectContaining({ query: "//" }),
    );

    fireEvent.input(input, { target: { value: "//lo" } });
    fireEvent.input(input, { target: { value: "//log" } });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(options).toHaveBeenCalledTimes(1);
    expect(view.getAllByRole("option")).toHaveLength(2);

    fireEvent.input(input, { target: { value: "//home/l" } });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(160);
    });
    expect(options).toHaveBeenCalledTimes(2);
    expect(options).toHaveBeenLastCalledWith(
      expect.objectContaining({ query: "//home/" }),
    );

    fireEvent.input(input, { target: { value: "//lo" } });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(options).toHaveBeenCalledTimes(2);
    expect(view.getAllByRole("option")).toHaveLength(2);
  });
});

function Harness({
  options,
  initialValue,
  source = "endpoint.paths",
  disabled = false,
}: {
  options: (request: DynamicOptionsQuery) => Promise<{
    options: Array<{ value: string; label: string }>;
  }>;
  initialValue: string;
  source?: string;
  disabled?: boolean;
}) {
  const [value, setValue] = useState(initialValue);
  return (
    <FormEnvironmentProvider environment={{ options }}>
      <DynamicPathControl
        source={source}
        dependencies={{ installation: "cluster-a" }}
        value={value}
        disabled={disabled}
        onChange={setValue}
      />
    </FormEnvironmentProvider>
  );
}
