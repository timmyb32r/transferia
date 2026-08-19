// @vitest-environment jsdom

import { cleanup, fireEvent, render, waitFor } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { YamlEditorPanel } from "../src/delivery/YamlEditorPanel";

afterEach(cleanup);

describe("YAML editor", () => {
  it("locks Copy immediately and reports completion in a reserved slot", async () => {
    let finish: (() => void) | undefined;
    const writeText = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    );
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    const view = render(
      <YamlEditorPanel value="delivery_id: one" disabled onChange={vi.fn()} />,
    );

    const button = view.getByRole("button", { name: "Copy" });
    const status = view.getByRole("status");
    fireEvent.click(button);

    expect((button as HTMLButtonElement).disabled).toBe(true);
    expect(button.getAttribute("aria-busy")).toBe("true");
    expect(status.textContent).toBe("Copying…");
    expect(status.parentElement?.contains(button)).toBe(true);

    finish?.();
    await waitFor(() => expect(status.textContent).toBe("Copied"));
    expect((button as HTMLButtonElement).disabled).toBe(false);
    expect(writeText).toHaveBeenCalledOnce();
  });
});
