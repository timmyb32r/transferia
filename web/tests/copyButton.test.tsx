// @vitest-environment jsdom
import { act, cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { CopyButton } from "../src/ui/CopyButton";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.useRealTimers(); vi.unstubAllGlobals(); });

it("shows one delayed Copy tooltip on hover and keyboard focus", async () => {
  vi.useFakeTimers();
  const view = render(<CopyButton text="exact" label="Copy value" />);
  const button = view.getByRole("button", { name: "Copy value" });
  fireEvent.mouseEnter(button);
  expect(view.queryByRole("tooltip")).toBeNull();
  await act(async () => { await vi.advanceTimersByTimeAsync(350); });
  expect(view.getByRole("tooltip").textContent).toBe("Copy");
  expect(button.hasAttribute("title")).toBe(false);
  fireEvent.mouseLeave(button);
  expect(view.queryByRole("tooltip")).toBeNull();
  act(() => button.focus());
  await act(async () => { await vi.advanceTimersByTimeAsync(350); });
  expect(view.getByRole("tooltip").textContent).toBe("Copy");
  fireEvent.keyDown(button, { key: "Escape" });
  expect(view.queryByRole("tooltip")).toBeNull();
});

it("deduplicates writes and changes only the icon and tooltip after success", async () => {
  let finish!: () => void;
  const writeText = vi.fn(() => new Promise<void>(resolve => { finish = resolve; }));
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  const view = render(<div><CopyButton text="db.`exact.name`" label="Copy value" /><button>Next</button></div>);
  const button = view.getByRole("button", { name: "Copy value" });
  const next = view.getByRole("button", { name: "Next" });
  const icon = button.querySelector(".copy-icon");
  fireEvent.click(button);
  fireEvent.click(button);
  expect(button.getAttribute("aria-busy")).toBe("true");
  expect(button.querySelector(".copy-icon-check")).toBeNull();
  expect(view.getByRole("tooltip").textContent).toBe("Copying…");
  expect(writeText).toHaveBeenCalledExactlyOnceWith("db.`exact.name`");
  finish();
  await waitFor(() => expect(button.getAttribute("aria-busy")).toBe("false"));
  expect(view.getByRole("tooltip").textContent).toBe("Copied");
  expect(button.querySelector(".copy-icon-check")).not.toBeNull();
  expect(button.querySelector(".copy-icon")).toBe(icon);
  expect(view.getByRole("button", { name: "Next" })).toBe(next);
  expect(view.getByRole("tooltip").parentElement).toBe(document.body);
});

it("reports clipboard failure without a success check and allows retry", async () => {
  const writeText = vi.fn().mockRejectedValueOnce(new Error("denied")).mockResolvedValueOnce(undefined);
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  const view = render(<CopyButton text="exact" label="Copy value" />);
  const button = view.getByRole("button", { name: "Copy value" });
  fireEvent.click(button);
  await waitFor(() => expect(view.getByRole("tooltip").textContent).toBe("Copy failed"));
  expect(button.querySelector(".copy-icon-check")).toBeNull();
  fireEvent.mouseLeave(button);
  expect(button.querySelector(".copy-icon-failed")).not.toBeNull();
  expect(button.querySelector('[aria-live="polite"]')?.textContent).toBe("Copy failed");
  fireEvent.click(button);
  await waitFor(() => expect(view.getByRole("tooltip").textContent).toBe("Copied"));
});

it("does not mark changed content as copied when an older write finishes", async () => {
  let finish!: () => void;
  const writeText = vi.fn(() => new Promise<void>(resolve => { finish = resolve; }));
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  const view = render(<CopyButton text="old" label="Copy value" />);
  const button = view.getByRole("button", { name: "Copy value" });
  fireEvent.click(button);
  view.rerender(<CopyButton text="new" label="Copy value" />);
  fireEvent.click(button);
  expect(writeText).toHaveBeenCalledOnce();
  finish();
  await waitFor(() => expect(button.getAttribute("data-copy-state")).toBe("idle"));
  expect(button.querySelector(".copy-icon-check")).toBeNull();
});

it("keeps failure feedback visible and accessible after focus leaves during a write", async () => {
  let reject!: (reason: Error) => void;
  vi.stubGlobal("navigator", { clipboard: { writeText: () => new Promise<void>((_resolve, fail) => { reject = fail; }) } });
  const view = render(<CopyButton text="exact" label="Copy value" />);
  const button = view.getByRole("button", { name: "Copy value" });
  act(() => button.focus());
  fireEvent.click(button);
  act(() => button.blur());
  reject(new Error("denied"));
  await waitFor(() => expect(button.querySelector(".copy-icon-failed")).not.toBeNull());
  expect(view.queryByRole("tooltip")).toBeNull();
  expect(button.querySelector('[aria-live="polite"]')?.textContent).toBe("Copy failed");
});
