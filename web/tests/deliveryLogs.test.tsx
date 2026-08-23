// @vitest-environment jsdom

import { cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";
import { DeliveryLogs } from "../src/delivery/DeliveryLogs";
import { render } from "./support/render";

describe("delivery logs", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("copies an immutable snapshot while newer log lines arrive", async () => {
    vi.useFakeTimers();
    vi.spyOn(api, "deliveryLogs").mockResolvedValue({
      workers: [{ worker_id: "active-run", size_bytes: 12, active: true }],
    });
    vi.spyOn(api, "deliveryLog")
      .mockResolvedValueOnce({
        text: "first\n",
        start_offset: 0,
        next_offset: 6,
        end_offset: 6,
        truncated_before: false,
      })
      .mockResolvedValue({
        text: "second\n",
        start_offset: 6,
        next_offset: 13,
        end_offset: 13,
        truncated_before: false,
      });
    let finishCopy: (() => void) | undefined;
    const writeText = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          finishCopy = resolve;
        }),
    );
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    const view = render(<DeliveryLogs deliveryId="delivery-1" />);
    await vi.waitFor(() => expect(view.getByText(/first/)).toBeTruthy());
    fireEvent.click(view.getByRole("button", { name: "Copy to clipboard" }));
    expect(writeText).toHaveBeenCalledWith("first\n");

    await vi.advanceTimersByTimeAsync(1000);
    await vi.waitFor(() => expect(view.getByText(/second/)).toBeTruthy());
    expect(writeText).toHaveBeenCalledOnce();
    expect(writeText).toHaveBeenCalledWith("first\n");

    finishCopy?.();
    await vi.waitFor(() => expect(view.getByRole("status").textContent).toBe("Copied"));
  });

  it("selects the active worker and follows bounded log chunks", async () => {
    vi.spyOn(api, "deliveryLogs").mockResolvedValue({
      workers: [
        { worker_id: "old-run", size_bytes: 3, active: false },
        { worker_id: "active-run", size_bytes: 5, active: true },
      ],
    });
    const read = vi.spyOn(api, "deliveryLog").mockResolvedValue({
      text: "ready\n",
      start_offset: 0,
      next_offset: 6,
      end_offset: 6,
      truncated_before: false,
    });

    const view = render(<DeliveryLogs deliveryId="delivery-1" />);

    await waitFor(() =>
      expect(read).toHaveBeenCalledWith("delivery-1", "active-run", undefined),
    );
    expect(view.getByText("ready", { exact: false })).toBeTruthy();
    fireEvent.pointerDown(view.getByRole("button", { name: "Worker" }));
    fireEvent.pointerDown(view.getByRole("option", { name: /old-run/ }));
    await waitFor(() =>
      expect(read).toHaveBeenCalledWith("delivery-1", "old-run", undefined),
    );
  });

  it("renders backend failures beside the viewer", async () => {
    vi.spyOn(api, "deliveryLogs").mockRejectedValue(new Error("logs denied"));
    vi.spyOn(api, "deliveryLog");

    const view = render(<DeliveryLogs deliveryId="delivery-1" />);

    expect(await view.findByText("logs denied")).toBeTruthy();
    expect(view.getByText("No log output yet.")).toBeTruthy();
  });
});
