// @vitest-environment jsdom

import { cleanup, fireEvent, render, waitFor } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../src/api";
import { DeliveryLogs } from "../src/delivery/DeliveryLogs";

describe("delivery logs", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
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
    fireEvent.change(view.getByLabelText("Worker"), {
      target: { value: "old-run" },
    });
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
