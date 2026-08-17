// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MessagePreviewDialog } from "../src/delivery/MessagePreviewDialog";

afterEach(cleanup);

describe("message preview dialog", () => {
  it("renders text and hex, applies detection, and closes with Escape", () => {
    const apply = vi.fn();
    const close = vi.fn();
    const view = render(
      <MessagePreviewDialog
        loading={false}
        allowApply
        result={{
          text: "A\u0000B",
          payload_base64: "QQBC",
          byte_length: 3,
          detections: [
            {
              key: "json_parser",
              label: "JSON parser",
              config: { common: {}, json_parser: {} },
            },
          ],
        }}
        onApply={apply}
        onClose={close}
      />,
    );

    expect(view.container.textContent).toContain("41 00 42");
    expect(view.container.textContent).toContain("A·B");
    fireEvent.click(view.getByRole("button", { name: "Use parser" }));
    expect(apply).toHaveBeenCalledWith(
      expect.objectContaining({ key: "json_parser" }),
    );
    fireEvent.keyDown(document, { key: "Escape" });
    expect(close).toHaveBeenCalled();
  });
});
