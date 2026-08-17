// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MessagePreviewDialog } from "../src/delivery/MessagePreviewDialog";

afterEach(cleanup);

describe("message preview dialog", () => {
  it("opens binary for non-printable content and closes with Escape", () => {
    const apply = vi.fn();
    const close = vi.fn();
    const view = render(
      <MessagePreviewDialog
        loading={false}
        allowApply
        result={{
          text_preview: "A\u0000B",
          payload_preview_base64: "QQBC",
          payload_base64: "QQBC",
          byte_length: 3,
          preview_bytes: 3,
          metadata: metadata(),
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
    expect(
      view.getByRole("tab", { name: "Binary" }).getAttribute("aria-selected"),
    ).toBe("true");
    expect(view.container.textContent).toContain("shown in full");
    fireEvent.click(view.getByRole("button", { name: "Use parser" }));
    expect(apply).toHaveBeenCalledWith(
      expect.objectContaining({ key: "json_parser" }),
    );
    fireEvent.keyDown(document, { key: "Escape" });
    expect(close).toHaveBeenCalled();
  });

  it("keeps a long message truncated until View full is requested", () => {
    const view = render(
      <MessagePreviewDialog
        loading={false}
        allowApply
        result={{
          text_preview: "A",
          payload_preview_base64: "QQ==",
          payload_base64: "QUI=",
          byte_length: 2,
          preview_bytes: 1,
          metadata: metadata(),
          detections: [],
        }}
        onApply={vi.fn()}
        onClose={vi.fn()}
      />,
    );

    expect(view.container.textContent).toContain("Showing 1 B of 2 B");
    expect(
      view.getByRole("tab", { name: "Text" }).getAttribute("aria-selected"),
    ).toBe("true");
    expect(view.container.textContent).toContain("partially shown");
    fireEvent.click(view.getByRole("button", { name: "View full" }));
    fireEvent.click(view.getByRole("tab", { name: "Binary" }));
    expect(view.container.textContent).toContain("41 42");
    fireEvent.click(view.getByRole("tab", { name: "Metadata" }));
    expect(view.getByText("cdc/prod/logs")).toBeTruthy();
  });
});

function metadata() {
  return {
    topic: "cdc/prod/logs",
    partition: 1,
    partition_session_id: 7,
    offset: 42,
    sequence_number: 10,
    created_at_ms: null,
    written_at_ms: null,
    producer_id: "producer",
    message_group_id: null,
    codec: "raw",
    compressed_size: 3,
    declared_uncompressed_size: 3,
    message_metadata: [],
    write_session_metadata: {},
  };
}
