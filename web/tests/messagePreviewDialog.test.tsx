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
              inferred_columns: [],
              sample_rows: [{}],
              preview_tabs: [],
              sampled_messages: 1,
              sampled_rows: 1,
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

  it("copies hexadecimal bytes and exposes parser-owned and parsed previews", async () => {
    const copy = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText: copy },
    });
    const apply = vi.fn();
    const payload = btoa('{"id":1}');
    const view = render(
      <MessagePreviewDialog
        loading={false}
        allowApply
        result={{
          text_preview: '{"id":1}',
          payload_preview_base64: payload,
          payload_base64: payload,
          byte_length: 8,
          preview_bytes: 8,
          metadata: metadata(),
          detections: [
            {
              key: "json_parser",
              label: "JSON parser",
              config: { common: {}, json_parser: {} },
              inferred_columns: [
                {
                  name: "id",
                  source_type: "number",
                  arrow_type: "Int64",
                  nullable: false,
                },
              ],
              sample_rows: [{ id: 1 }],
              preview_tabs: [
                {
                  key: "json_pretty_print",
                  label: "Pretty print",
                  content: '{\n  "id": 1\n}',
                  truncated: false,
                },
              ],
              sampled_messages: 3,
              sampled_rows: 3,
            },
          ],
        }}
        onApply={apply}
        onClose={vi.fn()}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: "Copy message" }));
    expect(copy).toHaveBeenCalledWith("7b 22 69 64 22 3a 31 7d");
    fireEvent.click(view.getByRole("tab", { name: "Pretty print" }));
    expect(view.getByText(/"id": 1/)).toBeTruthy();
    fireEvent.click(view.getByRole("button", { name: "See parsed" }));
    expect(
      view
        .getByRole("tab", { name: "Schema" })
        .getAttribute("aria-selected"),
    ).toBe("true");
    expect(view.getByText("3 messages · 3 rows")).toBeTruthy();
    expect(view.getByText("Int64")).toBeTruthy();
    expect(
      view.getAllByRole("tab").map((tab) => tab.textContent),
    ).toEqual(["Text", "Binary", "Metadata", "Pretty print", "Schema"]);
    fireEvent.click(view.getAllByRole("button", { name: "Use parser" })[0]!);
    expect(apply).toHaveBeenCalledWith(
      expect.objectContaining({ key: "json_parser" }),
    );
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
