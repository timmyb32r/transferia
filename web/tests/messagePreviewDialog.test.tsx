// @vitest-environment jsdom

import { cleanup, fireEvent, render, waitFor } from "@testing-library/preact";
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

  it("copies text as text and binary as bytes, with raw actions only on raw tabs", async () => {
    const copyText = vi.fn().mockResolvedValue(undefined);
    const copyBinary = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText: copyText, write: copyBinary },
    });
    class TestClipboardItem {
      constructor(readonly items: Record<string, Blob>) {}
    }
    vi.stubGlobal("ClipboardItem", TestClipboardItem);
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

    const actionBar = view.container.querySelector(".message-preview-download");
    fireEvent.click(view.getByRole("button", { name: "Copy message" }));
    expect(copyText).toHaveBeenCalledWith('{"id":1}');
    await waitFor(() =>
      expect(
        actionBar?.contains(view.getByText("Message copied as text")),
      ).toBe(true),
    );
    expect(view.container.querySelector(".message-preview-download")).toBe(
      actionBar,
    );
    fireEvent.click(view.getByRole("tab", { name: "Binary" }));
    fireEvent.click(view.getByRole("button", { name: "Copy message" }));
    await waitFor(() => expect(copyBinary).toHaveBeenCalledTimes(1));
    const [clipboardItems] = copyBinary.mock.calls[0] as [
      TestClipboardItem[],
    ];
    expect(Object.keys(clipboardItems[0]!.items)).toEqual([
      "web application/octet-stream",
    ]);
    expect(clipboardItems[0]!.items["web application/octet-stream"]?.size).toBe(
      8,
    );
    fireEvent.click(view.getByRole("tab", { name: "Pretty print" }));
    expect(view.queryByRole("button", { name: "Copy message" })).toBeNull();
    expect(view.queryByRole("button", { name: "Download message" })).toBeNull();
    expect(view.container.querySelector(".syntax-code")?.textContent).toContain(
      '"id": 1',
    );
    expect(view.container.querySelector(".syntax-key")?.textContent).toBe(
      '"id":',
    );
    expect(view.container.querySelector(".syntax-number")?.textContent).toBe(
      "1",
    );
    fireEvent.click(view.getByRole("button", { name: "See parsed" }));
    expect(
      view.getByRole("tab", { name: "Schema" }).getAttribute("aria-selected"),
    ).toBe("true");
    expect(view.getByText("3 messages · 3 rows")).toBeTruthy();
    expect(view.getByText("Int64")).toBeTruthy();
    expect(view.getAllByRole("tab").map((tab) => tab.textContent)).toEqual([
      "Text",
      "Binary",
      "Metadata",
      "Pretty print",
      "Schema",
    ]);
    fireEvent.click(view.getAllByRole("button", { name: "Use parser" })[0]!);
    expect(apply).toHaveBeenCalledWith(
      expect.objectContaining({ key: "json_parser" }),
    );
    vi.unstubAllGlobals();
  });

  it("labels the dialog as exactly one message", () => {
    const view = render(
      <MessagePreviewDialog
        loading={false}
        allowApply
        result={undefined}
        onApply={vi.fn()}
        onClose={vi.fn()}
      />,
    );

    expect(view.getByRole("heading", { name: "One message preview" })).toBeTruthy();
  });

  it("groups equivalent pretty-print previews and lets the user choose the parser", () => {
    const payload = btoa("tskv\tlevel=INFO\tmessage=ready");
    const view = render(
      <MessagePreviewDialog
        loading={false}
        allowApply
        result={{
          text_preview: "tskv\tlevel=INFO\tmessage=ready",
          payload_preview_base64: payload,
          payload_base64: payload,
          byte_length: 29,
          preview_bytes: 29,
          metadata: metadata(),
          detections: [
            detection("json_parser", "JSON parser", "{ pretty json }"),
            detection("tskv", "TSKV parser", "level   = INFO"),
          ],
        }}
        onApply={vi.fn()}
        onClose={vi.fn()}
      />,
    );

    expect(view.getAllByRole("tab", { name: "Pretty print" })).toHaveLength(1);
    fireEvent.click(view.getByRole("tab", { name: "Pretty print" }));
    expect(view.getByText("{ pretty json }")).toBeTruthy();
    fireEvent.click(view.getByRole("button", { name: "JSON parser" }));
    fireEvent.click(view.getByRole("option", { name: "TSKV parser" }));
    expect(view.container.querySelector(".parser-preview-tab pre")?.textContent).toBe(
      "level   = INFO",
    );
    expect(view.queryByText("{ pretty json }")).toBeNull();
  });
});

function detection(key: string, label: string, content: string) {
  return {
    key,
    label,
    config: {},
    inferred_columns: [],
    sample_rows: [],
    preview_tabs: [
      {
        key: `${key}_pretty_print`,
        label: "Pretty print",
        content,
        truncated: false,
      },
    ],
    sampled_messages: 1,
    sampled_rows: 1,
  };
}

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
