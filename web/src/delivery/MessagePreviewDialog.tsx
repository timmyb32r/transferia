import { useEffect, useMemo, useRef, useState } from "preact/hooks";

import type {
  MessagePreviewResult,
  ParserDetection,
} from "../generated/apiContract";
import { Button } from "../ui/Button";

export function MessagePreviewDialog({
  result,
  error,
  loading,
  allowApply,
  onApply,
  onClose,
}: {
  result?: MessagePreviewResult | undefined;
  error?: string | undefined;
  loading: boolean;
  allowApply: boolean;
  onApply: (detection: ParserDetection) => void;
  onClose: () => void;
}) {
  const dialog = useRef<HTMLElement>(null);
  const [showFull, setShowFull] = useState(false);
  useEffect(() => {
    dialog.current
      ?.querySelector<HTMLButtonElement>("[aria-label='Close message preview']")
      ?.focus();
    const keydown = (event: KeyboardEvent) =>
      event.key === "Escape" && onClose();
    document.addEventListener("keydown", keydown);
    return () => document.removeEventListener("keydown", keydown);
  }, [onClose]);
  const previewBytes = useMemo(
    () =>
      result ? decodeBase64(result.payload_preview_base64) : new Uint8Array(),
    [result],
  );
  const fullBytes = useMemo(
    () =>
      result && showFull
        ? decodeBase64(result.payload_base64)
        : new Uint8Array(),
    [result, showFull],
  );
  const visibleBytes = showFull ? fullBytes : previewBytes;
  const binary = useMemo(() => hexDump(visibleBytes), [visibleBytes]);
  const text = useMemo(
    () =>
      result && showFull
        ? new TextDecoder("utf-8", { fatal: false }).decode(fullBytes)
        : (result?.text_preview ?? ""),
    [fullBytes, result, showFull],
  );
  const truncated = Boolean(
    result && result.preview_bytes < result.byte_length,
  );
  return (
    <div class="message-preview-backdrop" onMouseDown={onClose}>
      <section
        ref={dialog}
        class="message-preview-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="message-preview-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header>
          <div>
            <h2 id="message-preview-title">Message preview</h2>
            <span>
              {result
                ? `${result.byte_length} bytes · not committed`
                : "Read-only · no commit"}
            </span>
          </div>
          <Button
            shape="icon"
            aria-label="Close message preview"
            onClick={onClose}
          >
            ×
          </Button>
        </header>
        {loading && (
          <div class="message-preview-state">
            <span class="connection-check-spinner" />
            Reading one message…
          </div>
        )}
        {error && (
          <div class="message-preview-error" role="alert">
            {error}
          </div>
        )}
        {result && (
          <>
            <div class="message-preview-panes">
              <section>
                <h3>Text</h3>
                <pre>{text}</pre>
              </section>
              <section>
                <h3>Binary</h3>
                <pre class="hex-editor">{binary}</pre>
              </section>
            </div>
            {truncated && (
              <div class="message-preview-truncation" role="note">
                <span>
                  Showing{" "}
                  {formatBytes(
                    showFull ? result.byte_length : result.preview_bytes,
                  )}{" "}
                  of {formatBytes(result.byte_length)}
                </span>
                <Button onClick={() => setShowFull((current) => !current)}>
                  {showFull ? "Show 16 KiB preview" : "View full"}
                </Button>
                <Button onClick={() => downloadMessage(result)}>
                  Download full message
                </Button>
              </div>
            )}
            {!truncated && (
              <div class="message-preview-download">
                <Button onClick={() => downloadMessage(result)}>
                  Download message
                </Button>
              </div>
            )}
            <MessageMetadata result={result} />
            <footer>
              <div>
                <strong>Detected parsers</strong>
                {result.detections.length === 0 && (
                  <span class="muted">No parser recognized this message</span>
                )}
              </div>
              <div class="detected-parsers">
                {result.detections.map((detection) => (
                  <div class="detected-parser" key={detection.key}>
                    <span>{detection.label}</span>
                    <Button
                      variant="primary"
                      disabled={!allowApply}
                      title={
                        allowApply
                          ? "Use detected parser"
                          : "Enter edit mode to apply this parser"
                      }
                      onClick={() => onApply(detection)}
                    >
                      Use parser
                    </Button>
                  </div>
                ))}
              </div>
              {!allowApply && result.detections.length > 0 && (
                <span class="muted">
                  Enter edit mode to apply a detected parser.
                </span>
              )}
            </footer>
          </>
        )}
      </section>
    </div>
  );
}

function decodeBase64(value: string): Uint8Array {
  const binary = atob(value);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

function hexDump(bytes: Uint8Array): string {
  const rows: string[] = [];
  for (let offset = 0; offset < bytes.length; offset += 16) {
    const slice = bytes.slice(offset, offset + 16);
    const address = offset.toString(16).padStart(8, "0");
    const hex = Array.from(slice, (byte) => byte.toString(16).padStart(2, "0"))
      .join(" ")
      .padEnd(47, " ");
    const ascii = Array.from(slice, (byte) =>
      byte >= 32 && byte <= 126 ? String.fromCharCode(byte) : "·",
    ).join("");
    rows.push(`${address}  ${hex}  ${ascii}`);
  }
  return rows.join("\n");
}

function MessageMetadata({ result }: { result: MessagePreviewResult }) {
  const metadata = result.metadata;
  const rows: Array<[string, string]> = [
    ["Topic", metadata.topic],
    ["Partition", String(metadata.partition)],
    ["Offset", String(metadata.offset)],
    ["Sequence number", String(metadata.sequence_number)],
    ["Partition session", String(metadata.partition_session_id)],
    ["Producer", metadata.producer_id || "—"],
    ["Message group", metadata.message_group_id ?? "—"],
    ["Codec", metadata.codec],
    ["Created", formatTimestamp(metadata.created_at_ms)],
    ["Written", formatTimestamp(metadata.written_at_ms)],
    ["Compressed size", formatBytes(metadata.compressed_size)],
    [
      "Declared uncompressed size",
      metadata.declared_uncompressed_size == null
        ? "Not provided"
        : formatBytes(metadata.declared_uncompressed_size),
    ],
  ];
  return (
    <details class="message-preview-metadata" open>
      <summary>Message metadata</summary>
      <dl>
        {rows.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
      {Object.keys(metadata.write_session_metadata).length > 0 && (
        <MetadataGroup
          title="Write session metadata"
          entries={Object.entries(metadata.write_session_metadata)}
        />
      )}
      {metadata.message_metadata.length > 0 && (
        <MetadataGroup
          title="Message metadata"
          entries={metadata.message_metadata.map((item) => [
            item.key,
            item.value_text ?? `base64:${item.value_base64}`,
          ])}
        />
      )}
    </details>
  );
}

function MetadataGroup({
  title,
  entries,
}: {
  title: string;
  entries: Array<[string, string]>;
}) {
  return (
    <section>
      <h4>{title}</h4>
      <dl>
        {entries.map(([key, value], index) => (
          <div key={`${key}-${index}`}>
            <dt>{key}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}

function formatTimestamp(value: number | null | undefined): string {
  return value == null ? "Not provided" : new Date(value).toISOString();
}

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
}

function downloadMessage(result: MessagePreviewResult) {
  const bytes = decodeBase64(result.payload_base64);
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  const blob = new Blob([buffer], {
    type: "application/octet-stream",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `message-${result.metadata.partition}-${result.metadata.offset}.bin`;
  link.click();
  URL.revokeObjectURL(url);
}
