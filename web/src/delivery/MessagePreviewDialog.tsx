import { useEffect, useMemo, useRef, useState } from "preact/hooks";

import type {
  MessagePreviewResult,
  ParserDetection,
} from "../generated/apiContract";
import { Button } from "../ui/Button";
import { SelectControl } from "../ui/SelectControl";
import { SyntaxHighlight } from "../ui/SyntaxHighlight";

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
  const [activeTab, setActiveTab] = useState("text");
  const [selectedParser, setSelectedParser] = useState("");
  const [copyState, setCopyState] = useState<
    "idle" | "copying" | "copied" | "error"
  >("idle");
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
  useEffect(() => {
    setShowFull(false);
    setActiveTab(isPrintableText(previewBytes) ? "text" : "binary");
    setSelectedParser(result?.detections[0]?.key ?? "");
    setCopyState("idle");
  }, [previewBytes, result]);
  useEffect(() => setCopyState("idle"), [activeTab]);
  const fullBytes = useMemo(
    () =>
      result && showFull
        ? decodeBase64(result.payload_base64)
        : new Uint8Array(),
    [result, showFull],
  );
  const visibleBytes = showFull ? fullBytes : previewBytes;
  const binary = useMemo(() => hexColumns(visibleBytes), [visibleBytes]);
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
  const shownInFull = Boolean(result && (!truncated || showFull));
  const previewGroups = useMemo(
    () => groupParserPreviews(result?.detections ?? []),
    [result],
  );
  const selectParsed = (detection: ParserDetection) => {
    setSelectedParser(detection.key);
    setActiveTab("parsed");
  };
  const copyMessage = async (format: "text" | "binary") => {
    if (!result || copyState === "copying") return;
    setCopyState("copying");
    try {
      const bytes = decodeBase64(result.payload_base64);
      if (format === "text") {
        await navigator.clipboard.writeText(
          new TextDecoder("utf-8", { fatal: false }).decode(bytes),
        );
      } else {
        const buffer = new ArrayBuffer(bytes.byteLength);
        new Uint8Array(buffer).set(bytes);
        await navigator.clipboard.write([
          new ClipboardItem({
            "web application/octet-stream": new Blob([buffer], {
              type: "application/octet-stream",
            }),
          }),
        ]);
      }
      setCopyState("copied");
    } catch {
      setCopyState("error");
    }
  };
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
            <h2 id="message-preview-title">One message preview</h2>
            <span>
              {result
                ? `${result.byte_length} bytes (${shownInFull ? "shown in full" : "partially shown"}) · not committed`
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
            <div class="message-preview-tabs" role="tablist">
              {[
                { key: "text", label: "Text" },
                { key: "binary", label: "Binary" },
                { key: "metadata", label: "Metadata" },
                ...previewGroups.map((group) => ({
                  key: `parser:${group.key}`,
                  label: group.label,
                })),
                { key: "parsed", label: "Schema" },
              ].map((tab) => (
                <Button
                  key={tab.key}
                  type="button"
                  role="tab"
                  aria-selected={activeTab === tab.key}
                  class={activeTab === tab.key ? "is-active" : undefined}
                  onClick={() => setActiveTab(tab.key)}
                >
                  {tab.label}
                </Button>
              ))}
            </div>
            <div class="message-preview-content" role="tabpanel">
              {activeTab === "text" && (
                <pre class="message-preview-text">{text}</pre>
              )}
              {activeTab === "binary" && <HexEditor columns={binary} />}
              {activeTab.startsWith("parser:") && (
                <ParserPreview
                  group={previewGroups.find(
                    (group) => `parser:${group.key}` === activeTab,
                  )}
                  selectedParser={selectedParser}
                  onSelectParser={setSelectedParser}
                />
              )}
              {activeTab === "metadata" && <MessageMetadata result={result} />}
              {activeTab === "parsed" && (
                <ParsedPreview
                  detections={result.detections}
                  selectedKey={selectedParser}
                  allowApply={allowApply}
                  onSelect={setSelectedParser}
                  onApply={onApply}
                />
              )}
            </div>
            <div
              class={
                truncated
                  ? "message-preview-truncation"
                  : "message-preview-download"
              }
              role={truncated ? "note" : undefined}
            >
              {(activeTab === "text" || activeTab === "binary") && (
                <>
                  <CopyStatus state={copyState} format={activeTab} />
                  {truncated && (
                    <>
                      <span>
                        Showing{" "}
                        {formatBytes(
                          showFull
                            ? result.byte_length
                            : result.preview_bytes,
                        )}{" "}
                        of {formatBytes(result.byte_length)}
                      </span>
                      <Button
                        onClick={() => setShowFull((current) => !current)}
                      >
                        {showFull ? "Show 16 KiB preview" : "View full"}
                      </Button>
                    </>
                  )}
                  <Button
                    pending={copyState === "copying"}
                    onClick={() => void copyMessage(activeTab)}
                  >
                    Copy message
                  </Button>
                  <Button onClick={() => downloadMessage(result)}>
                    {truncated ? "Download full message" : "Download message"}
                  </Button>
                </>
              )}
            </div>
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
                    <span>{detectionDisplayLabel(detection)}</span>
                    <Button onClick={() => selectParsed(detection)}>
                      See parsed
                    </Button>
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

function CopyStatus({
  state,
  format,
}: {
  state: "idle" | "copying" | "copied" | "error";
  format: "text" | "binary";
}) {
  return (
    <span
      class={`message-preview-copy-state ${state}`}
      role={state === "error" ? "alert" : "status"}
      aria-live="polite"
    >
      {state === "copied"
        ? format === "text"
          ? "Message copied as text"
          : "Message copied as binary data"
        : state === "error"
          ? "Could not copy the message"
          : state === "copying"
            ? "Copying message…"
            : "Copy status"}
    </span>
  );
}

function decodeBase64(value: string): Uint8Array {
  const binary = atob(value);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

function ParserPreview({
  group,
  selectedParser,
  onSelectParser,
}: {
  group: ParserPreviewGroup | undefined;
  selectedParser: string;
  onSelectParser: (key: string) => void;
}) {
  if (!group) return <div class="message-preview-state">Preview unavailable</div>;
  const selected =
    group.entries.find((entry) => entry.detection.key === selectedParser) ??
    group.entries[0];
  if (!selected)
    return <div class="message-preview-state">Preview unavailable</div>;
  const { tab } = selected;
  const json = tab.key.toLowerCase().includes("json");
  return (
    <div class="parser-preview-tab">
      {group.entries.length > 1 && (
        <div class="parser-preview-toolbar">
          <span>Pretty-print parser</span>
          <SelectControl
            value={selected.detection.key}
            placeholder="Parser"
            options={group.entries.map((entry) => ({
              value: entry.detection.key,
              label: detectionDisplayLabel(entry.detection),
            }))}
            searchable={false}
            clearable={false}
            onChange={onSelectParser}
          />
        </div>
      )}
      <pre>
        {json ? (
          <SyntaxHighlight value={tab.content} language="json" />
        ) : (
          tab.content
        )}
      </pre>
      {tab.truncated && (
        <span class="muted">Parser preview truncated to 64 KiB</span>
      )}
    </div>
  );
}

interface ParserPreviewGroup {
  key: string;
  label: string;
  entries: Array<{
    detection: ParserDetection;
    tab: ParserDetection["preview_tabs"][number];
  }>;
}

function groupParserPreviews(
  detections: ParserDetection[],
): ParserPreviewGroup[] {
  const groups = new Map<string, ParserPreviewGroup>();
  for (const detection of detections) {
    for (const tab of detection.preview_tabs) {
      let group = groups.get(tab.label);
      if (group === undefined) {
        group = { key: tab.label, label: tab.label, entries: [] };
        groups.set(tab.label, group);
      }
      group.entries.push({ detection, tab });
    }
  }
  return [...groups.values()];
}

function ParsedPreview({
  detections,
  selectedKey,
  allowApply,
  onSelect,
  onApply,
}: {
  detections: ParserDetection[];
  selectedKey: string;
  allowApply: boolean;
  onSelect: (key: string) => void;
  onApply: (detection: ParserDetection) => void;
}) {
  const selected = detections.find(
    (detection) => detection.key === selectedKey,
  );
  if (detections.length === 0)
    return (
      <div class="message-preview-state">No parser recognized this sample</div>
    );
  return (
    <div class="parsed-preview">
      <div class="parsed-preview-toolbar">
        <SelectControl
          value={selectedKey}
          placeholder="Not selected"
          options={detections.map((detection) => ({
            value: detection.key,
            label: detectionDisplayLabel(detection),
          }))}
          onChange={onSelect}
        />
        {selected && (
          <>
            <span class="muted">
              {selected.sampled_messages} messages · {selected.sampled_rows}{" "}
              rows
            </span>
            <Button
              variant="primary"
              disabled={!allowApply}
              onClick={() => onApply(selected)}
            >
              Use parser
            </Button>
          </>
        )}
      </div>
      {selected && (
        <table class="parsed-preview-table">
          <thead>
            <tr>
              <th>Column</th>
              <th>Source type</th>
              <th>Arrow type</th>
              <th>Nullable</th>
            </tr>
          </thead>
          <tbody>
            {selected.inferred_columns.map((column) => (
              <tr key={column.name}>
                <td>{column.name}</td>
                <td>{column.source_type}</td>
                <td>{column.arrow_type}</td>
                <td>{column.nullable ? "Yes" : "No"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function hexColumns(bytes: Uint8Array) {
  const addresses: string[] = [];
  const values: string[] = [];
  const text: string[] = [];
  for (let offset = 0; offset < bytes.length; offset += 16) {
    const slice = bytes.slice(offset, offset + 16);
    addresses.push(offset.toString(16).padStart(8, "0"));
    const hex = Array.from(slice, (byte) =>
      byte.toString(16).padStart(2, "0"),
    ).join(" ");
    const ascii = Array.from(slice, (byte) =>
      byte >= 32 && byte <= 126 ? String.fromCharCode(byte) : "·",
    ).join("");
    values.push(hex);
    text.push(ascii);
  }
  return { addresses, values, text };
}

function HexEditor({ columns }: { columns: ReturnType<typeof hexColumns> }) {
  return (
    <div class="hex-editor" aria-label="Binary message">
      <pre class="hex-addresses" aria-label="Offsets">
        {columns.addresses.join("\n")}
      </pre>
      <pre class="hex-values" aria-label="Hex values">
        {columns.values.join("\n")}
      </pre>
      <pre class="hex-text" aria-label="ASCII text">
        {columns.text.join("\n")}
      </pre>
    </div>
  );
}

function isPrintableText(bytes: Uint8Array): boolean {
  let value: string;
  try {
    value = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return false;
  }
  return Array.from(value).every((character) => {
    const code = character.codePointAt(0) ?? 0;
    return (
      character === "\n" ||
      character === "\r" ||
      character === "\t" ||
      (code >= 0x20 && code !== 0x7f)
    );
  });
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
    <div class="message-preview-metadata">
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
    </div>
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

function detectionDisplayLabel(detection: ParserDetection): string {
  if (!isObject(detection.config)) return detection.label;
  const parser = detection.config["json_parser"];
  if (!isObject(parser)) return detection.label;
  const framing = parser["json_framing"];
  const label =
    framing === "json_lines"
      ? "JSON lines"
      : framing === "json_array"
        ? "JSON array"
        : framing === "single_document"
          ? "Single document"
          : undefined;
  return label ? `${detection.label} · ${label}` : detection.label;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
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
