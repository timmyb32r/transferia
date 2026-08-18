import { useEffect, useRef, useState } from "preact/hooks";

import type { JsonValue } from "../json";
import { Button } from "../ui/Button";
import { EyeIcon, EyeOffIcon } from "../ui/icons";
import { SelectControl } from "../ui/SelectControl";
import { humanize, type CompiledNode } from "./compiler";
import { parsePartitionIds } from "./formLogic";
import { isObject } from "./value";

export function IndeterminateCheckbox({
  ariaLabel,
  checked,
  indeterminate,
  disabled,
  onChange,
}: {
  ariaLabel: string;
  checked: boolean;
  indeterminate: boolean;
  disabled: boolean;
  onChange: (checked: boolean) => void;
}) {
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (input.current) input.current.indeterminate = indeterminate;
  }, [indeterminate]);
  return (
    <input
      ref={input}
      type="checkbox"
      aria-label={ariaLabel}
      checked={checked}
      disabled={disabled}
      onChange={(event) => onChange(event.currentTarget.checked)}
    />
  );
}

export function useStableRowIds(length: number) {
  const sequence = useRef(0);
  const values = useRef<string[]>([]);
  const create = () => `row-${++sequence.current}`;
  while (values.current.length < length) values.current.push(create());
  if (values.current.length > length) values.current.length = length;
  return {
    values: values.current,
    insert: (index: number) => values.current.splice(index, 0, create()),
    remove: (index: number) => values.current.splice(index, 1),
    retain: (keep: (id: string, index: number) => boolean) => {
      values.current = values.current.filter(keep);
    },
    move: (from: number, to: number) => {
      const [id] = values.current.splice(from, 1);
      if (id !== undefined) values.current.splice(to, 0, id);
    },
  };
}

export function PasswordInput({
  id,
  value,
  disabled,
  onChange,
}: {
  id?: string | undefined;
  value: string;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const [visible, setVisible] = useState(false);
  return (
    <div class="password-control">
      <input
        id={id}
        type={visible ? "text" : "password"}
        name={`transferia-secret-${id ?? "field"}`}
        autoComplete="new-password"
        value={value}
        disabled={disabled}
        onInput={(event) => onChange(event.currentTarget.value)}
      />
      <Button
        class="password-reveal"
        aria-label={visible ? "Hide secret" : "Show secret"}
        aria-pressed={visible}
        disabled={disabled}
        onClick={() => setVisible((current) => !current)}
      >
        {visible ? <EyeOffIcon /> : <EyeIcon />}
      </Button>
    </div>
  );
}

const SYSTEM_COLUMN_DEFAULTS: Record<string, string> = {
  topic: "_system_topic",
  partition: "_system_partition",
  offset: "_system_offset",
  message_index: "_system_message_index",
  write_timestamp_ms: "_system_write_timestamp_ms",
};

export function SystemColumnsEditor({
  node,
  value,
  disabled,
  onChange,
}: {
  node: Extract<CompiledNode, { kind: "object" }>;
  value: JsonValue;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const object = isObject(value) ? value : {};
  return (
    <div class="system-column-list">
      {Object.keys(node.properties).map((name) => {
        const configured = typeof object[name] === "string";
        const columnName = configured
          ? String(object[name])
          : (SYSTEM_COLUMN_DEFAULTS[name] ?? `_system_${name}`);
        return (
          <div class="system-column-row" key={name}>
            <span class="system-column-label">
              {humanize(name)}
              {node.properties[name]?.description && (
                <span
                  class="help"
                  tabindex={0}
                  data-tooltip={node.properties[name]!.description}
                >
                  ?
                </span>
              )}
            </span>
            <input
              type="checkbox"
              checked={configured}
              disabled={disabled}
              aria-label={`Include ${humanize(name)}`}
              onChange={(event) =>
                onChange({
                  ...object,
                  [name]: event.currentTarget.checked ? columnName : null,
                })
              }
            />
            <input
              type="text"
              autoComplete="off"
              value={columnName}
              disabled={disabled || !configured}
              aria-label={`${humanize(name)} column name`}
              onInput={(event) =>
                onChange({ ...object, [name]: event.currentTarget.value })
              }
            />
          </div>
        );
      })}
    </div>
  );
}

const BYTE_UNITS = [
  { label: "B", factor: 1 },
  { label: "KiB", factor: 1024 },
  { label: "MiB", factor: 1024 * 1024 },
  { label: "GiB", factor: 1024 * 1024 * 1024 },
] as const;

export function ByteSizeInput({
  id,
  value,
  disabled,
  onChange,
}: {
  id?: string | undefined;
  value: number | null;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const [unitIndex, setUnitIndex] = useState(() => bestByteUnit(value ?? 0));
  const unit = BYTE_UNITS[unitIndex]!;
  return (
    <div class="byte-size-input">
      <input
        id={id}
        type="number"
        autoComplete="off"
        min={0}
        step="any"
        value={value === null ? "" : value / unit.factor}
        disabled={disabled}
        onInput={(event) => {
          const raw = event.currentTarget.value;
          if (raw === "") {
            onChange(null);
            return;
          }
          const bytes = Number(raw) * unit.factor;
          if (Number.isSafeInteger(bytes) && bytes >= 0) onChange(bytes);
        }}
      />
      <SelectControl
        value={String(unitIndex)}
        placeholder="Unit"
        disabled={disabled}
        options={BYTE_UNITS.map((candidate, index) => ({
          value: String(index),
          label: candidate.label,
        }))}
        onChange={(next) => setUnitIndex(Number(next))}
      />
    </div>
  );
}

function bestByteUnit(value: number): number {
  for (let index = BYTE_UNITS.length - 1; index > 0; index -= 1) {
    if (
      value >= BYTE_UNITS[index]!.factor &&
      value % BYTE_UNITS[index]!.factor === 0
    )
      return index;
  }
  return 0;
}

export function PartitionRangesInput({
  id,
  value,
  disabled,
  onChange,
}: {
  id?: string | undefined;
  value: JsonValue;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const canonical = formatPartitionIds(value);
  const [raw, setRaw] = useState(canonical);
  const [error, setError] = useState<string>();
  useEffect(() => setRaw(canonical), [canonical]);
  return (
    <div class="validated-input">
      <input
        id={id}
        type="text"
        autoComplete="off"
        inputMode="numeric"
        placeholder="e.g. 1-5,7"
        value={raw}
        disabled={disabled}
        aria-invalid={error !== undefined}
        onInput={(event) => {
          const next = event.currentTarget.value;
          setRaw(next);
          const parsed = parsePartitionIds(next);
          setError(parsed.error);
          if (parsed.value !== undefined) onChange(parsed.value);
        }}
      />
      {error && <small class="validation-error">{error}</small>}
    </div>
  );
}

function formatPartitionIds(value: JsonValue): string {
  return Array.isArray(value) && value.every((item) => typeof item === "number")
    ? value.join(",")
    : "";
}
