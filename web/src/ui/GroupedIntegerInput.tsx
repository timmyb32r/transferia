import { useEffect, useRef, useState } from "preact/hooks";

import { AutofillResistantInput } from "./AutofillResistantField";

export interface GroupedIntegerInputProps {
  id?: string | undefined;
  value: number | null;
  minimum?: number | undefined;
  maximum?: number | undefined;
  disabled?: boolean | undefined;
  onChange: (value: number | null) => void;
}

export function GroupedIntegerInput({
  id,
  value,
  minimum,
  maximum,
  disabled = false,
  onChange,
}: GroupedIntegerInputProps) {
  const input = useRef<HTMLInputElement>(null);
  const focused = useRef(false);
  const [displayValue, setDisplayValue] = useState(() => formatInteger(value));

  useEffect(() => {
    if (!focused.current) setDisplayValue(formatInteger(value));
  }, [value]);

  return (
    <AutofillResistantInput
      inputRef={input}
      id={id}
      type="text"
      inputMode="numeric"
      value={displayValue}
      disabled={disabled}
      onFocus={() => {
        focused.current = true;
      }}
      onBlur={() => {
        focused.current = false;
        setDisplayValue(formatInteger(value));
      }}
      onInput={(event) => {
        const raw = event.currentTarget.value;
        const digits = raw.replaceAll(/[\s,_]/g, "");
        if (digits === "") {
          setDisplayValue("");
          onChange(null);
          return;
        }
        if (!/^\d+$/.test(digits)) {
          const formatted = formatInteger(value);
          event.currentTarget.value = formatted;
          setDisplayValue(formatted);
          return;
        }
        const digitsBeforeCaret = raw
          .slice(0, event.currentTarget.selectionStart ?? raw.length)
          .replaceAll(/[^\d]/g, "").length;
        const formatted = formatDigits(digits);
        event.currentTarget.value = formatted;
        setDisplayValue(formatted);
        requestAnimationFrame(() => {
          const position = caretAfterDigits(formatted, digitsBeforeCaret);
          input.current?.setSelectionRange(position, position);
        });
        const parsed = Number(digits);
        if (
          Number.isSafeInteger(parsed) &&
          (minimum === undefined || parsed >= minimum) &&
          (maximum === undefined || parsed <= maximum)
        )
          onChange(parsed);
      }}
    />
  );
}

function formatInteger(value: number | null): string {
  if (value === null) return "";
  return formatDigits(String(value));
}

function formatDigits(value: string): string {
  return value.replace(/\B(?=(\d{3})+(?!\d))/g, "\u202f");
}

function caretAfterDigits(value: string, digitCount: number): number {
  if (digitCount === 0) return 0;
  let seen = 0;
  for (let index = 0; index < value.length; index += 1) {
    if (/\d/.test(value[index]!)) seen += 1;
    if (seen === digitCount) return index + 1;
  }
  return value.length;
}
