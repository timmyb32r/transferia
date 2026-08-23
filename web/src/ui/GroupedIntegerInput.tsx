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
  const focused = useRef(false);
  const [displayValue, setDisplayValue] = useState(() => formatInteger(value));

  useEffect(() => {
    if (!focused.current) setDisplayValue(formatInteger(value));
  }, [value]);

  return (
    <AutofillResistantInput
      id={id}
      type="text"
      inputMode="numeric"
      value={displayValue}
      disabled={disabled}
      onFocus={() => {
        focused.current = true;
        setDisplayValue(value === null ? "" : String(value));
      }}
      onBlur={() => {
        focused.current = false;
        setDisplayValue(formatInteger(value));
      }}
      onInput={(event) => {
        const raw = event.currentTarget.value;
        setDisplayValue(raw);
        const digits = raw.replaceAll(/[\s,_]/g, "");
        if (digits === "") {
          onChange(null);
          return;
        }
        if (!/^\d+$/.test(digits)) return;
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
  return String(value).replace(/\B(?=(\d{3})+(?!\d))/g, "\u202f");
}
