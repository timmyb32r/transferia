import { Button } from "./Button";

export function SegmentedControl<T extends string>({ label, value, options, disabled = false, onChange }: {
  label: string; value: T; options: readonly { value: T; label: string }[];
  disabled?: boolean; onChange: (value: T) => void;
}) {
  return <div class="segmented-control" role="radiogroup" aria-label={label}>
    {options.map((option, index) => <Button variant="plain" role="radio" aria-checked={value === option.value}
      disabled={disabled} tabIndex={value === option.value ? 0 : -1}
      onClick={() => onChange(option.value)} onKeyDown={event => {
        const step = event.key === "ArrowRight" || event.key === "ArrowDown" ? 1
          : event.key === "ArrowLeft" || event.key === "ArrowUp" ? -1 : 0;
        if (!step && event.key !== "Home" && event.key !== "End") return;
        event.preventDefault();
        const next = event.key === "Home" ? 0 : event.key === "End" ? options.length - 1
          : (index + step + options.length) % options.length;
        onChange(options[next]!.value);
        (event.currentTarget.parentElement?.children[next] as HTMLElement | undefined)?.focus();
      }}>{option.label}</Button>)}
  </div>;
}
