import { useMemo, useRef, useState } from "preact/hooks";

import {
  anchoredMenuStyle,
  dismissActiveTextSelection,
  useAnchoredOverlay,
} from "./overlay";

export interface SelectOption {
  value: string;
  label: string;
}

interface SelectControlProps {
  id?: string | undefined;
  value: string;
  placeholder: string;
  options: SelectOption[];
  disabled?: boolean;
  searchable?: boolean;
  onOpen?: () => void;
  onChange: (value: string) => void;
}

export function SelectControl({
  id,
  value,
  placeholder,
  options,
  disabled = false,
  searchable = false,
  onOpen,
  onChange,
}: SelectControlProps) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const selected = options.find((option) => option.value === value);
  const filtered = useMemo(
    () =>
      options.filter((option) =>
        option.label.toLowerCase().includes(query.toLowerCase()),
      ),
    [options, query],
  );
  const close = () => {
    setOpen(false);
    setQuery("");
  };
  const toggle = () => {
    setQuery("");
    setOpen((current) => {
      if (!current) onOpen?.();
      return !current;
    });
  };
  useAnchoredOverlay({ open, root, trigger, onClose: close });
  const choose = (next: string) => {
    onChange(next);
    close();
  };
  return (
    <div
      ref={root}
      class={`select ${open ? "open" : ""}`}
      onKeyDown={(event) =>
        handleSelectKeyDown(
          event,
          open,
          () => {
            onOpen?.();
            setOpen(true);
          },
          close,
          root,
          trigger,
        )
      }
    >
      <button
        id={id}
        ref={trigger}
        type="button"
        class="select-trigger"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        onPointerDown={(event) => {
          if (event.button !== 0) return;
          event.preventDefault();
          dismissActiveTextSelection();
          trigger.current?.focus({ preventScroll: true });
          toggle();
        }}
        onClick={(event) => {
          if (event.detail === 0) toggle();
        }}
      >
        <span class={selected === undefined ? "placeholder" : ""}>
          {selected?.label ?? placeholder}
        </span>
        <svg
          class="chevron"
          viewBox="0 0 16 16"
          aria-hidden="true"
          focusable="false"
        >
          <path d="m3.5 6 4.5 4 4.5-4" />
        </svg>
      </button>
      {open && (
        <div
          class="select-menu select-menu-floating"
          style={anchoredMenuStyle(trigger.current)}
        >
          {searchable && (
            <input
              class="select-search"
              type="search"
              placeholder="Search"
              value={query}
              onInput={(event) => setQuery(event.currentTarget.value)}
            />
          )}
          <div role="listbox">
            {filtered.map((option) => (
              <button
                key={option.value}
                type="button"
                role="option"
                aria-selected={option.value === value}
                class="select-option"
                onPointerDown={(event) => {
                  if (event.button !== 0) return;
                  event.preventDefault();
                  dismissActiveTextSelection();
                  choose(option.value);
                }}
                onClick={(event) => {
                  if (event.detail === 0) choose(option.value);
                }}
              >
                {option.label}
              </button>
            ))}
            {filtered.length === 0 && (
              <div class="select-empty">No matches</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export function MultiSelectControl({
  values,
  placeholder,
  options,
  disabled,
  onChange,
}: {
  values: string[];
  placeholder: string;
  options: SelectOption[];
  disabled: boolean;
  onChange: (values: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const close = () => {
    setOpen(false);
    setQuery("");
  };
  useAnchoredOverlay({ open, root, trigger, onClose: close });
  const labels = values.map(
    (value) => options.find((option) => option.value === value)?.label ?? value,
  );
  const filtered = options.filter((option) =>
    option.label.toLowerCase().includes(query.toLowerCase()),
  );
  const toggle = () => {
    setQuery("");
    setOpen((current) => !current);
  };
  return (
    <div
      ref={root}
      class={`select multi-select ${open ? "open" : ""}`}
      onKeyDown={(event) =>
        handleSelectKeyDown(
          event,
          open,
          () => setOpen(true),
          close,
          root,
          trigger,
        )
      }
    >
      <button
        ref={trigger}
        type="button"
        class="select-trigger"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        onPointerDown={(event) => {
          if (event.button !== 0) return;
          event.preventDefault();
          dismissActiveTextSelection();
          trigger.current?.focus({ preventScroll: true });
          toggle();
        }}
        onClick={(event) => {
          if (event.detail === 0) toggle();
        }}
      >
        <span class={labels.length === 0 ? "placeholder" : ""}>
          {labels.length === 0 ? placeholder : labels.join(", ")}
        </span>
        <svg class="chevron" viewBox="0 0 16 16" aria-hidden="true">
          <path d="m3.5 6 4.5 4 4.5-4" />
        </svg>
      </button>
      {open && (
        <div
          class="select-menu select-menu-floating"
          style={anchoredMenuStyle(trigger.current)}
          role="listbox"
          aria-multiselectable="true"
        >
          <input
            class="select-search"
            type="search"
            placeholder="Search"
            value={query}
            onInput={(event) => setQuery(event.currentTarget.value)}
          />
          {filtered.map((option) => {
            const selected = values.includes(option.value);
            const choose = () =>
              onChange(
                selected
                  ? values.filter((value) => value !== option.value)
                  : [...values, option.value],
              );
            return (
              <button
                key={option.value}
                type="button"
                role="option"
                aria-selected={selected}
                class="select-option multi-select-option"
                onPointerDown={(event) => {
                  if (event.button !== 0) return;
                  event.preventDefault();
                  dismissActiveTextSelection();
                  choose();
                }}
                onClick={(event) => {
                  if (event.detail === 0) choose();
                }}
              >
                <span class={`multi-check ${selected ? "checked" : ""}`}>
                  {selected ? "✓" : ""}
                </span>
                {option.label}
              </button>
            );
          })}
          {filtered.length === 0 && (
            <div class="select-empty">
              {options.length === 0 ? "Add output columns first" : "No matches"}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function handleSelectKeyDown(
  event: KeyboardEvent,
  open: boolean,
  openMenu: () => void,
  closeMenu: () => void,
  root: { current: HTMLDivElement | null },
  trigger: { current: HTMLButtonElement | null },
): void {
  if (event.key === "Escape" && open) {
    event.preventDefault();
    closeMenu();
    trigger.current?.focus();
    return;
  }
  if (
    (event.key !== "ArrowDown" && event.key !== "ArrowUp") ||
    !(event.target instanceof HTMLButtonElement)
  )
    return;
  event.preventDefault();
  if (!open) {
    const direction = event.key;
    openMenu();
    queueMicrotask(() => {
      const options = [
        ...(root.current?.querySelectorAll<HTMLButtonElement>(
          '[role="option"]',
        ) ?? []),
      ];
      const target =
        direction === "ArrowDown" ? options[0] : options[options.length - 1];
      target?.focus();
    });
    return;
  }
  const options = [
    ...(root.current?.querySelectorAll<HTMLButtonElement>('[role="option"]') ??
      []),
  ];
  const current = options.indexOf(event.target);
  if (current < 0 || options.length === 0) return;
  const direction = event.key === "ArrowDown" ? 1 : -1;
  options[(current + direction + options.length) % options.length]?.focus();
}
