import { useId, useMemo } from "preact/hooks";

import { anchoredMenuStyle, dismissActiveTextSelection } from "./overlay";
import { rankSearchResults } from "./search";
import { SearchHighlight } from "./SearchHighlight";
import { useListbox } from "./useListbox";

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
  loading?: boolean;
  searchable?: boolean;
  clearable?: boolean;
  onOpen?: () => void;
  onChange: (value: string) => void;
}

export function SelectControl({
  id,
  value,
  placeholder,
  options,
  disabled = false,
  loading = false,
  searchable = true,
  clearable = true,
  onOpen,
  onChange,
}: SelectControlProps) {
  const generatedId = useId();
  const menuId = `${id ?? generatedId}-listbox`;
  const listbox = useListbox({ disabled, onOpen });
  const { open, query, root, trigger, close, toggle, setQuery, onKeyDown } =
    listbox;
  const selected = options.find((option) => option.value === value);
  const filtered = useMemo(() => {
    const clearOption = clearable
      ? (options.find((option) => option.value === "") ?? {
          value: "",
          label: placeholder,
        })
      : undefined;
    return [
      ...(clearOption === undefined ? [] : [clearOption]),
      ...rankSearchResults(
        options.filter((option) => option.value !== ""),
        query,
        (option) => option.label,
      ),
    ];
  }, [clearable, options, placeholder, query]);
  const choose = (next: string) => {
    if (disabled) return;
    const active = document.activeElement;
    if (active instanceof HTMLElement) active.blur();
    trigger.current?.blur();
    onChange(next);
    close();
  };
  return (
    <div
      ref={root}
      class={`select ${open ? "open" : ""}`}
      onKeyDown={onKeyDown}
    >
      <button
        id={id}
        ref={trigger}
        type="button"
        class="select-trigger"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={menuId}
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
        <span class="select-trigger-indicator" aria-hidden="true">
          {loading ? (
            <span class="spinner" />
          ) : (
            <svg class="chevron" viewBox="0 0 16 16" focusable="false">
              <path d="m3.5 6 4.5 4 4.5-4" />
            </svg>
          )}
        </span>
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
              name={`${menuId}-query`}
              autoComplete="new-password"
              autocapitalize="off"
              autocorrect="off"
              spellcheck={false}
              data-1p-ignore
              data-lpignore="true"
              data-form-type="other"
              placeholder="Search"
              value={query}
              onInput={(event) => setQuery(event.currentTarget.value)}
            />
          )}
          <div id={menuId} role="listbox" aria-label={placeholder}>
            {loading && (
              <div class="select-loading" role="status">
                <span class="spinner" aria-hidden="true" /> Loading…
              </div>
            )}
            {filtered.map((option) => (
              <button
                key={option.value}
                type="button"
                disabled={disabled}
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
                <SearchHighlight text={option.label} query={query} />
              </button>
            ))}
            {!loading && filtered.length === 0 && (
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
  const menuId = `${useId()}-listbox`;
  const { open, query, root, trigger, toggle, setQuery, onKeyDown } =
    useListbox({ disabled });
  const labels = values.map(
    (value) => options.find((option) => option.value === value)?.label ?? value,
  );
  const filtered = rankSearchResults(options, query, (option) => option.label);
  return (
    <div
      ref={root}
      class={`select multi-select ${open ? "open" : ""}`}
      onKeyDown={onKeyDown}
    >
      <button
        ref={trigger}
        type="button"
        class="select-trigger"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={menuId}
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
          id={menuId}
          class="select-menu select-menu-floating"
          style={anchoredMenuStyle(trigger.current)}
          role="listbox"
          aria-multiselectable="true"
        >
          <input
            class="select-search"
            type="search"
            name={`${menuId}-query`}
            autoComplete="new-password"
            autocapitalize="off"
            autocorrect="off"
            spellcheck={false}
            data-1p-ignore
            data-lpignore="true"
            data-form-type="other"
            placeholder="Search"
            value={query}
            onInput={(event) => setQuery(event.currentTarget.value)}
          />
          <button
            type="button"
            disabled={disabled}
            role="option"
            aria-selected={values.length === 0}
            class="select-option multi-select-option"
            onPointerDown={(event) => {
              if (event.button !== 0) return;
              event.preventDefault();
              dismissActiveTextSelection();
              onChange([]);
            }}
            onClick={(event) => {
              if (event.detail === 0) onChange([]);
            }}
          >
            <span class={`multi-check ${values.length === 0 ? "checked" : ""}`}>
              {values.length === 0 ? "✓" : ""}
            </span>
            {placeholder}
          </button>
          {filtered.map((option) => {
            const selected = values.includes(option.value);
            const choose = () =>
              !disabled &&
              onChange(
                selected
                  ? values.filter((value) => value !== option.value)
                  : [...values, option.value],
              );
            return (
              <button
                key={option.value}
                type="button"
                disabled={disabled}
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
                <SearchHighlight text={option.label} query={query} />
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
