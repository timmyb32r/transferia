import { useEffect, useId, useRef, useState } from "preact/hooks";

import { LatestJob } from "../effects";
import type { DynamicOptions } from "../generated/apiContract";
import { AutofillResistantInput } from "../ui/AutofillResistantField";
import { anchoredMenuStyle, useAnchoredOverlay } from "../ui/overlay";
import {
  pathBrowseQuery,
  pathSearchFragment,
  rankSearchResults,
} from "../ui/search";
import { SearchHighlight } from "../ui/SearchHighlight";
import { Button } from "../ui/Button";
import { useFormEnvironment } from "./formEnvironment";

const QUERY_DEBOUNCE_MS = 160;

function splitPathLabel(label: string) {
  const trailingSlash = label.endsWith("/") ? "/" : "";
  const path = trailingSlash === "" ? label : label.slice(0, -1);
  const separator = path.lastIndexOf("/");
  return {
    prefix: path.slice(0, separator + 1),
    name: path.slice(separator + 1),
    trailingSlash,
  };
}

export function DynamicPathControl({
  id,
  source,
  dependencies,
  value,
  disabled,
  onChange,
}: {
  id?: string | undefined;
  source: string;
  dependencies: Record<string, string>;
  value: string;
  disabled: boolean;
  onChange: (value: string) => void;
}) {
  const generatedId = useId();
  const menuId = `${id ?? generatedId}-path-listbox`;
  const { options: loadOptions } = useFormEnvironment();
  const [open, setOpen] = useState(false);
  const [options, setOptions] = useState<
    Array<{ value: string; label: string }>
  >([]);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();
  const root = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const job = useRef(new LatestJob<string, string, DynamicOptions>()).current;
  const dependencyKey = JSON.stringify(dependencies);

  const close = () => {
    setOpen(false);
    setActiveIndex(-1);
  };
  useAnchoredOverlay({ open, root, trigger: input, onClose: close });

  useEffect(() => {
    const incompleteYtsaurusRoot =
      source === "yandex.ytsaurus.tables" &&
      (value.trim() === "" || value.trim() === "/");
    if (!open || disabled || incompleteYtsaurusRoot) {
      setLoading(false);
      setOptions([]);
      setActiveIndex(-1);
      setError(undefined);
      return;
    }
    setLoading(true);
    setError(undefined);
    const timer = window.setTimeout(() => {
      void job
        .run(`${source}:${dependencyKey}:${value}`, source, (key, signal) =>
          loadOptions({
            key,
            query: pathBrowseQuery(value),
            dependencies,
            signal,
          }),
        )
        .then((result) => {
          if (result === undefined) return;
          setOptions(
            rankSearchResults(
              result.value.options,
              pathSearchFragment(value),
              (option) => option.label.split("/").filter(Boolean).at(-1) ?? "",
            ),
          );
          setActiveIndex(-1);
          setError(result.value.warning);
          setLoading(false);
        })
        .catch((reason: unknown) => {
          setOptions([]);
          setActiveIndex(-1);
          setError(reason instanceof Error ? reason.message : String(reason));
          setLoading(false);
        });
    }, QUERY_DEBOUNCE_MS);
    return () => {
      window.clearTimeout(timer);
      job.cancel();
    };
  }, [open, disabled, source, dependencyKey, value]);

  useEffect(() => {
    if (disabled) close();
  }, [disabled]);

  useEffect(() => {
    if (activeIndex < 0) return;
    root.current
      ?.querySelector<HTMLElement>(`[data-option-index="${activeIndex}"]`)
      ?.scrollIntoView?.({ block: "nearest" });
  }, [activeIndex]);

  const choose = (next: string) => {
    onChange(next);
    if (next.endsWith("/")) {
      setOpen(true);
      queueMicrotask(() => input.current?.focus());
    } else {
      close();
    }
  };

  const chooseAndLeave = (next: string) => {
    onChange(next);
    close();
  };

  return (
    <div ref={root} class={`dynamic-path ${open ? "open" : ""}`}>
      <div class="dynamic-path-input-wrap">
        <AutofillResistantInput
          id={id}
          inputRef={input}
          type="text"
          value={value}
          disabled={disabled}
          role="combobox"
          aria-autocomplete="list"
          aria-expanded={open}
          aria-controls={menuId}
          aria-activedescendant={
            activeIndex >= 0 ? `${menuId}-option-${activeIndex}` : undefined
          }
          placeholder="Start typing a path"
          onFocus={() => setOpen(true)}
          onInput={(event) => {
            onChange(event.currentTarget.value);
            setActiveIndex(-1);
            setOpen(true);
          }}
          onKeyDown={(event) => {
            if (event.key === "Escape") {
              event.preventDefault();
              close();
              return;
            }
            if (event.key === "ArrowDown") {
              event.preventDefault();
              setOpen(true);
              setActiveIndex((current) =>
                options.length === 0 ? -1 : (current + 1) % options.length,
              );
              return;
            }
            if (event.key === "ArrowUp") {
              event.preventDefault();
              setOpen(true);
              setActiveIndex((current) =>
                options.length === 0
                  ? -1
                  : (current <= 0 ? options.length : current) - 1,
              );
              return;
            }
            if (
              (event.key === "Tab" || event.key === "Enter") &&
              activeIndex >= 0
            ) {
              const active = options[activeIndex];
              if (active === undefined) return;
              if (event.key === "Enter") event.preventDefault();
              chooseAndLeave(active.value);
            }
          }}
        />
        <span class="dynamic-path-spinner-slot" aria-live="polite">
          {loading && (
            <span
              class="spinner"
              role="status"
              aria-label="Loading path suggestions"
            />
          )}
        </span>
      </div>
      {open && (
        <div
          id={menuId}
          class="select-menu select-menu-floating dynamic-path-menu"
          style={anchoredMenuStyle(input.current)}
          role="listbox"
          aria-label="Path suggestions"
        >
          {options.map((option, index) => {
            const directory = option.value.endsWith("/");
            const label = splitPathLabel(option.label);
            return (
              <button
                id={`${menuId}-option-${index}`}
                key={option.value}
                type="button"
                role="option"
                aria-selected={index === activeIndex}
                tabIndex={-1}
                data-option-index={index}
                class="select-option dynamic-path-option"
                onPointerEnter={() => setActiveIndex(index)}
                onPointerDown={(event) => {
                  if (event.button !== 0) return;
                  event.preventDefault();
                }}
                onClick={() => choose(option.value)}
              >
                <span class="dynamic-path-kind" aria-hidden="true">
                  {directory ? "▸" : ""}
                </span>
                <span>
                  {label.prefix}
                  <SearchHighlight
                    text={label.name}
                    query={pathSearchFragment(value)}
                  />
                  {label.trailingSlash}
                </span>
              </button>
            );
          })}
          {!loading && options.length === 0 && error === undefined && (
            <div class="select-empty">No matching paths</div>
          )}
          {!loading && error !== undefined && (
            <div class="select-empty dynamic-path-message" role="alert">
              <span>{error}</span>
              <Button
                shape="icon"
                class="dynamic-path-error-close"
                aria-label="Close path suggestion error"
                title="Close"
                onClick={close}
              >
                ×
              </Button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
