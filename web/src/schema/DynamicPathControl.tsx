import { useEffect, useId, useRef, useState } from "preact/hooks";

import { LatestJob } from "../effects";
import type { DynamicOptions } from "../generated/apiContract";
import { anchoredMenuStyle, useAnchoredOverlay } from "../ui/overlay";
import {
  pathBrowseQuery,
  pathSearchFragment,
  rankSearchResults,
} from "../ui/search";
import { useFormEnvironment } from "./formEnvironment";

const QUERY_DEBOUNCE_MS = 160;

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
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();
  const root = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const job = useRef(new LatestJob<string, string, DynamicOptions>()).current;
  const dependencyKey = JSON.stringify(dependencies);

  const close = () => setOpen(false);
  useAnchoredOverlay({ open, root, trigger: input, onClose: close });

  useEffect(() => {
    if (!open || disabled) {
      setLoading(false);
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
          setError(result.value.warning);
          setLoading(false);
        })
        .catch((reason: unknown) => {
          setOptions([]);
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

  const choose = (next: string) => {
    onChange(next);
    if (next.endsWith("/")) {
      setOpen(true);
      queueMicrotask(() => input.current?.focus());
    } else {
      close();
    }
  };

  return (
    <div ref={root} class={`dynamic-path ${open ? "open" : ""}`}>
      <div class="dynamic-path-input-wrap">
        <input
          id={id}
          ref={input}
          type="text"
          name={`transferia-${id ?? menuId}`}
          autoComplete="off"
          value={value}
          disabled={disabled}
          role="combobox"
          aria-autocomplete="list"
          aria-expanded={open}
          aria-controls={menuId}
          placeholder="Start typing a path"
          onFocus={() => setOpen(true)}
          onInput={(event) => {
            onChange(event.currentTarget.value);
            setOpen(true);
          }}
          onKeyDown={(event) => {
            if (event.key === "Escape") {
              event.preventDefault();
              close();
            }
            if (event.key === "ArrowDown") {
              event.preventDefault();
              setOpen(true);
              queueMicrotask(() =>
                root.current
                  ?.querySelector<HTMLButtonElement>('[role="option"]')
                  ?.focus(),
              );
            }
          }}
        />
        {loading && (
          <span class="spinner dynamic-path-spinner" aria-label="Loading" />
        )}
      </div>
      {open && (
        <div
          id={menuId}
          class="select-menu select-menu-floating dynamic-path-menu"
          style={anchoredMenuStyle(input.current)}
          role="listbox"
          aria-label="Path suggestions"
        >
          {options.map((option) => {
            const directory = option.value.endsWith("/");
            return (
              <button
                key={option.value}
                type="button"
                role="option"
                aria-selected={option.value === value}
                class="select-option dynamic-path-option"
                onPointerDown={(event) => {
                  if (event.button !== 0) return;
                  event.preventDefault();
                }}
                onClick={() => choose(option.value)}
              >
                <span class="dynamic-path-kind" aria-hidden="true">
                  {directory ? "▸" : ""}
                </span>
                <span>{option.label}</span>
              </button>
            );
          })}
          {!loading && options.length === 0 && error === undefined && (
            <div class="select-empty">No matching paths</div>
          )}
          {!loading && error !== undefined && (
            <div class="select-empty dynamic-path-message">{error}</div>
          )}
        </div>
      )}
    </div>
  );
}
