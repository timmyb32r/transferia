import { useEffect, useId, useMemo, useRef, useState } from "preact/hooks";

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
import { YTsaurusFolderIcon, YTsaurusTableIcon } from "../ui/icons";
import { useFormEnvironment } from "./formEnvironment";

const QUERY_DEBOUNCE_MS = 160;
const DIRECTORY_CACHE_CAPACITY = 64;

function readCachedDirectory(
  cache: Map<string, DynamicOptions>,
  key: string,
): DynamicOptions | undefined {
  const cached = cache.get(key);
  if (cached === undefined) return undefined;
  cache.delete(key);
  cache.set(key, cached);
  return cached;
}

function cacheDirectory(
  cache: Map<string, DynamicOptions>,
  key: string,
  value: DynamicOptions,
) {
  cache.delete(key);
  cache.set(key, value);
  while (cache.size > DIRECTORY_CACHE_CAPACITY) {
    const oldest = cache.keys().next().value;
    if (oldest === undefined) return;
    cache.delete(oldest);
  }
}

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
  const [directoryOptions, setDirectoryOptions] = useState<
    Array<{ value: string; label: string }>
  >([]);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();
  const root = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const keyboardSelection = useRef(false);
  const job = useRef(new LatestJob<string, string, DynamicOptions>()).current;
  const directoryCache = useRef(new Map<string, DynamicOptions>()).current;
  const dependencyKey = JSON.stringify(
    Object.entries(dependencies).sort(([left], [right]) =>
      left.localeCompare(right),
    ),
  );
  const browseQuery = pathBrowseQuery(value);
  const searchFragment = pathSearchFragment(value);
  const directoryCacheKey = `${source}\u0000${dependencyKey}\u0000${browseQuery}`;
  const incompleteYtsaurusRoot =
    source === "yandex.ytsaurus.tables" &&
    (value.trim() === "" || value.trim() === "/");
  const options = useMemo(
    () =>
      rankSearchResults(directoryOptions, searchFragment, (option) =>
        option.label.split("/").filter(Boolean).at(-1) ?? "",
      ),
    [directoryOptions, searchFragment],
  );
  const highlightedIndex =
    open && options.length > 0
      ? activeIndex >= 0 && activeIndex < options.length
        ? activeIndex
        : 0
      : -1;

  const close = () => {
    keyboardSelection.current = false;
    setOpen(false);
    setActiveIndex(-1);
  };
  useAnchoredOverlay({ open, root, trigger: input, onClose: close });

  useEffect(() => {
    if (!open || disabled || incompleteYtsaurusRoot) {
      setLoading(false);
      setDirectoryOptions([]);
      setActiveIndex(-1);
      setError(undefined);
      return;
    }
    const cached = readCachedDirectory(directoryCache, directoryCacheKey);
    if (cached !== undefined) {
      setDirectoryOptions(cached.options);
      setActiveIndex(cached.options.length === 0 ? -1 : 0);
      setError(cached.warning);
      setLoading(false);
      return;
    }
    setLoading(true);
    setDirectoryOptions([]);
    setError(undefined);
    const timer = window.setTimeout(() => {
      void job
        .run(directoryCacheKey, source, (key, signal) =>
          loadOptions({
            key,
            query: browseQuery,
            dependencies,
            signal,
          }),
        )
        .then((result) => {
          if (result === undefined) return;
          if (result.value.warning === undefined)
            cacheDirectory(directoryCache, directoryCacheKey, result.value);
          setDirectoryOptions(result.value.options);
          setActiveIndex(result.value.options.length === 0 ? -1 : 0);
          setError(result.value.warning);
          setLoading(false);
        })
        .catch((reason: unknown) => {
          setDirectoryOptions([]);
          setActiveIndex(-1);
          setError(reason instanceof Error ? reason.message : String(reason));
          setLoading(false);
        });
    }, QUERY_DEBOUNCE_MS);
    return () => {
      window.clearTimeout(timer);
      job.cancel();
    };
  }, [
    open,
    disabled,
    source,
    directoryCacheKey,
    browseQuery,
    incompleteYtsaurusRoot,
  ]);

  useEffect(() => {
    if (disabled) close();
  }, [disabled]);

  useEffect(() => {
    if (highlightedIndex < 0) return;
    root.current
      ?.querySelector<HTMLElement>(`[data-option-index="${highlightedIndex}"]`)
      ?.scrollIntoView?.({ block: "nearest" });
  }, [highlightedIndex]);

  const choose = (next: string) => {
    keyboardSelection.current = false;
    onChange(next);
    if (next.endsWith("/")) {
      setOpen(true);
    } else {
      close();
    }
    queueMicrotask(() => {
      input.current?.focus();
      input.current?.setSelectionRange(next.length, next.length);
    });
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
            highlightedIndex >= 0
              ? `${menuId}-option-${highlightedIndex}`
              : undefined
          }
          placeholder="Start typing a path"
          onFocus={() => setOpen(true)}
          onInput={(event) => {
            keyboardSelection.current = false;
            onChange(event.currentTarget.value);
            setActiveIndex(options.length === 0 ? -1 : 0);
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
              keyboardSelection.current = true;
              setOpen(true);
              setActiveIndex(
                options.length === 0
                  ? -1
                  : (highlightedIndex + 1) % options.length,
              );
              return;
            }
            if (event.key === "ArrowUp") {
              event.preventDefault();
              keyboardSelection.current = true;
              setOpen(true);
              setActiveIndex(
                options.length === 0
                  ? -1
                  : (highlightedIndex <= 0
                      ? options.length
                      : highlightedIndex) - 1,
              );
              return;
            }
            const acceptsActiveOption =
              (event.key === "Tab" || event.key === "Enter") &&
              highlightedIndex >= 0;
            const acceptsKeyboardSelection =
              event.key === "ArrowRight" &&
              keyboardSelection.current &&
              highlightedIndex >= 0;
            const acceptsFirstOption =
              event.key === "Tab" && highlightedIndex < 0 && options.length > 0;
            if (
              acceptsActiveOption ||
              acceptsKeyboardSelection ||
              acceptsFirstOption
            ) {
              const active = options[highlightedIndex < 0 ? 0 : highlightedIndex];
              if (active === undefined) return;
              event.preventDefault();
              choose(active.value);
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
                aria-selected={index === highlightedIndex}
                tabIndex={-1}
                data-option-index={index}
                class="select-option dynamic-path-option"
                onPointerEnter={() => {
                  keyboardSelection.current = false;
                  setActiveIndex(index);
                }}
                onPointerDown={(event) => {
                  if (event.button !== 0) return;
                  event.preventDefault();
                }}
                onClick={() => choose(option.value)}
              >
                <span class="dynamic-path-kind" aria-hidden="true">
                  {directory ? <YTsaurusFolderIcon /> : <YTsaurusTableIcon />}
                </span>
                <span class="dynamic-path-label" title={option.label}>
                  <strong class="dynamic-path-prefix">{label.prefix}</strong>
                  <SearchHighlight
                    text={label.name}
                    query={searchFragment}
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
