import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { createPortal } from "preact/compat";
import type { PatternMode, TableIdentity } from "../../generated/apiContract";
import { useTableCatalog } from "../../schema/tableCatalog";
import { AutofillResistantInput } from "../../ui/AutofillResistantField";
import { Button } from "../../ui/Button";
import { SearchIcon } from "../../ui/icons";
import { anchoredMenuStyle, useAnchoredOverlay } from "../../ui/overlay";
import { completionPattern, exactPattern, literalPatternPrefix, qualifiedName } from "./model";
import { useTableNamespace } from "./naming";

const GLOB_HELP = "Glob / wildcard: * matches any number of characters; ? matches one character. Matching starts at the beginning of the qualified name. Click to enable regex.";
const REGEX_HELP = "Regex is enabled. The expression matches the entire qualified name. Use .* for any characters and . for one character. Click to use glob / wildcard.";

export function TablePatternInput({ id, label, value, mode, disabled, required, invalid, onChange, onModeChange, placeholder, onBrowse, confirmed }: {
  id: string; label: string; value: string; mode: PatternMode; disabled: boolean;
  required: boolean; invalid: boolean;
  placeholder?: string;
  onBrowse?: (() => void) | undefined;
  confirmed?: boolean | undefined;
  onChange: (value: string) => void; onModeChange: (mode: PatternMode) => void;
}) {
  const catalog = useTableCatalog();
  const namespace = useTableNamespace();
  const root = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const [focused, setFocused] = useState(false);
  const [active, setActive] = useState(-1);
  const [fullName, setFullName] = useState<{ left: number; top: number; above: boolean }>();
  const hideFullName = () => setFullName(undefined);
  const showFullName = () => {
    const element = input.current;
    if (!element || !value) return;
    const style = getComputedStyle(element);
    const measure = document.createElement("canvas").getContext("2d");
    if (!measure) return;
    measure.font = style.font;
    const width = measure.measureText(value).width + (parseFloat(style.letterSpacing) || 0) * Math.max(0, value.length - 1);
    if (width <= element.clientWidth - (parseFloat(style.paddingLeft) || 0) - (parseFloat(style.paddingRight) || 0)) return;
    const rect = element.getBoundingClientRect();
    const above = rect.bottom + 100 > window.innerHeight;
    setFullName({ left: Math.max(12, Math.min(rect.left, window.innerWidth - Math.min(680, window.innerWidth - 24) - 12)),
      top: above ? rect.top - 6 : rect.bottom + 6, above });
  };
  useLayoutEffect(hideFullName, [value, disabled]);
  useEffect(() => {
    if (!fullName) return;
    window.addEventListener("scroll", hideFullName, true);
    window.addEventListener("resize", hideFullName);
    return () => { window.removeEventListener("scroll", hideFullName, true); window.removeEventListener("resize", hideFullName); };
  }, [Boolean(fullName)]);
  const [result, setResult] = useState<{ key: string; tables: TableIdentity[]; matches: TableIdentity[] }>();
  const key = JSON.stringify([value, mode]);
  const tables = catalog?.tables;
  const preview = catalog?.preview;
  const open = focused && value.length > 0 && !disabled && tables !== undefined && preview !== undefined;
  const current = result?.key === key && result.tables === tables ? result.matches : undefined;
  const suggestions = current?.slice(0, 30) ?? [];
  useAnchoredOverlay({ open, root, trigger: input, onClose: () => setFocused(false) });
  useEffect(() => {
    setActive(-1);
    if (!open || !tables || !preview) return;
    const controller = new AbortController();
    // Reuse the production matcher: completion is prefix-based for plain glob
    // input, and follows exact glob/regex semantics once a pattern is present.
    const timer = setTimeout(() => {
      void preview({ catalog: tables, selection: { type: "selected", rules: [{
        include: completionPattern(value, mode), include_mode: mode,
      }] } }, controller.signal).then(response => {
        if (!controller.signal.aborted) setResult({ key, tables, matches: response.cards[0]?.selected ?? [] });
      }).catch(() => {
        // Incomplete syntax while typing has no completions. The rule preview
        // reports validation errors; a suggestion must not add another error.
        if (!controller.signal.aborted) setResult({ key, tables, matches: [] });
      });
    }, 150);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [key, open, tables, preview]);
  const choose = (table: TableIdentity) => {
    onChange(exactPattern(table, mode));
    input.current?.focus({ preventScroll: true });
    setFocused(false);
    setActive(-1);
  };
  const prefix = literalPatternPrefix(value, mode);
  return <div class={`table-pattern-input${onBrowse ? " table-pattern-with-browser" : ""}${confirmed !== undefined ? " table-pattern-with-confirmation" : ""}`} ref={root} onBlur={event => {
    if (!root.current?.contains(event.relatedTarget as Node | null)) setFocused(false);
  }} onKeyDown={event => {
    if (event.key !== "Escape" || !catalog) return;
    event.preventDefault();
    event.stopPropagation();
    input.current?.focus({ preventScroll: true });
    setFocused(false);
    setActive(-1);
  }}>
    <AutofillResistantInput inputRef={input} type="text" id={id} role={catalog ? "combobox" : undefined}
      aria-label={label} aria-autocomplete={catalog ? "list" : undefined} aria-expanded={catalog ? open : undefined} aria-controls={catalog ? `${id}-suggestions` : undefined}
      aria-activedescendant={open && active >= 0 ? `${id}-suggestion-${active}` : undefined}
      aria-invalid={invalid} required={required}
      aria-describedby={fullName ? `${id}-full-name` : undefined}
      placeholder={placeholder ?? (required ? `${namespace}.table or ${namespace}.*` : "Optional pattern")}
      value={value} disabled={disabled} onFocus={() => setFocused(true)}
      onMouseEnter={showFullName} onMouseLeave={hideFullName}
      onInput={event => { hideFullName(); setFocused(true); onChange(event.currentTarget.value); }}
      onKeyDown={event => {
        if (event.key === "ArrowDown" || event.key === "ArrowUp") {
          event.preventDefault(); setFocused(true);
          if (suggestions.length) {
            const next = event.key === "ArrowDown" ? (active + 1) % suggestions.length
              : (active <= 0 ? suggestions.length : active) - 1;
            setActive(next);
            document.getElementById(`${id}-suggestion-${next}`)?.scrollIntoView?.({ block: "nearest" });
          }
        } else if (event.key === "Enter" && !event.isComposing) {
          event.preventDefault();
          event.stopPropagation();
          setFocused(false);
          setActive(-1);
          event.currentTarget.blur();
        }
      }} />
    {confirmed !== undefined && <span class="table-pattern-confirmation" aria-live="polite">
      {confirmed && <span role="img" aria-label="Table found" title="Table found">✓</span>}
    </span>}
    {onBrowse && <Button variant="plain" shape="icon" class="table-pattern-browse" aria-label={`Browse tables for ${label}`}
      title="Browse available tables" aria-haspopup="dialog" disabled={disabled || !catalog}
      onClick={() => { setFocused(false); setActive(-1); onBrowse(); }}><SearchIcon /></Button>}
    <Button variant="plain" shape="icon" class="regex-toggle" aria-label={label.includes(" rule") ? label.toLowerCase().replace(" rule", " regex rule") : `${label} regex`}
      aria-pressed={mode === "regex"} title={mode === "regex" ? REGEX_HELP : GLOB_HELP}
      disabled={disabled} onClick={() => onModeChange(mode === "regex" ? "glob" : "regex")}>.*</Button>
    {open && <div class="select-menu select-menu-floating table-suggestions"
      style={anchoredMenuStyle(input.current, { estimatedHeight: 160, maxHeight: 224 })}>
      <div id={`${id}-suggestions`} role="listbox" aria-label={`${label} suggestions`} aria-busy={current === undefined}>
        {current === undefined ? <div class="select-empty">Searching…</div>
          : suggestions.length === 0 ? <div class="select-empty">No matching tables</div>
          : suggestions.map((table, index) => {
            const name = qualifiedName(table);
            return <Button variant="plain" id={`${id}-suggestion-${index}`} key={name} role="option" aria-selected={index === active}
              tabIndex={-1} class="select-option" onPointerDown={event => event.preventDefault()}
              onClick={() => choose(table)}>
              {prefix && name.startsWith(prefix) ? <><strong>{name.slice(0, prefix.length)}</strong>{name.slice(prefix.length)}</> : name}
            </Button>;
          })}
      </div>
      {current && current.length > suggestions.length && <div class="select-empty">Showing {suggestions.length} of {current.length}; keep typing to narrow the list.</div>}
    </div>}
    {fullName && createPortal(<span id={`${id}-full-name`} role="tooltip"
      class={`table-pattern-tooltip${fullName.above ? " table-pattern-tooltip-above" : ""}`}
      style={{ left: fullName.left, top: fullName.top }}>{value}</span>, document.body)}
  </div>;
}
