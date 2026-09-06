import { createPortal } from "preact/compat";
import { useEffect, useId, useLayoutEffect, useRef, useState } from "preact/hooks";
import { Button } from "./Button";

export type CopyState = "idle" | "copying" | "copied" | "error";

export function CopyIcon({ copied = false, failed = false }: { copied?: boolean; failed?: boolean }) {
  return <span class="ui-icon copy-icon" aria-hidden="true">
    {copied && <span class="copy-icon-check" />}
    {failed && <span class="copy-icon-failed" />}
  </span>;
}

export function CopyButton({ text, label = "Copy", class: className, framed = false, disabled = false, resetKey, lock, onStateChange }: {
  text: string | (() => string);
  label?: string;
  class?: string;
  framed?: boolean;
  disabled?: boolean;
  /** Invalidate feedback when the content of a lazy text producer changes. */
  resetKey?: unknown;
  /** Serialize clipboard writes across rows in a single list. */
  lock?: { current: boolean };
  onStateChange?: (state: CopyState) => void;
}) {
  const [state, setState] = useState<CopyState>("idle");
  const [anchor, setAnchor] = useState<{ left: number; top: number; above: boolean }>();
  const button = useRef<HTMLButtonElement>(null);
  const busy = useRef(false);
  const mounted = useRef(true);
  const timer = useRef<ReturnType<typeof setTimeout>>();
  const identity = typeof text === "string" ? text : resetKey;
  const currentIdentity = useRef(identity);
  currentIdentity.current = identity;
  const feedback = useRef(onStateChange);
  feedback.current = onStateChange;
  const id = useId();
  const publish = (next: CopyState) => { setState(next); feedback.current?.(next); };
  const hide = () => { clearTimeout(timer.current); setAnchor(undefined); };
  const show = () => {
    clearTimeout(timer.current);
    const rect = button.current?.getBoundingClientRect();
    if (!rect) return;
    const above = rect.bottom + 44 > window.innerHeight;
    setAnchor({ left: Math.max(56, Math.min(window.innerWidth - 56, rect.left + rect.width / 2)), top: above ? rect.top - 6 : rect.bottom + 6, above });
  };
  const schedule = () => {
    if (disabled) return;
    clearTimeout(timer.current);
    // One delayed overlay, never a second native title tooltip. Click feedback
    // bypasses this delay and updates the already-reserved icon immediately.
    timer.current = setTimeout(show, 350);
  };
  useLayoutEffect(() => { if (!busy.current) publish("idle"); }, [identity]);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      clearTimeout(timer.current);
    };
  }, []);
  useEffect(() => {
    if (!anchor) return;
    window.addEventListener("scroll", hide, true);
    window.addEventListener("resize", hide);
    return () => {
      window.removeEventListener("scroll", hide, true);
      window.removeEventListener("resize", hide);
    };
  }, [Boolean(anchor)]);
  const copy = async () => {
    if (busy.current || lock?.current || disabled) return;
    busy.current = true;
    if (lock) lock.current = true;
    const copiedIdentity = identity;
    publish("copying");
    show();
    let next: CopyState = "copied";
    try { await navigator.clipboard.writeText(typeof text === "string" ? text : text()); }
    catch { next = "error"; }
    finally { busy.current = false; if (lock) lock.current = false; }
    if (mounted.current) publish(Object.is(copiedIdentity, currentIdentity.current) ? next : "idle");
    else feedback.current?.("idle");
  };
  const tooltip = state === "copied" ? "Copied" : state === "copying" ? "Copying…" : state === "error" ? "Copy failed" : "Copy";
  return <>
    <Button variant="plain" shape="icon" class={["copy-action", framed ? "copy-action-framed" : "", className].filter(Boolean).join(" ")}
      buttonRef={button} disabled={disabled} pending={state === "copying"} aria-label={label}
      aria-describedby={anchor ? id : undefined} data-copy-state={state}
      onMouseEnter={schedule} onMouseLeave={hide} onFocus={schedule} onBlur={hide}
      onKeyDown={event => {
        if (event.key === "Enter" || event.key === " ") event.stopPropagation();
        if (event.key === "Escape") { if (anchor) event.stopPropagation(); hide(); }
      }}
      onClick={event => { event.stopPropagation(); void copy(); }}>
      <CopyIcon copied={state === "copied"} failed={state === "error"} />
      {!onStateChange && <span class="visually-hidden" aria-live="polite">{state === "idle" ? "" : tooltip}</span>}
    </Button>
    {anchor && createPortal(<span id={id} class={`copy-tooltip${anchor.above ? " copy-tooltip-above" : ""}`} role="tooltip"
      style={{ left: anchor.left, top: anchor.top }}>{tooltip}</span>, document.body)}
  </>;
}
