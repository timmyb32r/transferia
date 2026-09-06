/** A single explicit connection gesture may reveal Tables, unless the user
 * moved on while the request was pending. Polling and Validate never call this. */
export function beginMetadataReveal(target: () => HTMLElement | null, isCurrent: () => boolean) {
  let cancelled = false;
  let frame: number | undefined;
  const trigger = document.activeElement;
  const cancel = () => {
    cancelled = true;
    if (frame !== undefined) cancelAnimationFrame(frame);
    for (const event of ["pointerdown", "keydown", "wheel", "touchstart"]) window.removeEventListener(event, cancel, true);
    document.removeEventListener("focusin", focusChanged, true);
  };
  const focusChanged = (event: Event) => { if (event.target !== trigger) cancel(); };
  for (const event of ["pointerdown", "keydown", "wheel", "touchstart"]) window.addEventListener(event, cancel, { capture: true, passive: true });
  document.addEventListener("focusin", focusChanged, true);
  return {
    cancel,
    complete: () => {
      if (cancelled) return;
      frame = requestAnimationFrame(() => {
        frame = requestAnimationFrame(() => {
          if (!cancelled && isCurrent()) target()?.scrollIntoView({
            block: "start", behavior: window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth",
          });
          cancel();
        });
      });
    },
  };
}
