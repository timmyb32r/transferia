import type { RefObject } from "preact";
import { useEffect, useState } from "preact/hooks";

export function useAnchoredOverlay({
  open,
  root,
  trigger,
  onClose,
  closeOnViewportChange = false,
}: {
  open: boolean;
  root: RefObject<HTMLElement>;
  trigger: RefObject<HTMLElement>;
  onClose: () => void;
  closeOnViewportChange?: boolean;
}): void {
  const [, refreshPosition] = useState(0);
  useEffect(() => {
    if (!open) return;
    const closeOutside = (event: PointerEvent) => {
      if (!root.current?.contains(event.target as Node)) onClose();
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      onClose();
      trigger.current?.focus();
    };
    const viewportChanged = () => {
      if (closeOnViewportChange) onClose();
      else refreshPosition((revision) => revision + 1);
    };
    document.addEventListener("pointerdown", closeOutside);
    document.addEventListener("keydown", closeOnEscape);
    window.addEventListener("resize", viewportChanged);
    window.addEventListener("scroll", viewportChanged, true);
    return () => {
      document.removeEventListener("pointerdown", closeOutside);
      document.removeEventListener("keydown", closeOnEscape);
      window.removeEventListener("resize", viewportChanged);
      window.removeEventListener("scroll", viewportChanged, true);
    };
  }, [open, closeOnViewportChange, onClose]);
}

export function anchoredMenuStyle(
  trigger: HTMLElement | null,
  {
    estimatedHeight = 120,
    maxHeight = 320,
    width,
    align = "start",
  }: {
    estimatedHeight?: number;
    maxHeight?: number;
    width?: number;
    align?: "start" | "end";
  } = {},
) {
  if (trigger === null) return undefined;
  const bounds = trigger.getBoundingClientRect();
  const gap = 3;
  const viewportPadding = 12;
  const roomBelow = window.innerHeight - bounds.bottom - viewportPadding - gap;
  const roomAbove = bounds.top - viewportPadding - gap;
  const openUpward = roomBelow < estimatedHeight && roomAbove > roomBelow;
  const menuWidth = Math.min(
    width ?? bounds.width,
    window.innerWidth - viewportPadding * 2,
  );
  const desiredLeft = align === "end" ? bounds.right - menuWidth : bounds.left;
  return {
    top: openUpward ? "auto" : `${bounds.bottom + gap}px`,
    bottom: openUpward ? `${window.innerHeight - bounds.top + gap}px` : "auto",
    left: `${Math.max(
      viewportPadding,
      Math.min(desiredLeft, window.innerWidth - menuWidth - viewportPadding),
    )}px`,
    width: width === undefined ? `${menuWidth}px` : undefined,
    maxHeight:
      width === undefined
        ? `${Math.min(maxHeight, Math.max(80, openUpward ? roomAbove : roomBelow))}px`
        : undefined,
  };
}

export function dismissActiveTextSelection(): void {
  const active = document.activeElement;
  if (
    active instanceof HTMLInputElement ||
    active instanceof HTMLTextAreaElement
  ) {
    const end = active.value.length;
    try {
      active.setSelectionRange(end, end);
    } catch {
      // Some input types do not expose a text selection range.
    }
    active.blur();
  }
  window.getSelection()?.removeAllRanges();
}
