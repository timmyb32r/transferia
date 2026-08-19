import { useCallback, useEffect, useRef, useState } from "preact/hooks";

import { useAnchoredOverlay } from "./overlay";

export function useListbox({
  disabled,
  onOpen,
}: {
  disabled: boolean;
  onOpen?: (() => void) | undefined;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);

  const close = useCallback(() => {
    setOpen(false);
    setQuery("");
  }, []);
  const openMenu = useCallback(() => {
    if (disabled) return;
    setQuery("");
    setOpen(true);
    onOpen?.();
  }, [disabled, onOpen]);
  const toggle = useCallback(() => {
    if (open) close();
    else openMenu();
  }, [close, open, openMenu]);

  useEffect(() => {
    if (disabled) close();
  }, [close, disabled]);
  useAnchoredOverlay({ open, root, trigger, onClose: close });

  return {
    open,
    query,
    root,
    trigger,
    close,
    openMenu,
    toggle,
    setQuery,
    onKeyDown: (event: KeyboardEvent) =>
      handleListboxKeyDown(event, open, openMenu, close, root, trigger),
  };
}

function handleListboxKeyDown(
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
  if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) return;
  event.preventDefault();
  if (!open) {
    const direction = event.key;
    openMenu();
    queueMicrotask(() => {
      const options = listboxOptions(root.current);
      const target = direction === "ArrowDown" ? options[0] : options.at(-1);
      target?.focus();
    });
    return;
  }
  const options = listboxOptions(root.current);
  if (options.length === 0) return;
  if (event.key === "Home" || event.key === "End") {
    options[event.key === "Home" ? 0 : options.length - 1]?.focus();
    return;
  }
  if (!(event.target instanceof HTMLButtonElement)) {
    options[event.key === "ArrowDown" ? 0 : options.length - 1]?.focus();
    return;
  }
  const current = options.indexOf(event.target);
  if (current < 0) return;
  const direction = event.key === "ArrowDown" ? 1 : -1;
  options[(current + direction + options.length) % options.length]?.focus();
}

function listboxOptions(root: HTMLDivElement | null): HTMLButtonElement[] {
  return [
    ...(root?.querySelectorAll<HTMLButtonElement>('[role="option"]') ?? []),
  ];
}
