import type { JSX } from "preact";
import { useEffect, useRef } from "preact/hooks";

// Freeze the rendered appearance, including live field values, without moving
// the real editor DOM or its focus/state out of the layout.
function snapshot(row: HTMLElement): HTMLElement {
  const clone = row.cloneNode(true) as HTMLElement;
  const sources = [row, ...row.querySelectorAll<HTMLElement>("*")];
  const copies = [clone, ...clone.querySelectorAll<HTMLElement>("*")];
  sources.forEach((source, index) => {
    const copy = copies[index]!;
    const style = getComputedStyle(source);
    for (let i = 0; i < style.length; i++) {
      const property = style.item(i);
      copy.style.setProperty(property, style.getPropertyValue(property));
    }
    copy.removeAttribute("id");
    copy.removeAttribute("name");
    if (source instanceof HTMLInputElement && copy instanceof HTMLInputElement) {
      copy.value = source.value;
      copy.checked = source.checked;
    }
    if (source instanceof HTMLTextAreaElement && copy instanceof HTMLTextAreaElement) copy.value = source.value;
    if (source instanceof HTMLSelectElement && copy instanceof HTMLSelectElement) copy.selectedIndex = source.selectedIndex;
  });
  return clone;
}

export function useRowReorder({ disabled, revision, onMove }: {
  disabled: boolean; revision: unknown; onMove: (from: number, to: number) => void;
}) {
  const active = useRef<(() => void) | undefined>();
  // A changed configuration during a gesture must not reorder stale indices.
  useEffect(() => () => active.current?.(), [disabled, revision]);
  return (event: JSX.TargetedPointerEvent<HTMLButtonElement>, selector: string) => {
    if (disabled || event.button !== 0 || !event.isPrimary || active.current) return;
    const handle = event.currentTarget;
    const row = handle.closest<HTMLElement>(selector);
    const parent = row?.parentElement;
    if (!row || !parent) return;
    const rows = Array.from(parent.children).filter((child): child is HTMLElement => child instanceof HTMLElement && child.matches(selector));
    const from = rows.indexOf(row);
    if (from < 0) return;
    event.preventDefault();
    handle.focus({ preventScroll: true });
    const bounds = row.getBoundingClientRect();
    const overlay = document.createElement("div");
    overlay.className = "row-reorder-overlay";
    overlay.setAttribute("aria-hidden", "true");
    overlay.inert = true;
    Object.assign(overlay.style, { left: `${bounds.left}px`, top: `${bounds.top}px`, width: `${bounds.width}px`, height: `${bounds.height}px` });
    const clone = snapshot(row);
    if (row instanceof HTMLTableRowElement) {
      const table = document.createElement("table");
      table.style.cssText = "border-collapse:collapse;width:100%;table-layout:fixed";
      const body = document.createElement("tbody");
      Array.from(row.cells).forEach((cell, index) => {
        const copy = (clone as HTMLTableRowElement).cells[index]!;
        copy.style.width = `${cell.getBoundingClientRect().width}px`;
      });
      body.append(clone); table.append(body); overlay.append(table);
    } else overlay.append(clone);
    const marker = document.createElement("div");
    marker.className = "row-reorder-marker";
    marker.setAttribute("aria-hidden", "true");
    document.body.append(overlay, marker);
    const opacity = row.style.opacity;
    row.style.opacity = "0";
    const cursor = document.documentElement.style.cursor;
    document.documentElement.style.cursor = "grabbing";
    handle.setPointerCapture(event.pointerId);
    let x = event.clientX, y = event.clientY, slot = from, moved = false, frame = 0;
    const scrollParents: HTMLElement[] = [];
    for (let node: HTMLElement | null = parent; node; node = node.parentElement) {
      if (/(auto|scroll)/.test(getComputedStyle(node).overflowY)) scrollParents.push(node);
    }
    const scroller = document.scrollingElement as HTMLElement | null;
    if (scroller && !scrollParents.includes(scroller)) scrollParents.push(scroller);
    const draw = () => {
      overlay.style.transform = `translate(${x - event.clientX}px, ${y - event.clientY}px)`;
      slot = rows.findIndex(item => { const rect = item.getBoundingClientRect(); return y < rect.top + rect.height / 2; });
      if (slot < 0) slot = rows.length;
      const target = rows[Math.min(slot, rows.length - 1)]!.getBoundingClientRect();
      Object.assign(marker.style, { left: `${target.left}px`, top: `${slot === rows.length ? target.bottom : target.top}px`, width: `${target.width}px` });
    };
    const tick = () => {
      if (moved) {
        for (const node of scrollParents) {
          const rect = node === scroller ? { top: 0, bottom: window.innerHeight } : node.getBoundingClientRect();
          const delta = y < rect.top + 40 ? -12 : y > rect.bottom - 40 ? 12 : 0;
          const before = node.scrollTop;
          if (delta) node.scrollTop += delta;
          if (node.scrollTop !== before) break;
        }
        draw();
      }
      frame = requestAnimationFrame(tick);
    };
    const cleanup = () => {
      active.current = undefined;
      cancelAnimationFrame(frame);
      document.removeEventListener("pointermove", move);
      document.removeEventListener("pointerup", up);
      document.removeEventListener("pointercancel", cancel);
      document.removeEventListener("keydown", key);
      window.removeEventListener("blur", cleanup);
      handle.removeEventListener("lostpointercapture", cleanup);
      if (handle.hasPointerCapture(event.pointerId)) handle.releasePointerCapture(event.pointerId);
      row.style.opacity = opacity;
      document.documentElement.style.cursor = cursor;
      overlay.remove(); marker.remove();
    };
    const move = (next: PointerEvent) => {
      if (next.pointerId !== event.pointerId) return;
      x = next.clientX; y = next.clientY;
      moved ||= x !== event.clientX || y !== event.clientY;
      draw();
    };
    const up = (next: PointerEvent) => {
      if (next.pointerId !== event.pointerId) return;
      const to = slot > from ? slot - 1 : slot;
      cleanup();
      if (moved && to !== from) onMove(from, to);
    };
    const cancel = (next: PointerEvent) => { if (next.pointerId === event.pointerId) cleanup(); };
    const key = (next: KeyboardEvent) => { if (next.key === "Escape") { next.preventDefault(); next.stopPropagation(); cleanup(); } };
    active.current = cleanup;
    document.addEventListener("pointermove", move);
    document.addEventListener("pointerup", up);
    document.addEventListener("pointercancel", cancel);
    document.addEventListener("keydown", key);
    window.addEventListener("blur", cleanup);
    handle.addEventListener("lostpointercapture", cleanup);
    draw();
    frame = requestAnimationFrame(tick);
  };
}
