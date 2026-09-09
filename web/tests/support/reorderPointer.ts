import { fireEvent } from "@testing-library/preact";

export function pointer(target: Element | Document, type: string, x: number, y: number) {
  const event = new MouseEvent(type, { bubbles: true, cancelable: true, clientX: x, clientY: y, button: 0 });
  Object.defineProperties(event, { pointerId: { value: 1 }, isPrimary: { value: true } });
  fireEvent(target, event);
}

export function startRowDrag(handle: HTMLElement) {
  handle.setPointerCapture = () => {};
  handle.hasPointerCapture = () => false;
  handle.releasePointerCapture = () => {};
  pointer(handle, "pointerdown", 0, 0);
}

export function reorderPointer(handle: HTMLElement, y: number) {
  startRowDrag(handle);
  pointer(document, "pointermove", 0, y);
  pointer(document, "pointerup", 0, y);
}
