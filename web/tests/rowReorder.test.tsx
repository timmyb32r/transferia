// @vitest-environment jsdom
import { cleanup, fireEvent } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useRowReorder } from "../src/ui/useRowReorder";
import { Button } from "../src/ui/Button";
import { render } from "./support/render";
import { pointer, startRowDrag } from "./support/reorderPointer";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

function Harness({ onMove, disabled = false }: { onMove: (from: number, to: number) => void; disabled?: boolean }) {
  const start = useRowReorder({ disabled, revision: "unchanged", onMove });
  return <section>{["First", "Second", "Third"].map(name => <article class="test-row">
    <Button disabled={disabled} onPointerDown={event => start(event, ".test-row")}>{name}</Button>
    <span>Whole row content</span>
  </article>)}</section>;
}

function setup() {
  const onMove = vi.fn();
  const view = render(<Harness onMove={onMove} />);
  const rows = view.container.querySelectorAll<HTMLElement>(".test-row");
  rows.forEach((row, index) => vi.spyOn(row, "getBoundingClientRect").mockReturnValue({
    x: 20, y: 100 + index * 60, left: 20, top: 100 + index * 60,
    width: 400, height: 40, right: 420, bottom: 140 + index * 60, toJSON: () => ({}),
  }));
  return { ...view, rows, onMove };
}

describe("pointer row reordering", () => {
  it("follows the first pixel, preserves the original footprint and commits a gap drop only on release", () => {
    const view = setup();
    startRowDrag(view.getByRole("button", { name: "First" }));
    const overlay = document.querySelector<HTMLElement>(".row-reorder-overlay")!;
    expect(overlay.textContent).toContain("Whole row content");
    expect(overlay.inert).toBe(true);
    expect(view.rows[0]!.style.opacity).toBe("0");
    expect(view.rows[0]!.style.position).toBe("");
    pointer(document, "pointermove", 1, 1);
    expect(overlay.style.transform).toBe("translate(1px, 1px)");
    expect(view.onMove).not.toHaveBeenCalled();
    pointer(document, "pointermove", 10, 210); // Gap after Second.
    expect(view.rows[1]!.style.translate).toBe("0 -60px");
    expect(view.rows[2]!.style.translate).toBe("0 0px");
    expect(document.querySelector(".row-reorder-marker")).toBeNull();
    expect(view.onMove).not.toHaveBeenCalled();
    pointer(document, "pointerup", 10, 210);
    expect(view.onMove).toHaveBeenCalledExactlyOnceWith(0, 1);
    expect(view.rows[0]!.style.opacity).toBe("");
    expect(view.rows[1]!.style.translate).toBe("");
    expect(document.querySelector(".row-reorder-overlay")).toBeNull();
    expect(document.querySelector(".row-reorder-marker")).toBeNull();
  });

  for (const reason of ["escape", "pointercancel", "blur", "lostcapture", "unmount"]) {
    it(`cancels without changing data on ${reason}`, () => {
      const view = setup();
      const handle = view.getByRole("button", { name: "First" });
      startRowDrag(handle);
      pointer(document, "pointermove", 1, 300);
      if (reason === "escape") fireEvent.keyDown(document, { key: "Escape" });
      if (reason === "pointercancel") pointer(document, "pointercancel", 1, 300);
      if (reason === "blur") fireEvent(window, new Event("blur"));
      if (reason === "lostcapture") fireEvent(handle, new Event("lostpointercapture"));
      if (reason === "unmount") view.unmount();
      pointer(document, "pointerup", 1, 300);
      expect(view.onMove).not.toHaveBeenCalled();
      view.rows.forEach(row => expect(row.style.translate).toBe(""));
      expect(document.querySelector(".row-reorder-overlay")).toBeNull();
    });
  }

  it("slides neighbours in both directions and restores their places when the pointer returns", () => {
    const view = setup();
    startRowDrag(view.getByRole("button", { name: "Second" }));
    pointer(document, "pointermove", 0, 110);
    expect(view.rows[0]!.style.translate).toBe("0 60px");
    pointer(document, "pointermove", 0, 170);
    expect(view.rows[0]!.style.translate).toBe("0 0px");
    pointer(document, "pointermove", 0, 260);
    expect(view.rows[2]!.style.translate).toBe("0 -60px");
    // Repeated identical events must not reverse the neighbour's displacement.
    pointer(document, "pointermove", 0, 260);
    expect(view.rows[2]!.style.translate).toBe("0 -60px");
    expect(view.onMove).not.toHaveBeenCalled();
    pointer(document, "pointerup", 0, 260);
    expect(view.onMove).toHaveBeenCalledExactlyOnceWith(1, 2);
  });

  it("does not start from row content or a disabled handle", () => {
    const onMove = vi.fn();
    const view = render(<Harness onMove={onMove} disabled />);
    pointer(view.getByRole("button", { name: "First" }), "pointerdown", 0, 0);
    pointer(view.container.querySelector("article")!, "pointerdown", 0, 0);
    expect(document.querySelector(".row-reorder-overlay")).toBeNull();
    expect(onMove).not.toHaveBeenCalled();
  });
});
