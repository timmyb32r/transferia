// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import { beginMetadataReveal } from "../src/delivery/metadataReveal";

afterEach(() => { vi.unstubAllGlobals(); document.body.replaceChildren(); });
function setup() {
  const frames: FrameRequestCallback[] = [];
  vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => frames.push(callback));
  vi.stubGlobal("cancelAnimationFrame", vi.fn());
  const trigger = document.createElement("button"), target = document.createElement("section");
  document.body.append(trigger, target);
  trigger.focus();
  target.scrollIntoView = vi.fn();
  const flush = () => { while (frames.length) frames.shift()!(0); };
  return { trigger, target, flush };
}

it("reveals a completed explicit connection once without moving focus", () => {
  const { target, trigger, flush } = setup();
  const reveal = beginMetadataReveal(() => target, () => true);
  expect(target.scrollIntoView).not.toHaveBeenCalled();
  reveal.complete(); flush(); reveal.complete(); flush();
  expect(target.scrollIntoView).toHaveBeenCalledOnce();
  expect(document.activeElement).toBe(trigger);
});

it.each(["pointerdown", "keydown", "wheel", "touchstart"])("does not move the camera after another %s interaction", event => {
  const { target, flush } = setup();
  const reveal = beginMetadataReveal(() => target, () => true);
  window.dispatchEvent(new Event(event));
  reveal.complete(); flush();
  expect(target.scrollIntoView).not.toHaveBeenCalled();
});

it("ignores a failed/stale connection and cancels on unmount", () => {
  const { target, flush } = setup();
  beginMetadataReveal(() => target, () => false).complete(); flush();
  const reveal = beginMetadataReveal(() => target, () => true);
  reveal.complete(); reveal.cancel(); flush();
  expect(target.scrollIntoView).not.toHaveBeenCalled();
});
