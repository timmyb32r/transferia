import type { RefObject } from "preact";
import { useEffect } from "preact/hooks";

const INCOMPLETE_SELECTOR = ".required-incomplete";
const GUIDED_CONTROL_SELECTOR =
  "input:not([type='checkbox']), textarea, select, .select-trigger";

export function RequiredFieldGuide({
  root,
  enabled,
  revision,
}: {
  root: RefObject<HTMLElement>;
  enabled: boolean;
  revision: unknown;
}) {
  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      const container = root.current;
      if (container === null) return;
      const candidates = [
        ...container.querySelectorAll<HTMLElement>(INCOMPLETE_SELECTOR),
      ];
      for (const candidate of candidates) {
        candidate.classList.remove("required-next");
        for (const control of candidate.querySelectorAll<HTMLElement>(
          ".required-next-control",
        ))
          control.classList.remove("required-next-control");
      }
      if (!enabled) return;
      const leaf =
        candidates.find(
          (candidate) => candidate.querySelector(INCOMPLETE_SELECTOR) === null,
        ) ?? candidates[0];
      if (leaf === undefined) return;
      const path = [
        leaf,
        ...candidates.filter(
          (candidate) => candidate !== leaf && candidate.contains(leaf),
        ),
      ];
      for (const candidate of path) {
        candidate.classList.add("required-next");
        const ownControl = [
          ...candidate.querySelectorAll<HTMLElement>(GUIDED_CONTROL_SELECTOR),
        ].find((control) => control.closest(INCOMPLETE_SELECTOR) === candidate);
        ownControl?.classList.add("required-next-control");
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [enabled, revision, root]);

  return null;
}
