import type { RefObject } from "preact";
import { useEffect } from "preact/hooks";

const INCOMPLETE_SELECTOR = ".required-incomplete";

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
      for (const candidate of candidates)
        candidate.classList.remove("required-next");
      if (!enabled) return;
      const next =
        candidates.find(
          (candidate) => candidate.querySelector(INCOMPLETE_SELECTOR) === null,
        ) ?? candidates[0];
      next?.classList.add("required-next");
    });
    return () => window.cancelAnimationFrame(frame);
  }, [enabled, revision, root]);

  return null;
}
