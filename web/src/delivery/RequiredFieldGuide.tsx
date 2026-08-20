import type { RefObject } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

import { onRequiredGuidanceRequest } from "../ui/requiredGuidance";

const INCOMPLETE_SELECTOR = ".required-incomplete";
const GUIDED_CONTROL_SELECTOR =
  "input:not([type='checkbox']), textarea, select, .select-trigger, [data-required-control]";

export function RequiredFieldGuide({
  root,
  enabled,
  revision,
}: {
  root: RefObject<HTMLElement>;
  enabled: boolean;
  revision: unknown;
}) {
  const guidedPath = useRef<HTMLElement[]>([]);
  const guidedControls = useRef<HTMLElement[]>([]);
  const [blurRevision, setBlurRevision] = useState(0);
  const [requestedScope, setRequestedScope] = useState<HTMLElement>();

  useEffect(
    () =>
      onRequiredGuidanceRequest((scope) => {
        if (root.current?.contains(scope)) {
          setRequestedScope(scope);
        }
      }),
    [root],
  );

  useEffect(() => {
    let focusedControl: HTMLElement | undefined;
    const continueAfterBlur = () => setBlurRevision((value) => value + 1);
    const frame = window.requestAnimationFrame(() => {
      const workspace = root.current;
      const container =
        requestedScope !== undefined && workspace?.contains(requestedScope)
          ? requestedScope
          : workspace;
      if (container === null) return;
      const active = document.activeElement;
      if (
        enabled &&
        active instanceof HTMLElement &&
        guidedPath.current.every((candidate) =>
          container.contains(candidate),
        ) &&
        guidedPath.current.some((candidate) => candidate.contains(active))
      ) {
        for (const candidate of guidedPath.current)
          candidate.classList.add("required-next");
        for (const control of guidedControls.current)
          control.classList.add("required-next-control");
        focusedControl = active;
        focusedControl.addEventListener("blur", continueAfterBlur, {
          once: true,
        });
        return;
      }
      clearGuidance(guidedPath.current, guidedControls.current);
      guidedPath.current = [];
      guidedControls.current = [];
      const candidates = [
        ...container.querySelectorAll<HTMLElement>(INCOMPLETE_SELECTOR),
      ];
      if (!enabled) return;
      if (candidates.length === 0 && requestedScope !== undefined) {
        setRequestedScope(undefined);
        return;
      }
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
      guidedPath.current = path;
      for (const candidate of path) {
        candidate.classList.add("required-next");
        if (
          candidate !== leaf &&
          candidate.dataset.requiredGuidance === "structural"
        )
          continue;
        const ownControl = [
          ...candidate.querySelectorAll<HTMLElement>(GUIDED_CONTROL_SELECTOR),
        ].find((control) => control.closest(INCOMPLETE_SELECTOR) === candidate);
        if (ownControl !== undefined) {
          ownControl.classList.add("required-next-control");
          guidedControls.current.push(ownControl);
        }
      }
    });
    return () => {
      window.cancelAnimationFrame(frame);
      focusedControl?.removeEventListener("blur", continueAfterBlur);
    };
  }, [blurRevision, enabled, requestedScope, revision, root]);

  useEffect(
    () => () => {
      clearGuidance(guidedPath.current, guidedControls.current);
    },
    [],
  );

  return null;
}

function clearGuidance(path: HTMLElement[], controls: HTMLElement[]) {
  for (const candidate of path) {
    candidate.classList.remove("required-next");
  }
  for (const control of controls)
    control.classList.remove("required-next-control");
}
