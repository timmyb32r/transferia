import type { RefObject } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

import {
  nextRequiredTarget,
  onRequiredGuidanceRequest,
  REQUIRED_CONTROL_SELECTOR,
} from "../ui/requiredGuidance";

const INCOMPLETE_SELECTOR = ".required-incomplete";

export function RequiredFieldGuide({
  root,
  enabled,
  revision,
  tone = "guided",
}: {
  root: RefObject<HTMLElement>;
  enabled: boolean;
  revision: unknown;
  tone?: "guided" | "error";
}) {
  const guidedPath = useRef<HTMLElement[]>([]);
  const guidedControls = useRef<HTMLElement[]>([]);
  const [blurRevision, setBlurRevision] = useState(0);
  const [requestedScope, setRequestedScope] = useState<HTMLElement>();
  const [excludeSelector, setExcludeSelector] = useState<string>();
  const [requestRevision, setRequestRevision] = useState(0);
  const forceGuidance = useRef(false);

  useEffect(
    () =>
      onRequiredGuidanceRequest((scope, excluded) => {
        if (root.current?.contains(scope)) {
          forceGuidance.current = true;
          setRequestedScope(scope);
          setExcludeSelector(excluded);
          setRequestRevision((value) => value + 1);
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
      const force = forceGuidance.current;
      forceGuidance.current = false;
      if (
        !force &&
        enabled &&
        active instanceof HTMLElement &&
        guidedPath.current.every((candidate) =>
          container.contains(candidate),
        ) &&
        guidedPath.current.some((candidate) => candidate.contains(active))
      ) {
        applyGuidance(guidedPath.current, guidedControls.current, tone);
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
      ].filter(
        (candidate) =>
          excludeSelector === undefined ||
          candidate.closest(excludeSelector) === null,
      );
      if (!enabled) return;
      if (candidates.length === 0 && requestedScope !== undefined) {
        setRequestedScope(undefined);
        return;
      }
      const leaf = nextRequiredTarget(container, excludeSelector);
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
        if (tone === "error") candidate.classList.add("required-error");
        if (
          candidate !== leaf &&
          candidate.dataset.requiredGuidance === "structural"
        )
          continue;
        const ownControl = [
          ...candidate.querySelectorAll<HTMLElement>(REQUIRED_CONTROL_SELECTOR),
        ].find((control) => control.closest(INCOMPLETE_SELECTOR) === candidate);
        if (ownControl !== undefined) {
          ownControl.classList.add("required-next-control");
          if (tone === "error")
            ownControl.classList.add("required-error-control");
          guidedControls.current.push(ownControl);
        }
      }
    });
    return () => {
      window.cancelAnimationFrame(frame);
      focusedControl?.removeEventListener("blur", continueAfterBlur);
    };
  }, [
    blurRevision,
    enabled,
    excludeSelector,
    requestedScope,
    requestRevision,
    revision,
    root,
    tone,
  ]);

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
    candidate.classList.remove("required-error");
  }
  for (const control of controls) {
    control.classList.remove("required-next-control");
    control.classList.remove("required-error-control");
  }
}

function applyGuidance(
  path: HTMLElement[],
  controls: HTMLElement[],
  tone: "guided" | "error",
) {
  for (const candidate of path) {
    candidate.classList.add("required-next");
    candidate.classList.toggle("required-error", tone === "error");
  }
  for (const control of controls) {
    control.classList.add("required-next-control");
    control.classList.toggle("required-error-control", tone === "error");
  }
}
