const REQUIRED_GUIDANCE_EVENT = "transferia:required-guidance";
const INCOMPLETE_SELECTOR = ".required-incomplete";

export const REQUIRED_CONTROL_SELECTOR =
  "input:not([type='checkbox']), textarea, select, .select-trigger, [data-required-control]";

export function nextRequiredTarget(
  container: HTMLElement,
): HTMLElement | undefined {
  const candidates = [
    ...container.querySelectorAll<HTMLElement>(INCOMPLETE_SELECTOR),
  ];
  return (
    candidates.find(
      (candidate) => candidate.querySelector(INCOMPLETE_SELECTOR) === null,
    ) ?? candidates[0]
  );
}

export function requestRequiredGuidance(scope: HTMLElement): void {
  document.dispatchEvent(
    new CustomEvent<HTMLElement>(REQUIRED_GUIDANCE_EVENT, { detail: scope }),
  );
}

export function onRequiredGuidanceRequest(
  listener: (scope: HTMLElement) => void,
): () => void {
  const handler = (event: Event) => {
    if (event instanceof CustomEvent && event.detail instanceof HTMLElement)
      listener(event.detail);
  };
  document.addEventListener(REQUIRED_GUIDANCE_EVENT, handler);
  return () => document.removeEventListener(REQUIRED_GUIDANCE_EVENT, handler);
}
