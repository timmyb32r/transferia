const REQUIRED_GUIDANCE_EVENT = "transferia:required-guidance";
const INCOMPLETE_SELECTOR = ".required-incomplete";

export const REQUIRED_CONTROL_SELECTOR =
  "input:not([type='checkbox']), textarea, select, .select-trigger, [data-required-control]";

export function nextRequiredTarget(
  container: HTMLElement,
  excludeSelector?: string,
): HTMLElement | undefined {
  const candidates = [
    ...container.querySelectorAll<HTMLElement>(INCOMPLETE_SELECTOR),
  ].filter(
    (candidate) =>
      excludeSelector === undefined || candidate.closest(excludeSelector) === null,
  );
  return (
    candidates.find(
      (candidate) => candidate.querySelector(INCOMPLETE_SELECTOR) === null,
    ) ?? candidates[0]
  );
}

export function requestRequiredGuidance(
  scope: HTMLElement,
  excludeSelector?: string,
): void {
  document.dispatchEvent(
    new CustomEvent(REQUIRED_GUIDANCE_EVENT, {
      detail: { scope, excludeSelector },
    }),
  );
}

export function onRequiredGuidanceRequest(
  listener: (scope: HTMLElement, excludeSelector?: string) => void,
): () => void {
  const handler = (event: Event) => {
    if (
      event instanceof CustomEvent &&
      event.detail?.scope instanceof HTMLElement
    )
      listener(event.detail.scope, event.detail.excludeSelector);
  };
  document.addEventListener(REQUIRED_GUIDANCE_EVENT, handler);
  return () => document.removeEventListener(REQUIRED_GUIDANCE_EVENT, handler);
}
