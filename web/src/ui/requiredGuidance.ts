const REQUIRED_GUIDANCE_EVENT = "transferia:required-guidance";

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
