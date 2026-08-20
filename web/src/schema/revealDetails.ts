import { requestRequiredGuidance } from "../ui/requiredGuidance";

export function revealDetails(selector: string): void {
  requestAnimationFrame(() =>
    requestAnimationFrame(() => {
      const details = document.querySelector<HTMLElement>(selector);
      if (details === null) return;
      details.scrollIntoView({ behavior: "smooth", block: "start" });
      details.focus({ preventScroll: true });
      requestRequiredGuidance(details);
    }),
  );
}
