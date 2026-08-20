import { requestRequiredGuidance } from "../ui/requiredGuidance";

export function revealDetails(selector: string): void {
  requestAnimationFrame(() =>
    requestAnimationFrame(() => {
      const details = document.querySelector<HTMLElement>(selector);
      if (details === null) return;
      details.scrollIntoView({ behavior: "smooth", block: "start" });
      details.focus({ preventScroll: true });
      requestRequiredGuidance(details);
      const route = details.closest<HTMLElement>(".route-composition");
      route?.classList.remove("route-selection-flash");
      void route?.offsetWidth;
      route?.classList.add("route-selection-flash");
      window.setTimeout(
        () => route?.classList.remove("route-selection-flash"),
        1000,
      );
    }),
  );
}
