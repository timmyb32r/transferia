import { nextRequiredTarget, REQUIRED_CONTROL_SELECTOR, requestRequiredGuidance } from "../ui/requiredGuidance";

export function revealDetails(selector: string): void {
  requestAnimationFrame(() =>
    requestAnimationFrame(() => {
      const details = document.querySelector<HTMLElement>(selector);
      if (details === null) return;
      const tableName = details.querySelector<HTMLElement>('[data-field-name="table_naming"]');
      const target = tableName ?? nextRequiredTarget(details) ?? details;
      const control = [...target.querySelectorAll<HTMLElement>(REQUIRED_CONTROL_SELECTOR)]
        .find((element) => !element.matches(":disabled")) ?? target;
      // Keep source context visible above the newly selected parser. Measure the
      // control, not the whole (potentially very tall) parser settings card.
      const bounds = control.getBoundingClientRect();
      window.scrollTo({
        top: Math.max(0, window.scrollY + bounds.top + bounds.height / 2 - window.innerHeight * 0.6),
        behavior: "smooth",
      });
      control.focus({ preventScroll: true });
      requestRequiredGuidance(details);
    }),
  );
}
