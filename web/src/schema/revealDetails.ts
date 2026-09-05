import { requestRequiredGuidance } from "../ui/requiredGuidance";

export function revealDetails(selector: string): void {
  requestAnimationFrame(() =>
    requestAnimationFrame(() => {
      const details = document.querySelector<HTMLElement>(selector);
      if (details === null) return;
      const tableName = details.querySelector<HTMLElement>('[data-field-name="table_naming"]');
      const nameInput = tableName?.querySelector<HTMLInputElement>('input:not([type="checkbox"]):not(:disabled)');
      if (tableName && nameInput) {
        tableName.scrollIntoView({ behavior: "smooth", block: "start" });
        nameInput.focus({ preventScroll: true });
        return;
      }
      details.scrollIntoView({ behavior: "smooth", block: "start" });
      details.focus({ preventScroll: true });
      requestRequiredGuidance(details);
    }),
  );
}
