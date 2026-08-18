import type { ComponentChildren } from "preact";

export function Disclosure({
  label,
  class: className,
  children,
}: {
  label: ComponentChildren;
  class?: string;
  children: ComponentChildren;
}) {
  return (
    <details class={["foldout", className].filter(Boolean).join(" ")}>
      <summary
        onClick={(event) => {
          event.preventDefault();
          const details = event.currentTarget.parentElement;
          if (details instanceof HTMLDetailsElement)
            details.open = !details.open;
          if (event.detail > 0) {
            const summary = event.currentTarget;
            queueMicrotask(() => summary.blur());
          }
        }}
      >
        {label}
      </summary>
      <div class="foldout-content">{children}</div>
    </details>
  );
}
