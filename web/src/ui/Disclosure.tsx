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
    <details
      class={["foldout", className].filter(Boolean).join(" ")}
    >
      <summary
        onClick={(event) => {
          event.preventDefault();
          const summary = event.currentTarget;
          const details = summary.parentElement as HTMLDetailsElement;
          const root = summary.ownerDocument.documentElement;
          const anchor = root.style.getPropertyValue("overflow-anchor");
          const priority = root.style.getPropertyPriority("overflow-anchor");
          root.style.setProperty("overflow-anchor", "none");
          try {
            details.open = !details.open;
          } finally {
            void root.scrollHeight;
            if (anchor) root.style.setProperty("overflow-anchor", anchor, priority);
            else root.style.removeProperty("overflow-anchor");
          }
          if (event.detail > 0) {
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
