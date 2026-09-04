import type { ComponentChildren } from "preact";
import { useEffect, useRef } from "preact/hooks";

export function Disclosure({
  label,
  class: className,
  children,
}: {
  label: ComponentChildren;
  class?: string;
  children: ComponentChildren;
}) {
  const details = useRef<HTMLDetailsElement>(null);
  useEffect(() => {
    const closeOutside = (event: PointerEvent) => {
      const current = details.current;
      if (current?.open && !current.contains(event.target as Node)) {
        current.open = false;
      }
    };
    document.addEventListener("pointerdown", closeOutside);
    return () => document.removeEventListener("pointerdown", closeOutside);
  }, []);

  return (
    <details
      ref={details}
      class={["foldout", className].filter(Boolean).join(" ")}
    >
      <summary
        onClick={(event) => {
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
