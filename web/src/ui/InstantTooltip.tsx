import type { ComponentChildren } from "preact";
import { useId } from "preact/hooks";

export function InstantTooltip({
  children,
  content,
  class: className,
}: {
  children: ComponentChildren;
  content: string;
  class?: string | undefined;
}) {
  const tooltipId = useId();

  return (
    <span
      class={["instant-tooltip-host", className].filter(Boolean).join(" ")}
      tabindex={0}
      aria-describedby={tooltipId}
      title={content}
    >
      {children}
      <span
        id={tooltipId}
        role="tooltip"
        class="visually-hidden"
      >
        {content}
      </span>
    </span>
  );
}
