import type { ComponentChildren } from "preact";
import { useId } from "preact/hooks";

export function InstantTooltip({
  children,
  content,
  placement = "bottom",
  class: className,
}: {
  children: ComponentChildren;
  content: string;
  placement?: "bottom" | "right";
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
        class={`instant-tooltip-content ${placement}`}
      >
        {content}
      </span>
    </span>
  );
}
