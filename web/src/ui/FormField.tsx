import type { ComponentChildren } from "preact";
import { useId } from "preact/hooks";

export function FormField({
  label,
  optional,
  description,
  controlId,
  class: className,
  children,
}: {
  label: ComponentChildren;
  optional: boolean;
  description?: string | undefined;
  controlId?: string | undefined;
  class?: string | undefined;
  children: ComponentChildren;
}) {
  const tooltipId = useId();
  return (
    <div class={["form-row", className].filter(Boolean).join(" ")}>
      <label class="field-label" for={controlId}>
        <span>
          {label}
          {optional && <small class="optional">(optional)</small>}
        </span>
        {description && (
          <span class="help" tabindex={0} aria-describedby={tooltipId}>
            <span aria-hidden="true">?</span>
            <span id={tooltipId} role="tooltip" class="help-tooltip">
              {description}
            </span>
          </span>
        )}
      </label>
      <div class="field-control">{children}</div>
    </div>
  );
}

export function TopField({
  label,
  required = false,
  invalid = false,
  children,
}: {
  label: string;
  required?: boolean;
  invalid?: boolean;
  children: ComponentChildren;
}) {
  return (
    <label class={invalid ? "top-field required-missing" : "top-field"}>
      <span>
        {label}
        {!required && <small class="optional">(optional)</small>}
      </span>
      {children}
    </label>
  );
}
