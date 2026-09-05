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
          <span class="help" tabindex={0} aria-describedby={tooltipId} title={description}>
            <span aria-hidden="true">?</span>
            <span id={tooltipId} role="tooltip" class="visually-hidden">
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
  incomplete = false,
  children,
}: {
  label: string;
  required?: boolean;
  invalid?: boolean;
  incomplete?: boolean;
  children: ComponentChildren;
}) {
  return (
    <label
      class={[
        "top-field",
        incomplete ? "required-incomplete" : "",
        invalid ? "required-missing" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <span>
        {label}
        {!required && <small class="optional">(optional)</small>}
      </span>
      {children}
    </label>
  );
}
