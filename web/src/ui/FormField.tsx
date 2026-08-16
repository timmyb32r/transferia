import type { ComponentChildren } from "preact";

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
  return (
    <div class={["form-row", className].filter(Boolean).join(" ")}>
      <label class="field-label" for={controlId}>
        <span>
          {label}
          {optional && <small class="optional">(optional)</small>}
        </span>
        {description && (
          <span class="help" tabindex={0} data-tooltip={description}>
            ?
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
  children,
}: {
  label: string;
  required?: boolean;
  children: ComponentChildren;
}) {
  return (
    <label class="top-field">
      <span>
        {label}
        {!required && <small class="optional">(optional)</small>}
      </span>
      {children}
    </label>
  );
}
