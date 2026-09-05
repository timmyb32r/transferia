import type { ComponentChildren, JSX, Ref } from "preact";

type ButtonVariant = "default" | "primary" | "danger";
type ButtonShape = "default" | "icon" | "row" | "add-row";

export interface ButtonProps extends Omit<
  JSX.ButtonHTMLAttributes<HTMLButtonElement>,
  "class" | "aria-busy"
> {
  children: ComponentChildren;
  class?: string | undefined;
  variant?: ButtonVariant;
  shape?: ButtonShape;
  buttonRef?: Ref<HTMLButtonElement> | undefined;
  pending?: boolean;
}

const VARIANT_CLASS: Record<ButtonVariant, string> = {
  default: "",
  primary: "primary",
  danger: "danger-button",
};

const SHAPE_CLASS: Record<ButtonShape, string> = {
  default: "",
  icon: "icon-button",
  row: "row-action",
  "add-row": "add-row-button",
};

export function Button({
  children,
  class: className,
  variant = "default",
  shape = "default",
  type = "button",
  buttonRef,
  pending = false,
  disabled,
  onClick,
  ...props
}: ButtonProps) {
  const classes = [
    VARIANT_CLASS[variant],
    SHAPE_CLASS[shape],
    pending ? "interaction-pending" : "",
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <button
      {...(buttonRef === undefined ? {} : { ref: buttonRef })}
      type={type}
      class={classes || undefined}
      disabled={disabled}
      aria-disabled={disabled || pending || undefined}
      aria-busy={pending}
      onClick={(event) => {
        if (disabled || pending) {
          event.preventDefault();
          event.stopPropagation();
          return;
        }
        onClick?.(event);
      }}
      {...props}
    >
      {(props.role === "tab" || className?.split(" ").includes("transport-action")) && (
        <svg class="disabled-lock-icon" width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true" focusable="false">
          <rect x="3" y="7" width="10" height="8" rx="1.5" />
          <path d="M5 7V4a3 3 0 0 1 6 0v3" />
        </svg>
      )}
      {children}
    </button>
  );
}
