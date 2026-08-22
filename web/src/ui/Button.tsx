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
        if (pending) {
          event.preventDefault();
          event.stopPropagation();
          return;
        }
        onClick?.(event);
      }}
      {...props}
    >
      {children}
    </button>
  );
}
