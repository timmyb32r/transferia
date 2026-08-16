import type { ComponentChildren, JSX } from "preact";

type ButtonVariant = "default" | "primary" | "danger";
type ButtonShape = "default" | "icon" | "row" | "add-row";

export interface ButtonProps extends Omit<
  JSX.ButtonHTMLAttributes<HTMLButtonElement>,
  "class"
> {
  children: ComponentChildren;
  class?: string | undefined;
  variant?: ButtonVariant;
  shape?: ButtonShape;
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
  ...props
}: ButtonProps) {
  const classes = [VARIANT_CLASS[variant], SHAPE_CLASS[shape], className]
    .filter(Boolean)
    .join(" ");
  return (
    <button type={type} class={classes || undefined} {...props}>
      {children}
    </button>
  );
}
