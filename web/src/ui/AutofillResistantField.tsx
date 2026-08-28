import type { JSX, Ref } from "preact";
import { useRef } from "preact/hooks";

type ProtectedAttributes =
  | "autoComplete"
  | "autocomplete"
  | "autoCapitalize"
  | "autocapitalize"
  | "autoCorrect"
  | "autocorrect"
  | "name"
  | "spellCheck"
  | "spellcheck";

export interface AutofillResistantInputProps extends Omit<
  JSX.InputHTMLAttributes<HTMLInputElement>,
  ProtectedAttributes | "ref"
> {
  inputRef?: Ref<HTMLInputElement> | undefined;
  opaqueGroupName?: string | undefined;
}

export interface AutofillResistantTextareaProps extends Omit<
  JSX.TextareaHTMLAttributes<HTMLTextAreaElement>,
  ProtectedAttributes | "ref"
> {
  textareaRef?: Ref<HTMLTextAreaElement> | undefined;
}

export interface AutofillResistantSelectProps extends Omit<
  JSX.SelectHTMLAttributes<HTMLSelectElement>,
  ProtectedAttributes | "ref"
> {
  selectRef?: Ref<HTMLSelectElement> | undefined;
}

const SESSION_TOKEN = createSessionToken();
let fieldSequence = 0;
// HTML spellcheck is an enumerated string attribute. Preact's type incorrectly
// models it as boolean, whose false value removes the attribute altogether.
const SPELLCHECK_DISABLED = "false" as unknown as boolean;

export function useOpaqueFieldName(): string {
  const name = useRef<string>();
  if (name.current === undefined) {
    fieldSequence += 1;
    name.current = `tf-${SESSION_TOKEN}-${fieldSequence.toString(36)}`;
  }
  return name.current;
}

export function AutofillResistantInput({
  inputRef,
  opaqueGroupName,
  ...props
}: AutofillResistantInputProps) {
  const generatedName = useOpaqueFieldName();
  return (
    <input
      {...props}
      {...(inputRef === undefined ? {} : { ref: inputRef })}
      name={opaqueGroupName ?? generatedName}
      autoComplete="none"
      autocapitalize="off"
      autocorrect="off"
      spellcheck={SPELLCHECK_DISABLED}
      data-1p-ignore="true"
      data-lpignore="true"
      data-form-type="other"
    />
  );
}

export function AutofillResistantTextarea({
  textareaRef,
  ...props
}: AutofillResistantTextareaProps) {
  const name = useOpaqueFieldName();
  return (
    <textarea
      {...props}
      {...(textareaRef === undefined ? {} : { ref: textareaRef })}
      name={name}
      autoComplete="none"
      autocapitalize="off"
      autocorrect="off"
      spellcheck={SPELLCHECK_DISABLED}
      data-1p-ignore="true"
      data-lpignore="true"
      data-form-type="other"
    />
  );
}

export function AutofillResistantSelect({
  selectRef,
  ...props
}: AutofillResistantSelectProps) {
  const name = useOpaqueFieldName();
  return (
    <select
      {...props}
      {...(selectRef === undefined ? {} : { ref: selectRef })}
      name={name}
      autoComplete="none"
      data-1p-ignore="true"
      data-lpignore="true"
      data-form-type="other"
    />
  );
}

function createSessionToken(): string {
  if (typeof globalThis.crypto?.randomUUID === "function")
    return globalThis.crypto.randomUUID();
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}
