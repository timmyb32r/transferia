import { useLayoutEffect, useRef } from "preact/hooks";
import { AutofillResistantTextarea } from "./AutofillResistantField";
import { SyntaxHighlight } from "./SyntaxHighlight";

export function SqlEditor({ value, disabled, onChange }: {
  value: string; disabled: boolean; onChange: (value: string) => void;
}) {
  const highlight = useRef<HTMLPreElement>(null);
  const input = useRef<HTMLTextAreaElement>(null);
  const syncScroll = () => {
    if (!highlight.current || !input.current) return;
    highlight.current.scrollTop = input.current.scrollTop;
    highlight.current.scrollLeft = input.current.scrollLeft;
  };
  useLayoutEffect(syncScroll, [value]);
  return <div class="sql-code-editor">
    <pre ref={highlight} aria-hidden="true"><SyntaxHighlight value={`${value}\n`} language="sql" /></pre>
    <AutofillResistantTextarea textareaRef={input} aria-label="SQL over table input" value={value}
      disabled={disabled} wrap="off" onScroll={syncScroll}
      onInput={event => onChange(event.currentTarget.value)} />
  </div>;
}
