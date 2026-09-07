import { useId, useLayoutEffect, useRef, useState } from "preact/hooks";

import { AutofillResistantTextarea } from "../../ui/AutofillResistantField";
import { Button } from "../../ui/Button";
import { anchoredMenuStyle, useAnchoredOverlay } from "../../ui/overlay";

export function TransformNameAction({ name, index, disabled, onSave }: {
  name: string; index: number; disabled: boolean; onSave: (name: string) => void;
}) {
  const [view, setView] = useState<"menu" | "rename">();
  const [draft, setDraft] = useState(name);
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const item = useRef<HTMLButtonElement>(null);
  const input = useRef<HTMLTextAreaElement>(null);
  const id = useId();
  const close = () => setView(undefined);
  const finish = () => {
    close();
    trigger.current?.focus({ preventScroll: true });
  };
  const save = () => {
    if (disabled) return;
    onSave(draft);
    finish();
  };
  useAnchoredOverlay({ open: view !== undefined, root, trigger, onClose: close });
  useLayoutEffect(() => {
    if (disabled) { close(); return; }
    if (view === "menu") item.current?.focus({ preventScroll: true });
    if (view === "rename") {
      input.current?.focus({ preventScroll: true });
      input.current?.select();
    }
  }, [view, disabled]);
  const position = view === undefined ? undefined : anchoredMenuStyle(trigger.current, {
    width: view === "rename" ? 340 : 174,
    estimatedHeight: view === "rename" ? 208 : 48,
    align: "end",
  });
  const dialogPosition = position && { ...position,
    maxHeight: `calc(100dvh - ${position.top === "auto" ? position.bottom : position.top} - 12px)`,
  };

  return <div ref={root} class="middleware-name-action" onKeyDown={event => {
    if (view === undefined) return;
    if (event.key === "Escape") {
      event.preventDefault(); event.stopPropagation(); finish();
    } else if (event.key === "Tab" && view === "menu") {
      finish();
    } else if (view === "menu" && ["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
      event.preventDefault(); item.current?.focus({ preventScroll: true });
    } else if (event.key === "Tab" && view === "rename") {
      const controls = [...root.current!.querySelectorAll<HTMLElement>('[role="dialog"] textarea, [role="dialog"] button')];
      const first = controls[0], last = controls.at(-1);
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus({ preventScroll: true }); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus({ preventScroll: true }); }
    }
  }}>
    <Button variant="plain" shape="icon" buttonRef={trigger} disabled={disabled}
      aria-label={`Actions for transform ${index + 1}`} title="Transform actions"
      aria-haspopup="menu" aria-expanded={view !== undefined} aria-controls={view ? `${id}-${view}` : undefined}
      onKeyDown={event => {
        if (!disabled && ["ArrowDown", "ArrowUp"].includes(event.key)) { event.preventDefault(); setView("menu"); }
      }}
      onClick={() => setView(current => current ? undefined : "menu")}><span aria-hidden="true">⋯</span></Button>
    {view === "menu" && <div id={`${id}-menu`} class="middleware-name-menu" role="menu"
      aria-label={`Actions for transform ${index + 1}`}
      style={position}>
      <Button variant="plain" role="menuitem" aria-haspopup="dialog" buttonRef={item} onClick={() => {
        setDraft(name); setView("rename");
      }}>{name ? "Rename" : "Set name"}</Button>
    </div>}
    {view === "rename" && <section id={`${id}-rename`} class="middleware-name-dialog" role="dialog"
      aria-labelledby={`${id}-label`} aria-describedby={`${id}-hint`}
      style={dialogPosition}>
      <label id={`${id}-label`} for={`${id}-input`}>Transformation name</label>
      <AutofillResistantTextarea id={`${id}-input`} textareaRef={input} rows={2} value={draft} disabled={disabled}
        onInput={event => setDraft(event.currentTarget.value)}
        onKeyDown={event => {
          if (event.key === "Enter" && !event.shiftKey && !event.isComposing) { event.preventDefault(); save(); }
        }} />
      <p id={`${id}-hint`}>Leave empty to use the transformation type.</p>
      <footer><Button variant="plain" onClick={finish}>Cancel</Button>
        <Button variant="primary" disabled={disabled} onClick={save}>Save</Button></footer>
    </section>}
  </div>;
}
