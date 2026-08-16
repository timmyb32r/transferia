import { useRef, useState } from "preact/hooks";

import { anchoredMenuStyle, useAnchoredOverlay } from "../ui/overlay";

export function ColumnActions({
  row,
  disabled,
  hasSettings,
  hasCustomSettings,
  settingsExpanded,
  onSettings,
  onMoveUp,
  onMoveDown,
  onDuplicate,
  onDelete,
}: {
  row: number;
  disabled: boolean;
  hasSettings: boolean;
  hasCustomSettings: boolean;
  settingsExpanded: boolean;
  onSettings: () => void;
  onMoveUp: (() => void) | undefined;
  onMoveDown: (() => void) | undefined;
  onDuplicate: () => void;
  onDelete: () => void;
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  useAnchoredOverlay({
    open,
    root,
    trigger,
    onClose: () => setOpen(false),
    closeOnViewportChange: true,
  });
  const run = (action: () => void) => {
    setOpen(false);
    action();
  };
  return (
    <div class={`row-actions ${open ? "open" : ""}`} ref={root}>
      <button
        ref={trigger}
        class="row-action"
        type="button"
        aria-label={`Column ${row} actions`}
        aria-haspopup="menu"
        aria-expanded={open}
        disabled={disabled}
        onClick={() => setOpen((current) => !current)}
      >
        <span aria-hidden="true">⋯</span>
        {hasCustomSettings && (
          <span class="custom-settings-dot" title="Custom column settings" />
        )}
      </button>
      {open && (
        <div
          class="row-actions-menu row-actions-menu-floating"
          role="menu"
          style={anchoredMenuStyle(trigger.current, {
            width: 174,
            estimatedHeight: 165,
            align: "end",
          })}
        >
          {hasSettings && (
            <button
              type="button"
              role="menuitem"
              onClick={() => run(onSettings)}
            >
              Column settings{settingsExpanded ? " ✓" : ""}
            </button>
          )}
          <button
            type="button"
            role="menuitem"
            disabled={onMoveUp === undefined}
            onClick={() => onMoveUp && run(onMoveUp)}
          >
            Move up
          </button>
          <button
            type="button"
            role="menuitem"
            disabled={onMoveDown === undefined}
            onClick={() => onMoveDown && run(onMoveDown)}
          >
            Move down
          </button>
          <button
            type="button"
            role="menuitem"
            onClick={() => run(onDuplicate)}
          >
            Duplicate
          </button>
          <button
            class="danger"
            type="button"
            role="menuitem"
            onClick={() => run(onDelete)}
          >
            Delete
          </button>
        </div>
      )}
    </div>
  );
}
