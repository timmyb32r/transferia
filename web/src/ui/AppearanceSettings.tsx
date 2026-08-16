import { useState } from "preact/hooks";

import { Button } from "./Button";
import type { Appearance, ColorTheme, InterfaceDesign } from "./appearance";

const DESIGNS: ReadonlyArray<{
  value: InterfaceDesign;
  label: string;
  description: string;
}> = [
  {
    value: "yandex-cloud",
    label: "yandex-cloud",
    description: "Compact and operational",
  },
  {
    value: "airy-v0",
    label: "airy (adopted)",
    description: "Open and lightweight",
  },
];

const THEMES: ReadonlyArray<{ value: ColorTheme; label: string }> = [
  { value: "light", label: "Light" },
  { value: "dark", label: "Dark" },
];

function SettingsIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true">
      <path d="M6.8 1.5h2.4l.4 1.6c.4.1.8.3 1.2.5l1.4-.8 1.7 1.7-.8 1.4c.2.4.4.8.5 1.2l1.6.4v2.4l-1.6.4c-.1.4-.3.8-.5 1.2l.8 1.4-1.7 1.7-1.4-.8c-.4.2-.8.4-1.2.5l-.4 1.6H6.8l-.4-1.6a5 5 0 0 1-1.2-.5l-1.4.8-1.7-1.7.8-1.4a5 5 0 0 1-.5-1.2L.8 9.9V7.5l1.6-.4c.1-.4.3-.8.5-1.2l-.8-1.4 1.7-1.7 1.4.8c.4-.2.8-.4 1.2-.5l.4-1.6Z" />
      <circle cx="8" cy="8.7" r="2.2" />
    </svg>
  );
}

export function AppearanceSettings({
  value,
  onChange,
}: {
  value: Appearance;
  onChange: (appearance: Appearance) => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <div class={open ? "appearance-settings open" : "appearance-settings"}>
      {open && (
        <div
          id="appearance-settings-panel"
          class="appearance-panel"
          aria-label="Appearance settings"
        >
          <div class="appearance-panel-heading">
            <div>
              <small>INTERFACE</small>
              <strong>Appearance</strong>
            </div>
            <span aria-hidden="true">Aa</span>
          </div>

          <fieldset class="design-options">
            <legend>Design</legend>
            {DESIGNS.map((design) => (
              <label
                key={design.value}
                class={
                  value.design === design.value
                    ? "design-option selected"
                    : "design-option"
                }
              >
                <input
                  type="radio"
                  name="interface-design"
                  value={design.value}
                  checked={value.design === design.value}
                  onChange={() => onChange({ ...value, design: design.value })}
                />
                <span
                  class={`design-preview ${design.value}`}
                  aria-hidden="true"
                >
                  <i />
                  <b />
                  <em />
                </span>
                <span class="design-option-copy">
                  <strong>{design.label}</strong>
                  <small>{design.description}</small>
                </span>
                <span class="design-check" aria-hidden="true">
                  ✓
                </span>
              </label>
            ))}
          </fieldset>

          <fieldset class="theme-options">
            <legend>Theme</legend>
            <div>
              {THEMES.map((theme) => (
                <label
                  key={theme.value}
                  class={value.theme === theme.value ? "selected" : undefined}
                >
                  <input
                    type="radio"
                    name="color-theme"
                    value={theme.value}
                    checked={value.theme === theme.value}
                    onChange={() => onChange({ ...value, theme: theme.value })}
                  />
                  <span
                    class={`theme-icon ${theme.value}`}
                    aria-hidden="true"
                  />
                  {theme.label}
                </label>
              ))}
            </div>
          </fieldset>
        </div>
      )}

      <Button
        class="settings-toggle"
        aria-expanded={open}
        aria-controls="appearance-settings-panel"
        onClick={() => setOpen((current) => !current)}
      >
        <SettingsIcon />
        <span>Settings</span>
        <span class="settings-summary">
          {value.design === "airy-v0" ? "airy (adopted)" : "yandex-cloud"} ·{" "}
          {value.theme}
        </span>
      </Button>
    </div>
  );
}
