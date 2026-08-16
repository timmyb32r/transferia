// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AppearanceSettings } from "../src/ui/AppearanceSettings";
import {
  APPEARANCE_STORAGE_KEY,
  applyAppearance,
  loadAppearance,
  saveAppearance,
  type Appearance,
} from "../src/ui/appearance";

describe("appearance preferences", () => {
  const values = new Map<string, string>();
  const storage = {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
  };

  afterEach(() => {
    cleanup();
    values.clear();
    delete document.documentElement.dataset.design;
    delete document.documentElement.dataset.theme;
    document.documentElement.style.colorScheme = "";
  });

  it("uses the existing dark yandex-cloud design by default", () => {
    expect(loadAppearance(storage)).toEqual({
      design: "yandex-cloud",
      theme: "dark",
    });
  });

  it("rejects corrupt or unsupported persisted values", () => {
    storage.setItem(APPEARANCE_STORAGE_KEY, "not-json");
    expect(loadAppearance(storage)).toEqual({
      design: "yandex-cloud",
      theme: "dark",
    });

    storage.setItem(
      APPEARANCE_STORAGE_KEY,
      JSON.stringify({ design: "unknown", theme: "light" }),
    );
    expect(loadAppearance(storage)).toEqual({
      design: "yandex-cloud",
      theme: "dark",
    });
  });

  it("persists and applies both independent dimensions", () => {
    const appearance: Appearance = { design: "airy-v0", theme: "light" };
    saveAppearance(storage, appearance);
    applyAppearance(document.documentElement, appearance);

    expect(loadAppearance(storage)).toEqual(appearance);
    expect(document.documentElement.dataset.design).toBe("airy-v0");
    expect(document.documentElement.dataset.theme).toBe("light");
    expect(document.documentElement.style.colorScheme).toBe("light");
  });

  it("offers every design and theme from the sidebar settings", () => {
    const onChange = vi.fn();
    const view = render(
      <AppearanceSettings
        value={{ design: "yandex-cloud", theme: "dark" }}
        onChange={onChange}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: /Settings/ }));

    expect(view.getByRole("radio", { name: /yandex-cloud/ })).toBeTruthy();
    expect(view.getByRole("radio", { name: /airy \(adopted\)/ })).toBeTruthy();
    expect(view.getByRole("radio", { name: "Light" })).toBeTruthy();
    expect(view.getByRole("radio", { name: "Dark" })).toBeTruthy();

    fireEvent.click(view.getByRole("radio", { name: /airy \(adopted\)/ }));
    expect(onChange).toHaveBeenCalledWith({
      design: "airy-v0",
      theme: "dark",
    });

    fireEvent.click(view.getByRole("radio", { name: "Light" }));
    expect(onChange).toHaveBeenCalledWith({
      design: "yandex-cloud",
      theme: "light",
    });
  });
});
