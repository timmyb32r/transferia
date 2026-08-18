export const APPEARANCE_STORAGE_KEY = "transferia.appearance.v1";

export type InterfaceDesign = "yandex-cloud" | "airy-v0";
export type ColorTheme = "light" | "dark";

export interface Appearance {
  design: InterfaceDesign;
  theme: ColorTheme;
  autoShowSchemaWidget: boolean;
}

export const DEFAULT_APPEARANCE: Appearance = {
  design: "yandex-cloud",
  theme: "dark",
  autoShowSchemaWidget: true,
};

const isDesign = (value: unknown): value is InterfaceDesign =>
  value === "yandex-cloud" || value === "airy-v0";

const isTheme = (value: unknown): value is ColorTheme =>
  value === "light" || value === "dark";

export function loadAppearance(storage: Pick<Storage, "getItem">): Appearance {
  let stored: string | null;
  try {
    stored = storage.getItem(APPEARANCE_STORAGE_KEY);
  } catch {
    return DEFAULT_APPEARANCE;
  }
  if (stored === null) return DEFAULT_APPEARANCE;

  try {
    const value: unknown = JSON.parse(stored);
    if (
      typeof value === "object" &&
      value !== null &&
      "design" in value &&
      "theme" in value &&
      isDesign(value.design) &&
      isTheme(value.theme)
    ) {
      return {
        design: value.design,
        theme: value.theme,
        autoShowSchemaWidget:
          "autoShowSchemaWidget" in value &&
          typeof value.autoShowSchemaWidget === "boolean"
            ? value.autoShowSchemaWidget
            : true,
      };
    }
  } catch {
    // A corrupt browser preference must not prevent the editor from opening.
  }

  return DEFAULT_APPEARANCE;
}

export function saveAppearance(
  storage: Pick<Storage, "setItem">,
  appearance: Appearance,
): void {
  try {
    storage.setItem(APPEARANCE_STORAGE_KEY, JSON.stringify(appearance));
  } catch {
    // Appearance persistence is best-effort; applying it is not.
  }
}

export function applyAppearance(
  root: Pick<HTMLElement, "dataset" | "style">,
  appearance: Appearance,
): void {
  root.dataset.design = appearance.design;
  root.dataset.theme = appearance.theme;
  root.style.colorScheme = appearance.theme;
}
