export function closestArrowType(jsonType: string): string {
  return (
    {
      string: "Utf8",
      number: "Float64",
      boolean: "Boolean",
      json: "Json",
      decimal: "Decimal128",
    }[jsonType] ?? "Utf8"
  );
}

export function isStringArrowType(value: unknown): boolean {
  return value === "Utf8" || value === "LargeUtf8";
}

export function reconcileSystemColumnKeys(
  previous: unknown,
  next: unknown,
  keys: string[],
): string[] {
  const previousObject = isRecord(previous) ? previous : {};
  const nextObject = isRecord(next) ? next : {};
  const replacements = new Map<string, string>();
  for (const property of new Set([
    ...Object.keys(previousObject),
    ...Object.keys(nextObject),
  ])) {
    const oldName = stringValue(previousObject[property]);
    const newName = stringValue(nextObject[property]);
    if (oldName !== "" && oldName !== newName)
      replacements.set(oldName, newName);
  }
  return [...new Set(keys.map((key) => replacements.get(key) ?? key))].filter(
    (key) => key !== "",
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}
