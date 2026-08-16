export function parsePartitionIds(raw: string): {
  value?: number[];
  error?: string;
} {
  const text = raw.trim();
  if (text === "") return { value: [] };
  const result: number[] = [];
  const seen = new Set<number>();
  for (const rawPart of text.split(",")) {
    const part = rawPart.trim();
    const match = /^(\d+)(?:-(\d+))?$/.exec(part);
    if (match === null)
      return { error: `Invalid partition range '${part || rawPart}'` };
    const first = Number(match[1]);
    const last = Number(match[2] ?? match[1]);
    if (!Number.isSafeInteger(first) || !Number.isSafeInteger(last))
      return { error: "Partition IDs must be safe non-negative integers" };
    if (last < first) return { error: `Range '${part}' ends before it starts` };
    if (last - first > 10_000 || result.length + last - first + 1 > 10_000)
      return { error: "At most 10,000 partitions can be selected" };
    for (let id = first; id <= last; id += 1) {
      if (seen.has(id)) return { error: `Partition ${id} is selected twice` };
      seen.add(id);
      result.push(id);
    }
  }
  return { value: result };
}

export function closestArrowType(jsonType: string): string {
  return (
    {
      string: "Utf8",
      number: "Float64",
      boolean: "Boolean",
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
