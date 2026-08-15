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
      integer: "Int64",
      unsigned_integer: "UInt64",
      number: "Float64",
      boolean: "Boolean",
    }[jsonType] ?? "Utf8"
  );
}

export function isStringArrowType(value: unknown): boolean {
  return value === "Utf8" || value === "LargeUtf8";
}
