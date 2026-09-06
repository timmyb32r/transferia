import type { PatternMode, SelectionIssue, TableIdentity } from "../../generated/apiContract";

export function qualifiedName(table: TableIdentity): string {
  const part = (value: string) => value.replaceAll("\\", "\\\\").replaceAll(".", "\\.");
  return `${part(table.namespace)}.${part(table.name)}`;
}

export function exactPattern(table: TableIdentity, mode: PatternMode): string {
  return qualifiedName(table).replace(mode === "regex" ? /[\\.^$*+?()[\]{}|]/g : /[\\*?.]/g, "\\$&");
}

export function hasPattern(value: string, mode: PatternMode): boolean {
  return mode === "regex" ? value.length > 0 : /(^|[^\\])(?:\\\\)*[*?]/u.test(value);
}

export function completionPattern(value: string, mode: PatternMode): string {
  return mode === "glob" && !hasPattern(value, mode) ? `${value}*` : value;
}

export function literalPatternPrefix(value: string, mode: PatternMode): string {
  let prefix = "";
  let escaped = false;
  for (const character of value) {
    if (escaped) { prefix += character; escaped = false; }
    else if (character === "\\") escaped = true;
    else if ((mode === "glob" ? "*?" : ".^$*+?()[{}|").includes(character)) break;
    else prefix += character;
  }
  return prefix;
}

// PatternError is currently returned as an HTTP error message. Its zero-based
// index refers to the submitted (completed) rules, not the editor's draft rows.
export function tablePreviewError(error: unknown, indices: number[]): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.replace(/^Invalid table rule at card index (\d+), (Include|Exclude): /,
    (prefix, index: string, field: string) => indices[Number(index)] === undefined ? prefix
      : `Rule ${indices[Number(index)]! + 1}, ${field}: `);
}

export function selectionIssue(issue: SelectionIssue): string {
  if (issue.kind === "no_rules") return "Add at least one table rule.";
  if (issue.kind === "empty_match") return `Rule ${issue.card + 1} selects no tables.`;
  return `${qualifiedName(issue.table)}: rules ${issue.first_card + 1} and ${issue.second_card + 1} ${
    issue.conflict === "multiple_includes" ? "both include this table" : "include and exclude the same table"
  }.`;
}
