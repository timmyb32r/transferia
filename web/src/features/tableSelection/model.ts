import type { PatternMode, SelectionIssue, TableIdentity } from "../../generated/apiContract";

export function qualifiedName(table: TableIdentity): string {
  const part = (value: string) => value.replaceAll("\\", "\\\\").replaceAll(".", "\\.");
  return `${part(table.namespace)}.${part(table.name)}`;
}

export function exactPattern(table: TableIdentity, mode: PatternMode): string {
  return qualifiedName(table).replace(mode === "regex" ? /[\\.^$*+?()[\]{}|]/g : /[\\*?.]/g, "\\$&");
}

export function selectionIssue(issue: SelectionIssue): string {
  if (issue.kind === "no_rules") return "Add at least one table rule.";
  if (issue.kind === "no_tables") return "No tables selected. A delivery must select at least one table, even when empty matches are allowed for individual rules.";
  if (issue.kind === "empty_match") return `Rule ${issue.card + 1} selects no tables.`;
  return `${qualifiedName(issue.table)}: rules ${issue.first_card + 1} and ${issue.second_card + 1} ${
    issue.conflict === "multiple_includes" ? "both include this table" : "include and exclude the same table"
  }.`;
}
