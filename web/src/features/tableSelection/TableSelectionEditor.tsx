import { useEffect, useId, useRef, useState } from "preact/hooks";
import type { SelectionPreview, TableRule, TableSelection } from "../../generated/apiContract";
import type { JsonValue } from "../../json";
import { isObject } from "../../schema/value";
import { useTableCatalog } from "../../schema/tableCatalog";
import { Button } from "../../ui/Button";
import { FormField } from "../../ui/FormField";
import { SegmentedControl } from "../../ui/SegmentedControl";
import { TrashIcon } from "../../ui/icons";
import { hasPattern, qualifiedName, selectionIssue, tablePreviewError } from "./model";
import { TablePatternInput } from "./TablePatternInput";
const HELP = "Default: glob / wildcard, where * matches any number of characters and ? one character. The .* button enables regex independently for each field. Use schema.table for PostgreSQL or database.table for MySQL/ClickHouse. Suggestions escape exact names. Every row must select at least one table after exclusion. Duplicate includes and cross-row include/exclude conflicts fail validation.";

export function TableSelectionEditor({ value, disabled = false, fixed = false, onChange }: {
  value: JsonValue; disabled?: boolean | undefined; fixed?: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const catalog = useTableCatalog();
  const id = useId();
  const selection: TableSelection = isObject(value) && (value.type === "selected" || value.type === "all")
    ? value as unknown as TableSelection : { type: "selected", rules: [] };
  const drafts = useRef<Record<"selected" | "all", TableSelection>>({
    selected: { type: "selected", rules: [] }, all: { type: "all" },
  });
  drafts.current[selection.type] = selection;
  const fingerprint = JSON.stringify(selection);
  const incomplete = selection.type === "selected" && (selection.rules.length === 0 || selection.rules.some(rule => !rule.include.trim()));
  const [preview, setPreview] = useState<{ fingerprint: string; tables: NonNullable<typeof catalog>["tables"]; result?: SelectionPreview; error?: string }>();
  const [expanded, setExpanded] = useState(false);
  const [expandedRules, setExpandedRules] = useState<number[]>([]);
  const tables = catalog?.tables;
  const requestPreview = catalog?.preview;
  useEffect(() => {
    setPreview(undefined);
    if (!tables || !requestPreview) return;
    const draft = JSON.parse(fingerprint) as TableSelection;
    const indices = draft.type === "selected" ? draft.rules.flatMap((rule, index) => rule.include.trim() ? [index] : []) : [];
    if (draft.type === "selected" && indices.length === 0) return;
    // Empty rows are unfinished editor drafts, not invalid requests. Preview
    // completed rows while preserving their original indices and conflicts.
    // The saved configuration is unchanged and still validates every row.
    const evaluated = draft.type === "selected" ? { ...draft, rules: indices.map(index => draft.rules[index]!) } : draft;
    const controller = new AbortController();
    const timer = setTimeout(() => {
      void requestPreview({ selection: evaluated, catalog: tables }, controller.signal)
        .then(result => {
          if (controller.signal.aborted) return;
          if (draft.type === "selected") result = {
            cards: draft.rules.map((_, index) => result.cards[indices.indexOf(index)] ?? { selected: [], excluded: [] }),
            issues: result.issues.map(issue => issue.kind === "empty_match" ? { ...issue, card: indices[issue.card]! }
              : issue.kind === "conflict" ? { ...issue, first_card: indices[issue.first_card]!, second_card: indices[issue.second_card]! } : issue),
          };
          setPreview({ fingerprint, tables, result });
        })
        .catch(error => { if (!controller.signal.aborted) setPreview({ fingerprint, tables, error: tablePreviewError(error, indices) }); });
    }, 150);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [fingerprint, tables, requestPreview, incomplete]);
  const current = catalog && preview?.fingerprint === fingerprint && preview.tables === tables ? preview : undefined;
  const change = (next: TableSelection) => onChange(next as unknown as JsonValue);
  const allTables = selection.type === "all";
  const rules: TableRule[] = selection.type === "selected" ? (selection.rules.length ? selection.rules : [{ include: "" }])
    : [];
  const update = (index: number, patch: Partial<TableRule>) => {
    if (selection.type === "selected") change({ ...selection, rules: rules.map((rule, i) => i === index ? { ...rule, ...patch } : rule) });
  };
  const matches = [...new Map((current?.result?.cards ?? []).flatMap(card => card.selected)
    .map(table => [JSON.stringify([table.namespace, table.name]), table])).values()];
  const issue = current?.error || current?.result?.issues.map(selectionIssue).join(" ");
  const status = !catalog ? "Check connection to choose tables."
    : issue || (incomplete ? "Enter a table name or pattern." : "");
  const help = `${HELP} All tables selects every accessible table in the discovered catalog. Preview uses the last successful connection check; startup checks the catalog again.${fixed ? " Table patterns are resolved at delivery startup. Tables created later are not added automatically." : ""}`;
  return <section class="table-selection-editor">
    <div class="table-selection-toolbar">
    <SegmentedControl label="Tables to transfer" value={selection.type} disabled={disabled || !catalog}
      options={[{ value: "selected", label: "Selected tables" }, { value: "all", label: "All tables" }]}
      onChange={type => { setExpanded(false); setExpandedRules([]); change(drafts.current[type]); }} />
      <span class="help" tabIndex={0} title={help} aria-label="About table selection" aria-describedby={`${id}-help`}>
        <span aria-hidden="true">?</span>
        <span id={`${id}-help`} role="tooltip" class="visually-hidden">{help}</span>
      </span>
    </div>
    {rules.map((rule, index) => {
      const invalid = current?.result?.issues.some(issue => issue.kind === "empty_match" && issue.card === index);
      const field = (kind: "include" | "exclude") => {
        const mode = rule[`${kind}_mode`] ?? "glob";
        const text = rule[kind] ?? "";
        const controlId = `${id}-${index}-${kind}`;
        return <FormField label={kind === "include" ? "Include" : "Exclude"} optional={kind === "exclude"} controlId={controlId}
          description={`${HELP} ${kind === "exclude" ? "Exclude applies only to this row." : "Include is required."}`}>
          <TablePatternInput id={controlId} label={`${kind === "include" ? "Include" : "Exclude"} rule ${index + 1}`}
            value={text} mode={mode} disabled={disabled || !catalog} required={kind === "include"}
            invalid={kind === "include" && !!invalid} onChange={value => update(index, { [kind]: value })}
            onModeChange={mode => update(index, { [`${kind}_mode`]: mode })} />
        </FormField>;
      };
      return <section class="table-rule-row" key={`${selection.type}-${index}`} aria-label={`Table rule ${index + 1}`}>
        <div class="table-rule-patterns">
          {field("include")}{field("exclude")}
          <Button shape="icon" aria-label={`Remove rule ${index + 1}`} title="Remove rule" disabled={disabled}
            onClick={() => { if (selection.type === "selected") { setExpanded(false); setExpandedRules([]); change({ ...selection, rules: selection.rules.filter((_, i) => i !== index) }); } }}>
            <TrashIcon />
          </Button>
        </div>
        <div class="table-rule-result">
          {(expandedRules.includes(index) || hasPattern(rule.include, rule.include_mode ?? "glob") || hasPattern(rule.exclude ?? "", rule.exclude_mode ?? "glob")) &&
            <Button class="matched-toggle" aria-label={`Matched tables for rule ${index + 1}`}
              aria-expanded={expandedRules.includes(index)} aria-controls={`${id}-rule-${index}-matches`} disabled={!current?.result}
              onClick={() => setExpandedRules(expandedRules.includes(index) ? expandedRules.filter(item => item !== index) : [...expandedRules, index])}>
              <span class="table-matches-chevron" aria-hidden="true" />
              Matched tables <span class="table-match-count">{current?.result ? current.result.cards[index]?.selected.length ?? 0 : "—"}</span>
            </Button>}
        </div>
        {expandedRules.includes(index) && <div id={`${id}-rule-${index}-matches`} class="table-rule-matches" aria-label={`Matches for rule ${index + 1}`} aria-busy={!current?.result}>
          {!current?.result ? <div>Waiting for a valid table selection…</div>
            : current.result.cards[index]?.selected.length === 0 ? <div>No matched tables.</div>
            : (current.result.cards[index]?.selected ?? []).map(table => <div key={JSON.stringify([table.namespace, table.name])}>{qualifiedName(table)}</div>)}
        </div>}
      </section>;
    })}
    <div class="table-selection-footer">
    {!allTables && <Button shape="icon" aria-label="Add table rule" title="Add table rule" disabled={disabled || !catalog}
      onClick={() => { if (selection.type === "selected") change({ ...selection, rules: [...rules, { include: "" }] }); }}>+</Button>}
      <Button class="matched-toggle" aria-expanded={expanded} aria-controls={`${id}-matches`}
        disabled={!current?.result} onClick={() => setExpanded(!expanded)}>
        <span class="table-matches-chevron" aria-hidden="true" />
        Matched tables <span class="table-match-count">{current?.result ? matches.length : "—"}</span>
      </Button>
      <span class={`table-selection-status${issue ? " has-error" : ""}`} role="status" title={status || undefined}
        aria-busy={!!catalog && !incomplete && !current}>{status}</span>
    </div>
    {expanded && <div id={`${id}-matches`} class="table-rule-matches" aria-label="All matched tables"
      aria-busy={!current?.result}>
      {!current?.result ? <div>Waiting for a valid table selection…</div>
        : matches.length === 0 ? <div>No matched tables.</div>
        : matches.map(table => <div key={JSON.stringify([table.namespace, table.name])}>{qualifiedName(table)}</div>)}
    </div>}
  </section>;
}
