import { useEffect, useId, useRef, useState } from "preact/hooks";
import type { PatternMode, SelectionPreview, TableRule, TableSelection } from "../../generated/apiContract";
import type { JsonValue } from "../../json";
import { isObject } from "../../schema/value";
import { useTableCatalog } from "../../schema/tableCatalog";
import { AutofillResistantInput } from "../../ui/AutofillResistantField";
import { Button } from "../../ui/Button";
import { FormField } from "../../ui/FormField";
import { SegmentedControl } from "../../ui/SegmentedControl";
import { TrashIcon } from "../../ui/icons";
import { exactPattern, qualifiedName, selectionIssue } from "./model";

const GLOB_HELP = "Glob / wildcard: * matches any number of characters; ? matches one character. Backslash escapes literal wildcards. Click to enable regex.";
const REGEX_HELP = "Regex matches the entire qualified name. Use .* for any characters and . for one character; * and ? are quantifiers. Click to use glob / wildcard.";
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
    selected: { type: "selected", rules: [] }, all: { type: "all", exclude: "", exclude_mode: "glob" },
  });
  drafts.current[selection.type] = selection;
  const fingerprint = JSON.stringify(selection);
  const incomplete = selection.type === "selected" && (selection.rules.length === 0 || selection.rules.some(rule => !rule.include.trim()));
  const [preview, setPreview] = useState<{ fingerprint: string; tables: NonNullable<typeof catalog>["tables"]; result?: SelectionPreview; error?: string }>();
  const [expanded, setExpanded] = useState<number[]>([]);
  const tables = catalog?.tables;
  const requestPreview = catalog?.preview;
  useEffect(() => {
    setPreview(undefined);
    if (!tables || !requestPreview || incomplete) return;
    const controller = new AbortController();
    const timer = setTimeout(() => {
      void requestPreview({ selection: JSON.parse(fingerprint) as TableSelection, catalog: tables }, controller.signal)
        .then(result => { if (!controller.signal.aborted) setPreview({ fingerprint, tables, result }); })
        .catch(error => { if (!controller.signal.aborted) setPreview({ fingerprint, tables, error: String(error) }); });
    }, 150);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [fingerprint, tables, requestPreview, incomplete]);
  const current = catalog && preview?.fingerprint === fingerprint && preview.tables === tables ? preview : undefined;
  const change = (next: TableSelection) => onChange(next as unknown as JsonValue);
  const allTables = selection.type === "all";
  const rules: TableRule[] = selection.type === "selected" ? (selection.rules.length ? selection.rules : [{ include: "" }])
    : [{ include: "*", exclude: selection.exclude ?? "", exclude_mode: selection.exclude_mode ?? "glob" }];
  const update = (index: number, patch: Partial<TableRule>) => {
    if (selection.type === "all") change({ ...selection, ...patch });
    else change({ ...selection, rules: rules.map((rule, i) => i === index ? { ...rule, ...patch } : rule) });
  };
  return <section class="table-selection-editor">
    <SegmentedControl label="Tables to transfer" value={selection.type} disabled={disabled || !catalog}
      options={[{ value: "selected", label: "Selected tables" }, { value: "all", label: "All tables" }]}
      onChange={type => { setExpanded([]); change(drafts.current[type]); }} />
    <div class="table-selection-status" role="status" aria-busy={!!catalog && !incomplete && !current}>
      {!catalog ? "Check connection successfully to load accessible tables and enable table rules."
        : incomplete ? "Enter a table name or pattern."
        : current?.error ?? current?.result?.issues.map(selectionIssue).join("\n") ?? "Updating matched tables…"}
    </div>
    {fixed && <small class="muted">Table patterns are resolved at delivery startup. Tables created later are not added automatically.</small>}
    {rules.map((rule, index) => {
      const matches = current?.result?.cards[index]?.selected ?? [];
      const invalid = current?.result?.issues.some(issue => issue.kind === "empty_match" && issue.card === index);
      const pattern = allTables || hasPattern(rule.include, rule.include_mode) || hasPattern(rule.exclude ?? "", rule.exclude_mode);
      const open = expanded.includes(index);
      const field = (kind: "include" | "exclude") => {
        const mode = rule[`${kind}_mode`] ?? "glob";
        const text = rule[kind] ?? "";
        const controlId = `${id}-${index}-${kind}`;
        return <FormField label={kind === "include" ? "Include" : "Exclude"} optional={kind === "exclude"} controlId={controlId}
          description={`${HELP} ${kind === "exclude" ? "Exclude applies only to this row." : "Include is required."}`}>
          <div class="table-pattern-input">
            <AutofillResistantInput type="text" id={controlId} list={`${controlId}-suggestions`}
              aria-label={`${kind === "include" ? "Include" : "Exclude"} rule ${index + 1}`}
              aria-invalid={kind === "include" && !!invalid} required={kind === "include"}
              value={text} disabled={disabled || !catalog} onInput={event => update(index, { [kind]: event.currentTarget.value })} />
            <Button class="regex-toggle" aria-label={`${kind} regex rule ${index + 1}`} aria-pressed={mode === "regex"}
              title={mode === "regex" ? REGEX_HELP : GLOB_HELP} disabled={disabled || !catalog}
              onClick={() => update(index, { [`${kind}_mode`]: mode === "regex" ? "glob" : "regex" })}>.*</Button>
          </div>
          <datalist id={`${controlId}-suggestions`}>
            {(tables ?? []).filter(table => qualifiedName(table).toLowerCase().includes(text.toLowerCase()))
              .slice(0, 30).map(table => <option value={exactPattern(table, mode)}>{qualifiedName(table)}</option>)}
          </datalist>
        </FormField>;
      };
      return <section class="table-rule-row" key={`${selection.type}-${index}`} aria-label={`Table rule ${index + 1}`}>
        <div class={`table-rule-patterns${allTables ? " all-tables" : ""}`}>
          {!allTables && field("include")}{field("exclude")}
          {!allTables && <Button shape="icon" aria-label={`Remove rule ${index + 1}`} title="Remove rule" disabled={disabled}
            onClick={() => { if (selection.type === "selected") { setExpanded([]); change({ ...selection, rules: selection.rules.filter((_, i) => i !== index) }); } }}>
            <TrashIcon />
          </Button>}
        </div>
        <div class="table-rule-result">
          {pattern && <Button class="matched-toggle" aria-expanded={open} aria-controls={`${id}-matches-${index}`}
            disabled={!current?.result} onClick={() => setExpanded(open ? expanded.filter(i => i !== index) : [...expanded, index])}>
            Matched tables ({current?.result ? matches.length : "—"}) {open ? "▴" : "▾"}
          </Button>}
        </div>
        {pattern && open && <div id={`${id}-matches-${index}`} class="table-rule-matches" aria-label={`Matched tables for rule ${index + 1}`}>
          {matches.map(table => <div>{qualifiedName(table)}</div>)}
        </div>}
      </section>;
    })}
    {!allTables && <Button shape="icon" aria-label="Add table rule" title="Add table rule" disabled={disabled || !catalog}
      onClick={() => { if (selection.type === "selected") change({ ...selection, rules: [...rules, { include: "" }] }); }}>+</Button>}
    <small class="muted">Preview uses the last successful connection check. Startup checks the current catalog again.</small>
  </section>;
}

function hasPattern(text: string, mode: PatternMode = "glob"): boolean {
  if (mode === "regex") return text.length > 0;
  return /(^|[^\\])(?:\\\\)*[*?]/u.test(text);
}
