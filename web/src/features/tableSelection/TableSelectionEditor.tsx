import { useEffect, useId, useState } from "preact/hooks";
import type { EmptyMatches, PatternMode, SelectionPreview, TableRule, TableSelection } from "../../generated/apiContract";
import type { JsonValue } from "../../json";
import { isObject } from "../../schema/value";
import { useTableCatalog } from "../../schema/tableCatalog";
import { AutofillResistantInput } from "../../ui/AutofillResistantField";
import { Button } from "../../ui/Button";
import { FormField } from "../../ui/FormField";
import { SelectControl } from "../../ui/SelectControl";
import { exactPattern, qualifiedName, selectionIssue } from "./model";

export function TableSelectionEditor({ value, disabled = false, fixed = false, onChange }: {
  value: JsonValue; disabled?: boolean | undefined; fixed?: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const catalog = useTableCatalog();
  const id = useId();
  const selection: TableSelection = isObject(value) && Array.isArray(value.rules)
    ? value as unknown as TableSelection : { rules: [], empty_matches: "fail_validation" };
  const fingerprint = JSON.stringify(selection);
  const [preview, setPreview] = useState<{ fingerprint: string; tables: NonNullable<typeof catalog>["tables"]; result?: SelectionPreview; error?: string }>();
  const [expanded, setExpanded] = useState<number[]>([]);
  const tables = catalog?.tables;
  const requestPreview = catalog?.preview;
  useEffect(() => {
    setPreview(undefined);
    if (!tables || !requestPreview) return;
    const controller = new AbortController();
    // Immediate pending feedback is rendered in the already reserved region.
    const timer = setTimeout(() => {
      void requestPreview({ selection: JSON.parse(fingerprint) as TableSelection, catalog: tables }, controller.signal)
        .then(result => { if (!controller.signal.aborted) setPreview({ fingerprint, tables, result }); })
        .catch(error => { if (!controller.signal.aborted) setPreview({ fingerprint, tables, error: String(error) }); });
    }, 150);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [fingerprint, tables, requestPreview]);
  const current = catalog && preview?.fingerprint === fingerprint && preview.tables === tables ? preview : undefined;
  const change = (next: TableSelection) => onChange(next as unknown as JsonValue);
  const update = (index: number, patch: Partial<TableRule>) => change({ ...selection,
    rules: selection.rules.map((rule, position) => position === index ? { ...rule, ...patch } : rule),
  });
  return <section class="table-selection-editor">
    <div class="table-selection-status" role="status" aria-busy={!!catalog && !current}>
      {!catalog ? "Check connection successfully to load accessible tables and enable table rules."
        : current?.error ?? current?.result?.issues.map(selectionIssue).join("\n") ?? "Updating matched tables…"}
    </div>
    <FormField label="If a table rule matches nothing" optional={false}
      description="Fail validation stops before destination changes when a rule selects no tables after its own Exclude. Allow empty matches permits individual empty rules, but the combined selection must contain at least one table; otherwise the delivery fails before destination preparation. Invalid expressions and conflicts always remain errors.">
      <SelectControl value={selection.empty_matches ?? "fail_validation"} placeholder="Fail validation"
        disabled={disabled} clearable={false} options={[
          { value: "fail_validation", label: "Fail validation" },
          { value: "allow_empty_matches", label: "Allow empty matches" },
        ]} onChange={empty_matches => change({ ...selection, empty_matches: empty_matches as EmptyMatches })} />
    </FormField>
    {fixed && <p class="muted">Table patterns are resolved at delivery startup. Tables created later are not added automatically.</p>}
    {selection.rules.map((rule, index) => {
      const mode = rule.mode ?? "glob";
      const matches = current?.result?.cards[index]?.selected ?? [];
      const all = expanded.includes(index);
      return <section class="table-rule-card" key={index} aria-label={`Table rule ${index + 1}`}>
        <div class="table-rule-heading"><strong>Rule {index + 1}</strong>
          <Button disabled={disabled} onClick={() => change({ ...selection, rules: selection.rules.filter((_, position) => position !== index) })}>Remove rule</Button>
        </div>
        <FormField label="Pattern mode" optional={false} description="Glob is the default: * matches any characters, ? matches one character, and underscore is literal. Regex matches the whole qualified name. A backslash escapes special characters. Suggestions insert an exact escaped expression, without renaming tables.">
          <SelectControl value={mode} placeholder="Glob" disabled={disabled} clearable={false}
            options={[{ value: "glob", label: "Glob / exact name" }, { value: "regex", label: "Regular expression" }]}
            onChange={mode => update(index, { mode: mode as PatternMode })} />
        </FormField>
        <div class="table-rule-patterns">
          <FormField label="Include" optional={false} controlId={`${id}-${index}-include`}
            description="Use schema.table in PostgreSQL or database.table in MySQL/ClickHouse. Literal dots and backslashes within an identifier are escaped in the qualified representation. Choose a suggestion for exact escaping.">
            <AutofillResistantInput type="text" id={`${id}-${index}-include`} list={`${id}-${index}-suggestions`}
              value={rule.include} disabled={disabled || !catalog} onInput={event => update(index, { include: event.currentTarget.value })} />
            <datalist id={`${id}-${index}-suggestions`}>
              {(tables ?? []).filter(table => qualifiedName(table).toLowerCase().includes(rule.include.toLowerCase()))
                .slice(0, 30).map(table => <option value={exactPattern(table, mode)}>{qualifiedName(table)}</option>)}
            </datalist>
          </FormField>
          <FormField label="Exclude" optional controlId={`${id}-${index}-exclude`}
            description="Exclude subtracts only from this card's Include matches. A table included by another card and excluded here is a conflict. Two cards including the same table also conflict; no winner is chosen silently.">
            <AutofillResistantInput type="text" id={`${id}-${index}-exclude`} value={rule.exclude ?? ""}
              disabled={disabled || !catalog} onInput={event => update(index, { exclude: event.currentTarget.value })} />
          </FormField>
        </div>
        <div class="table-rule-heading"><span>Matched tables: {current?.result ? matches.length : "—"}</span>
          <Button disabled={!current?.result || matches.length <= 5} onClick={() => setExpanded(all ? expanded.filter(item => item !== index) : [...expanded, index])}>{all ? "Show first 5" : "Show all"}</Button>
        </div>
        <div class="table-rule-matches" aria-label={`Matched tables for rule ${index + 1}`}>
          {(all ? matches : matches.slice(0, 5)).map(table => <div>{qualifiedName(table)}</div>)}
        </div>
        <small class="muted">Preview uses the last successful connection check. Startup checks the current catalog again.</small>
      </section>;
    })}
    <Button disabled={disabled || !catalog} onClick={() => change({ ...selection, rules: [...selection.rules, { include: "", mode: "glob" }] })}>Add table rule</Button>
  </section>;
}
