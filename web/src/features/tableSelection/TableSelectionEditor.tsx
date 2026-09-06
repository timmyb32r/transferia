import type { ComponentChildren } from "preact";
import { useEffect, useId, useLayoutEffect, useRef, useState } from "preact/hooks";
import type { SelectionPreview, TableRule, TableSelection } from "../../generated/apiContract";
import type { JsonValue } from "../../json";
import { isObject } from "../../schema/value";
import { useTableCatalog } from "../../schema/tableCatalog";
import { Button } from "../../ui/Button";
import { FormField } from "../../ui/FormField";
import { SegmentedControl } from "../../ui/SegmentedControl";
import { TrashIcon } from "../../ui/icons";
import { exactPattern, hasPattern, selectionIssue, tablePreviewError } from "./model";
import { MatchedTablesDisclosure } from "./MatchedTablesDisclosure";
import { TablePatternInput } from "./TablePatternInput";
import { AvailableTablesButton, AvailableTablesDialog } from "./AvailableTablesDialog";
import { useTableNamespace } from "./naming";
const HELP = "Default: glob / wildcard, where * matches any number of characters and ? one character. The .* button enables regex independently for each field.";
const RULE_HELP = "Suggestions escape exact names. Every row must select at least one table after exclusion. Duplicate includes and cross-row include/exclude conflicts fail validation.";

export function TableSelectionEditor({ value, disabled = false, fixed = false, onChange, toolbar }: {
  value: JsonValue; disabled?: boolean | undefined; fixed?: boolean;
  onChange: (value: JsonValue) => void;
  toolbar?: ComponentChildren;
}) {
  const catalog = useTableCatalog();
  const namespace = useTableNamespace();
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
  const [expandedExcludes, setExpandedExcludes] = useState<number[]>([]);
  const [browseRule, setBrowseRule] = useState<number>();
  const focusField = useRef<string>();
  useLayoutEffect(() => { if (!catalog || disabled) setBrowseRule(undefined); }, [catalog, disabled]);
  useLayoutEffect(() => {
    if (focusField.current === undefined) return;
    document.getElementById(focusField.current)?.focus({ preventScroll: true });
    focusField.current = undefined;
  });
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
  const issue = current?.error || current?.result?.issues.map(issue => selectionIssue(issue, selection.type)).join(" ");
  const status = !catalog ? ""
    : issue || (incomplete ? "Enter a table name or pattern." : "");
  const includeHelp = `Include is required. Preview uses the last successful connection check; startup checks the catalog again.${fixed ? " Table patterns are resolved at delivery startup. Tables created later are not added automatically." : ""}`;
  return <section class={`table-selection-editor${incomplete && !disabled && catalog ? " required-incomplete" : ""}`}>
    <header class="table-selection-heading"><h3>Tables</h3>
      <AvailableTablesButton label="Available tables in source" title="Browse available source tables and schema status" showMetadata />
    </header>
    <div class="table-selection-toolbar">
    <SegmentedControl label="Tables to transfer" value={selection.type} disabled={disabled || !catalog}
      options={[{ value: "selected", label: "Selected tables" }, { value: "all", label: "All tables" }]}
      onChange={type => { setExpanded(false); setExpandedRules([]); setExpandedExcludes([]); change(drafts.current[type]); }} />
    {toolbar}
    </div>
    {rules.map((rule, index) => {
      const excludeOpen = !!rule.exclude || expandedExcludes.includes(index);
      const invalid = current?.result?.issues.some(issue => issue.kind === "empty_match" && issue.card === index);
      // Keep an explicitly opened viewport until the user closes it. A typed
      // exact name must not pull later controls upward under an active pointer.
      const showMatches = expandedRules.includes(index) || hasPattern(rule.include, rule.include_mode ?? "glob");
      const rowIssue = current?.result?.issues.some(issue => issue.kind === "no_rules"
        || (issue.kind === "empty_match" ? issue.card === index : issue.first_card === index || issue.second_card === index));
      const exactFound = !showMatches && rule.include.trim().length > 0
        && current?.result?.cards[index]?.selected.length === 1 && !rowIssue;
      const field = (kind: "include" | "exclude") => {
        const mode = rule[`${kind}_mode`] ?? "glob";
        const text = rule[kind] ?? "";
        const controlId = `${id}-${index}-${kind}`;
        return <FormField label={kind === "include" ? "Include" : "Exclude"} optional={false} controlId={controlId}
          description={`${HELP} Use ${namespace}.table or ${namespace}.*. ${RULE_HELP} ${kind === "exclude" ? "Exclude applies only to this row." : includeHelp}`}>
          <TablePatternInput id={controlId} label={`${kind === "include" ? "Include" : "Exclude"} rule ${index + 1}`}
            value={text} mode={mode} disabled={disabled || !catalog} required={kind === "include"}
            invalid={kind === "include" && !!invalid} onChange={value => {
              if (kind === "exclude" && !expandedExcludes.includes(index)) setExpandedExcludes([...expandedExcludes, index]);
              update(index, { [kind]: value });
            }}
            confirmed={kind === "include" ? exactFound : undefined}
            onBrowse={kind === "include" ? () => setBrowseRule(index) : undefined}
            onModeChange={mode => update(index, { [`${kind}_mode`]: mode })} />
        </FormField>;
      };
      return <section class="table-rule-row" key={`${selection.type}-${index}`} aria-label={`Table rule ${index + 1}`}>
        <div class={`table-rule-patterns${excludeOpen ? " table-rule-with-exclude" : ""}`}>
          {field("include")}
          {excludeOpen ? <div class="table-exclude-field">
            {field("exclude")}
            <Button variant="plain" class="table-exclude-hide" aria-label={`Hide Exclude for rule ${index + 1}`}
              title={rule.exclude ? "Clear Exclude to hide it" : "Hide empty Exclude"}
              disabled={disabled || !catalog || !!rule.exclude} aria-expanded="true" aria-controls={`${id}-${index}-exclude`}
              onClick={() => {
                focusField.current = `${id}-${index}-add-exclude`;
                setExpandedExcludes(expandedExcludes.filter(item => item !== index));
              }}>Hide</Button>
          </div> : <Button id={`${id}-${index}-add-exclude`} variant="plain" class="table-exclude-add"
            aria-label={`Add Exclude for rule ${index + 1}`} aria-expanded="false" disabled={disabled || !catalog}
            onClick={() => {
              focusField.current = `${id}-${index}-exclude`;
              setExpandedExcludes([...expandedExcludes, index]);
            }}><span aria-hidden="true">+</span> Exclude</Button>}
          <Button variant="plain" shape="icon" aria-label={`Remove rule ${index + 1}`} title="Remove rule" disabled={disabled || !catalog}
            onClick={() => { if (selection.type === "selected") {
              setExpanded(false); setExpandedRules([]);
              setExpandedExcludes(expandedExcludes.filter(item => item !== index).map(item => item > index ? item - 1 : item));
              change({ ...selection, rules: selection.rules.filter((_, i) => i !== index) });
            } }}>
            <TrashIcon />
          </Button>
        </div>
        {showMatches ? <MatchedTablesDisclosure id={`${id}-rule-${index}-matches`} headerClass="table-rule-result"
          label="Matched tables" toggleLabel={`Matched tables for rule ${index + 1}`} regionLabel={`Matches for rule ${index + 1}`}
          open={expandedRules.includes(index)}
          onToggle={() => setExpandedRules(expandedRules.includes(index) ? expandedRules.filter(item => item !== index) : [...expandedRules, index])}
          tables={current?.result ? current.result.cards[index]?.selected ?? [] : undefined} />
          : <div class="table-rule-result" />}
      </section>;
    })}
    <MatchedTablesDisclosure id={`${id}-matches`} headerClass="table-selection-footer" label="All matched tables"
      open={expanded} onToggle={() => setExpanded(!expanded)} tables={current?.result ? matches : undefined}
      before={!allTables && <Button variant="plain" class="table-add-action" aria-label="Add tables" disabled={disabled || !catalog}
        onClick={() => { if (selection.type === "selected") {
          focusField.current = `${id}-${rules.length}-include`;
          change({ ...selection, rules: [...rules, { include: "" }] });
        } }}><span aria-hidden="true">+</span> Add tables</Button>} />
    <span class={`table-selection-status${issue ? " has-error" : ""}`} role="status" title={status || undefined}
      aria-busy={!!catalog && !incomplete && !current}>{status}</span>
    {browseRule !== undefined && catalog && !disabled && rules[browseRule] && <AvailableTablesDialog catalog={catalog}
      onUse={table => {
        setExpandedRules(expandedRules.filter(index => index !== browseRule));
        update(browseRule, { include: exactPattern(table, rules[browseRule]!.include_mode ?? "glob") });
      }}
      onClose={() => setBrowseRule(undefined)} />}
  </section>;
}
