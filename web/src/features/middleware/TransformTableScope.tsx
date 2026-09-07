import { useEffect, useState } from "preact/hooks";
import type { TableIdentity, TableRule } from "../../generated/apiContract";
import { useTableCatalog } from "../../schema/tableCatalog";
import { MatchedTablesDisclosure } from "../tableSelection/MatchedTablesDisclosure";
import { AvailableTablesButton } from "../tableSelection/AvailableTablesDialog";
import { TableRuleFields } from "../tableSelection/TableRuleFields";
import { hasPattern } from "../tableSelection/model";

const SCOPE_HELP = "Each step matches the complete current table name and sees the name and columns produced by previous steps. Exclude applies only to this step. A table that does not match passes through unchanged; matching tables must have the columns this transform requires.";

export function useTransformMatches(rule: TableRule, enabled = true) {
  const catalog = useTableCatalog();
  const [result, setResult] = useState<{ key: string; catalog: TableIdentity[]; tables?: TableIdentity[]; error?: string }>();
  const key = JSON.stringify(rule);
  const tables = catalog?.tables, preview = catalog?.preview;
  useEffect(() => {
    if (!enabled || !tables || !preview || !rule.include) return;
    const controller = new AbortController();
    const timer = setTimeout(() => {
      void preview({ catalog: tables, selection: { type: "selected", rules: [rule] } }, controller.signal)
        .then(response => {
          // Unlike a source selection, a transform matching zero tables simply
          // passes them through. Each step has an independent scope.
          if (!controller.signal.aborted) setResult({ key, catalog: tables, tables: response.cards[0]?.selected ?? [] });
        }).catch(error => {
          if (!controller.signal.aborted) setResult({ key, catalog: tables, error: error instanceof Error ? error.message : String(error) });
        });
    }, 150);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [key, tables, preview, enabled]);
  return result?.key === key && result.catalog === tables ? result : undefined;
}

export function TransformTableScope({ id, index, matches: current, rule, disabled, onChange, onUseTable }: {
  id: string; index: number; matches: ReturnType<typeof useTransformMatches>; rule: TableRule; disabled: boolean;
  onChange: (patch: Partial<TableRule>) => void;
  onUseTable: ((table: TableIdentity) => void) | undefined;
}) {
  const catalog = useTableCatalog();
  const [matchedOpen, setMatchedOpen] = useState(false);
  const [excludeExpanded, setExcludeExpanded] = useState(false);
  const showMatches = matchedOpen || hasPattern(rule.include, rule.include_mode ?? "glob");
  const confirmed = !showMatches && rule.include.length > 0 && current?.tables?.length === 1 && !current?.error;
  return <div class="middleware-table-scope">
    <AvailableTablesButton label={`Available tables for transform ${index + 1}`} title="Browse tables selected in the source"
      onUse={onUseTable} showUse />
    <TableRuleFields id={id} rule={rule} labelSuffix={`transform ${index + 1}`} disabled={disabled}
      excludeExpanded={excludeExpanded} onExcludeExpanded={setExcludeExpanded} onChange={onChange}
      confirmed={confirmed} includeHelp={SCOPE_HELP} excludeHelp={SCOPE_HELP} onUse={() => setMatchedOpen(false)} />
    {showMatches ? <MatchedTablesDisclosure id={`${id}-matched`} label="Matched tables" headerClass="table-rule-result"
      toggleLabel={`Matched tables for transform ${index + 1}`} regionLabel={`Matched tables for transform ${index + 1}`}
      tables={current?.tables} open={matchedOpen} onToggle={() => setMatchedOpen(!matchedOpen)}
      after={<span class="middleware-scope-status" role="status" title={current?.error}>
        {current?.error ? "Invalid pattern" : !catalog ? "Use Discover tables in Tables first" : ""}
      </span>} /> : <div class="table-rule-result"><span class="middleware-scope-status" role="status" title={current?.error}>
        {current?.error ? "Invalid pattern" : ""}</span></div>}
  </div>;
}
