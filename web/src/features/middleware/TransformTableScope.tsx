import { useEffect, useState } from "preact/hooks";
import type { TableIdentity, TableRule, TableLineage } from "../../generated/apiContract";
import type { JsonValue } from "../../types";
import { useTableCatalog } from "../../schema/tableCatalog";
import { MatchedTablesDisclosure } from "../tableSelection/MatchedTablesDisclosure";
import { AvailableTablesButton } from "../tableSelection/AvailableTablesDialog";
import { TableRuleFields } from "../tableSelection/TableRuleFields";

const SCOPE_HELP = "Each step matches the complete current table name and sees the name and columns produced by previous steps. Exclude applies only to this step. A table that does not match passes through unchanged; matching tables must have the columns this transform requires.";

export function useTransformMatches(rule: TableRule, enabled = true, preceding: JsonValue[] = []) {
  const catalog = useTableCatalog();
  const [result, setResult] = useState<{ key: string; prefix: string; catalog: TableIdentity[]; tables?: TableIdentity[]; sourceTables?: TableIdentity[]; lineage?: TableLineage[]; error?: string }>();
  const prefix = JSON.stringify(preceding);
  const key = JSON.stringify([rule, preceding]);
  const tables = catalog?.tables, preview = catalog?.preview;
  useEffect(() => {
    if (!enabled || !tables || !preview) return;
    const controller = new AbortController();
    const timer = setTimeout(() => {
      void preview({ catalog: tables, selection: rule.include ? { type: "selected", rules: [rule] } : { type: "all" },
        ...(preceding.length ? { preceding_middlewares: preceding } : {}) }, controller.signal)
        .then(response => {
          // Unlike a source selection, a transform matching zero tables simply
          // passes them through. Each step has an independent scope.
          if (!controller.signal.aborted) {
            const selected = rule.include ? response.cards[0]?.selected ?? [] : [];
            const lineage = response.lineage ?? tables.map(table => ({ source: table, current: table }));
            const keys = new Set(selected.map(table => JSON.stringify([table.namespace, table.name])));
            setResult(previous => ({ key, prefix, catalog: tables, tables: selected,
              // Editing this step's Include does not change its input catalog.
              // Keep browsing/suggestion controls mounted across match replies.
              lineage: previous?.prefix === prefix && previous.catalog === tables && previous.lineage ? previous.lineage : lineage,
              sourceTables: lineage.filter(item => keys.has(JSON.stringify([item.current.namespace, item.current.name]))).map(item => item.source) }));
          }
        }).catch(error => {
          if (!controller.signal.aborted) setResult(previous => ({ key, prefix, catalog: tables,
            ...(previous?.prefix === prefix && previous.catalog === tables && previous.lineage ? { lineage: previous.lineage } : {}),
            error: error instanceof Error ? error.message : String(error) }));
        });
    }, 150);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [key, tables, preview, enabled]);
  if (result?.prefix !== prefix || result.catalog !== tables) return undefined;
  return result.key === key ? result : { ...result, tables: undefined, sourceTables: undefined, error: undefined };
}

export function TransformTableScope({ id, index, matches: current, rule, disabled, onChange, onUseTable, catalogUnavailableReason }: {
  id: string; index: number; matches: ReturnType<typeof useTransformMatches>; rule: TableRule; disabled: boolean;
  onChange: (patch: Partial<TableRule>) => void;
  onUseTable: ((table: TableIdentity) => void) | undefined;
  catalogUnavailableReason: string;
}) {
  const catalog = useTableCatalog();
  const [matchedOpen, setMatchedOpen] = useState(false);
  const [excludeExpanded, setExcludeExpanded] = useState(true);
  const showMatches = matchedOpen || rule.include.length > 0;
  return <div class="middleware-table-scope">
    <AvailableTablesButton label={`Available tables for transform ${index + 1}`} title="Browse tables selected in the source"
      onUse={onUseTable} showUse />
    <TableRuleFields id={id} rule={rule} labelSuffix={`transform ${index + 1}`} disabled={disabled}
      excludeExpanded={excludeExpanded} onExcludeExpanded={setExcludeExpanded} onChange={onChange}
      includeHelp={SCOPE_HELP} excludeHelp={SCOPE_HELP} onUse={() => setMatchedOpen(false)} />
    {showMatches ? <MatchedTablesDisclosure id={`${id}-matched`} label="Matched tables" headerClass="table-rule-result"
      toggleLabel={`Matched tables for transform ${index + 1}`} regionLabel={`Matched tables for transform ${index + 1}`}
      tables={current?.tables} open={matchedOpen} onToggle={() => setMatchedOpen(!matchedOpen)}
      after={<span class="middleware-scope-status" role="status" title={current?.error}>
        {current?.error ? "Cannot match tables" : !catalog ? catalogUnavailableReason : ""}
      </span>} /> : <div class="table-rule-result"><span class="middleware-scope-status" role="status" title={current?.error}>
        {current?.error ? "Cannot match tables" : ""}</span></div>}
  </div>;
}
