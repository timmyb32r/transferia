import type { ComponentChildren } from "preact";
import { useEffect, useState } from "preact/hooks";
import type { TableIdentity, TableRule } from "../../generated/apiContract";
import { useTableCatalog } from "../../schema/tableCatalog";
import { MatchedTablesDisclosure } from "../tableSelection/MatchedTablesDisclosure";
import { Button } from "../../ui/Button";
import { AvailableTablesDialog } from "./AvailableTablesDialog";

export function TransformTableScope({ id, index, rule, children }: {
  id: string; index: number; rule: TableRule; children: ComponentChildren;
}) {
  const catalog = useTableCatalog();
  const [availableOpen, setAvailableOpen] = useState(false);
  const [matchedOpen, setMatchedOpen] = useState(false);
  const [result, setResult] = useState<{ key: string; catalog: TableIdentity[]; tables?: TableIdentity[]; error?: string }>();
  const key = JSON.stringify(rule);
  const tables = catalog?.tables, preview = catalog?.preview;
  useEffect(() => {
    if (!tables || !preview || !rule.include) return;
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
  }, [key, tables, preview]);
  const current = result?.key === key && result.catalog === tables ? result : undefined;
  return <div class="middleware-table-scope">
    <div class="middleware-available-tables">
      <Button class="table-matches-height-toggle" aria-label={`Available tables for transform ${index + 1}`} aria-haspopup="dialog" disabled={!catalog}
        title="Browse tables selected in the source" onClick={() => setAvailableOpen(true)}>
        Available tables <span class="table-match-count">({tables?.length ?? "—"})</span>
      </Button>
    </div>
    {availableOpen && catalog && <AvailableTablesDialog catalog={catalog} onClose={() => setAvailableOpen(false)} />}
    {children}
    <MatchedTablesDisclosure id={`${id}-matched`} label="Matched tables" headerClass="table-rule-result"
      toggleLabel={`Matched tables for transform ${index + 1}`} regionLabel={`Matched tables for transform ${index + 1}`}
      tables={current?.tables} open={matchedOpen} onToggle={() => setMatchedOpen(!matchedOpen)}
      after={<span class="middleware-scope-status" role="status" title={current?.error}>
        {current?.error ? "Invalid pattern" : !catalog ? "Check the source connection first" : ""}
      </span>} />
  </div>;
}
