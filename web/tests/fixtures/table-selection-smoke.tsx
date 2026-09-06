import { render } from "preact";
import { useState } from "preact/hooks";
import { TableSelectionEditor } from "../../src/features/tableSelection/TableSelectionEditor";
import { TableCatalogContext } from "../../src/schema/tableCatalog";
import { Button } from "../../src/ui/Button";
import type { JsonValue } from "../../src/json";
import "../../src/style.css";

// Visual fixture only. Matcher correctness is covered by the Rust evaluator
// suite; this fixture supplies a deliberately large authenticated catalog.
const tables = Array.from({ length: 40 }, (_, index) => ({ namespace: "analytics", name: `reports_${index}` }));
const catalog = { tables, preview: async () => ({ cards: [{ selected: tables, excluded: [] }], issues: [] }) };
function Fixture() {
  const [verified, setVerified] = useState(false);
  const [value, setValue] = useState<JsonValue>({ type: "selected", rules: [{ include: "analytics.reports_*", include_mode: "glob" }] });
  return <main style={{ padding: "24px", maxWidth: "980px", margin: "auto" }}>
    <h1>Table selection · visual smoke fixture</h1>
    <Button onClick={() => setVerified(!verified)}>{verified ? "Invalidate connection" : "Provide verified catalog"}</Button>
    <TableCatalogContext.Provider value={verified ? catalog : undefined}>
      <TableSelectionEditor value={value} onChange={setValue} />
    </TableCatalogContext.Provider>
  </main>;
}
render(<Fixture />, document.getElementById("fixture")!);
