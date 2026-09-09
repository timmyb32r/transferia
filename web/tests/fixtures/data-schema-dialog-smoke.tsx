import { render } from "preact";
import { useLayoutEffect, useState } from "preact/hooks";
import { DataSchemaDialog } from "../../src/delivery/DataSchemaDialog";
import { DeliverySidebar, EditorTabs } from "../../src/delivery/EditorChrome";
import type { DiscoveryResult } from "../../src/types";
import "../../src/style.css";

const result: DiscoveryResult = { source: "postgres", sink: "clickhouse", pipeline_count: 1,
  performance_advice: [], sink_limits: { sink: "clickhouse", supported_arrow_types: [] },
  datasets: ["events", "users"].map((name, index) => {
    const columns = Array.from({ length: index ? 80 : 1 }, (_, i) => ({ name: `column_${i}`,
      arrow_type: "Utf8", nullable: false, primary_key: false, low_cardinality: false }));
    return { name, role: "Main", intermediate_columns: columns,
      final_columns: columns.map(column => ({ ...column, destination_type: "String" })) };
  }) };

function Fixture() {
  const [open, setOpen] = useState(false);
  const [state, setState] = useState("ready");
  useLayoutEffect(() => {
    const update = (event: Event) => setState((event as CustomEvent<string>).detail);
    window.addEventListener("schema-fixture-state", update);
    return () => window.removeEventListener("schema-fixture-state", update);
  }, []);
  return <div class="shell"><DeliverySidebar catalog={{ common_schema: {}, initial: {}, connectors: [] }}
    deliveries={[]} selectedId={undefined} appearance={{ design: "airy-v0", theme: "light" }}
    onAppearance={() => {}} onNew={() => {}} onOpen={() => {}}
    dataWidgetAvailable dataWidgetVisible={false} onToggleDataWidget={() => {}}
    dataViewer={{ available: true, visible: false, pending: false, onOpen: () => {} }}
    dataSchema={{ available: true, visible: open, pending: false, onOpen: () => setOpen(true) }} />
    <main class="workspace"><EditorTabs active="ui" disabled={false} onUi={() => {}}
      onYaml={() => {}} onPerformanceAdvice={() => {}} />
      <button>Editor target</button>
    </main>
    {open && <DataSchemaDialog result={state === "ready" ? result : undefined}
      error={state === "error" ? "Schema discovery failed. ".repeat(100) : undefined} onClose={() => setOpen(false)} />}
  </div>;
}
render(<Fixture />, document.getElementById("fixture")!);
