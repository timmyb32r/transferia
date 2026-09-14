import { render } from "preact";
import { useState } from "preact/hooks";
import { SchemaForm } from "../../src/schema/SchemaForm";
import type { CompiledNode } from "../../src/schema/compiler";
import { WidgetRegistryProvider } from "../../src/schema/widgetRegistry";
import { productionWidgetRegistry } from "../../src/features/formWidgetRegistry";
import type { JsonValue } from "../../src/types";
import "../../src/style.css";

document.documentElement.dataset.theme = new URLSearchParams(location.search).get("theme") ?? "light";
document.documentElement.dataset.design = "airy-v0";
const node: CompiledNode = {
  kind: "object", xUi: {}, required: new Set(), properties: Object.fromEntries(
    ["advanced", "performance"].flatMap(section => Array.from({ length: 12 }, (_, i) => [
      `${section}_${i}`, { kind: "string", title: `${section} setting ${i + 1}`, xUi: { section } },
    ])),
  ),
} as CompiledNode;
function Endpoint({ title }: { title: string }) {
  const [value, setValue] = useState<JsonValue>({});
  return <article class="card endpoint-card" data-endpoint={title}>
    <div class="island-form"><h2>{title}</h2><SchemaForm endpoint node={node} value={value} onChange={setValue} /></div>
  </article>;
}
render(<WidgetRegistryProvider registry={productionWidgetRegistry}>
  <div style={{ height: "110vh" }} />
  <main style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 350px), 1fr))", alignItems: "start", gap: "20px", padding: "16px" }}>
    <Endpoint title="Source" /><Endpoint title="Destination" />
  </main>
</WidgetRegistryProvider>, document.getElementById("fixture")!);
