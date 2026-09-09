import { render } from "preact";
import { useLayoutEffect, useState } from "preact/hooks";
import { ApplicationServicesProvider } from "../../src/bootstrap/ApplicationServicesProvider";
import { httpControlPlane } from "../../src/infrastructure/controlPlane/httpControlPlane";
import { SourceMetadataContext, type SourceMetadata } from "../../src/delivery/sourceMetadata";
import { SourceDataViewer } from "../../src/delivery/SourceDataViewer";
import { DeliverySidebar } from "../../src/delivery/EditorChrome";
import "../../src/style.css";

const metadata = { metadata: { id: "cache", loaded: [] }, discovery: { state: "success",
  tables: [{ namespace: "public", name: "events" }, { namespace: "public", name: "users" }] } } as unknown as SourceMetadata;
function Fixture() {
  const [open, setOpen] = useState(false);
  const [{ available, pending }, setTools] = useState({ available: true, pending: false });
  useLayoutEffect(() => {
    const update = (event: Event) => setTools((event as CustomEvent<{ available: boolean; pending: boolean }>).detail);
    window.addEventListener("sidebar-fixture-state", update);
    return () => window.removeEventListener("sidebar-fixture-state", update);
  }, []);
  const mode = new URL(location.href).searchParams.get("mode") === "parsed" ? "parsed" : "tables";
  return <ApplicationServicesProvider services={{ controlPlane: httpControlPlane }}>
    <SourceMetadataContext.Provider value={metadata}>
      <div class="shell"><DeliverySidebar catalog={{ common_schema: {}, initial: {}, connectors: [] }} deliveries={[]}
        selectedId={undefined} appearance={{ design: "airy-v0", theme: "light" }} onAppearance={() => {}}
        dataWidgetAvailable={available} dataWidgetVisible={false} onToggleDataWidget={() => {}} onNew={() => {}} onOpen={() => {}}
        dataSchema={{ available, visible: false, pending, onOpen: () => {} }}
        dataViewer={{ available, visible: open, pending, onOpen: () => setOpen(true) }} />
        <main class="workspace">Source editor</main>
      </div>
      {open && <SourceDataViewer source={{ connector: mode === "parsed" ? "kafka" : "postgres", config: { database: "db", tables: { type: "all" } } }} mode={mode}
        onClose={() => setOpen(false)} />}
    </SourceMetadataContext.Provider>
  </ApplicationServicesProvider>;
}
render(<Fixture />, document.getElementById("fixture")!);
