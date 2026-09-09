import { render } from "preact";
import { useState } from "preact/hooks";
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
  const available = new URL(location.href).searchParams.get("available") !== "false";
  const mode = new URL(location.href).searchParams.get("mode") === "parsed" ? "parsed" : "tables";
  return <ApplicationServicesProvider services={{ controlPlane: httpControlPlane }}>
    <SourceMetadataContext.Provider value={metadata}>
      <DeliverySidebar catalog={{ common_schema: {}, initial: {}, connectors: [] }} deliveries={[]}
        selectedId={undefined} appearance={{ design: "airy-v0", theme: "light" }} onAppearance={() => {}}
        dataWidgetAvailable={available} dataWidgetVisible={false} onToggleDataWidget={() => {}} onNew={() => {}} onOpen={() => {}}
        dataViewer={{ available, visible: open, pending: false, onOpen: () => setOpen(true) }} />
      {open && <SourceDataViewer source={{ connector: mode === "parsed" ? "kafka" : "postgres", config: { database: "db", tables: { type: "all" } } }} mode={mode}
        onClose={() => setOpen(false)} />}
    </SourceMetadataContext.Provider>
  </ApplicationServicesProvider>;
}
render(<Fixture />, document.getElementById("fixture")!);
