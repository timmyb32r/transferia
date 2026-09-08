import { render } from "preact";
import catalog from "../../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { AboutProvider, CompatibilityMatrixLauncher } from "../../src/ui/CompatibilityMatrixDialog";
import type { UiCatalog } from "../../src/types";
import "../../src/style.css";

render(<AboutProvider catalog={catalog as unknown as UiCatalog}>
  <CompatibilityMatrixLauncher />
</AboutProvider>, document.getElementById("fixture")!);
