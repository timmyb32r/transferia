import { render } from "preact";
import catalog from "../../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { AboutProvider, TypeMappingLink } from "../../src/ui/CompatibilityMatrixDialog";
import type { UiCatalog } from "../../src/types";
import "../../src/style.css";

render(<AboutProvider catalog={catalog as unknown as UiCatalog}>
  <TypeMappingLink role="source" connector="postgres" />
</AboutProvider>, document.getElementById("fixture")!);
