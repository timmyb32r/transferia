import type { WidgetPlugin } from "../../schema/widgetPlugin";
import { TableSelectionEditor } from "./TableSelectionEditor";

export const tableSelectionWidgets: readonly WidgetPlugin[] = [{
  name: "table_selection", kinds: ["union"], renderer: "both", wide: true,
  node: context => <TableSelectionEditor value={context.value} disabled={context.disabled}
    fixed={context.node.xUi.table_membership === "fixed"} onChange={context.onChange} />,
  property: context => <div data-field-name={context.name}>
    <TableSelectionEditor value={context.effectiveValue} disabled={context.disabled}
      fixed={context.node.xUi.table_membership === "fixed"} onChange={context.onChange} />
  </div>,
}];
