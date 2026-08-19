import type { WidgetPlugin } from "../../schema/widgetPlugin";
import { MiddlewareEditor } from "./MiddlewareEditor";

export const middlewareWidgets: readonly WidgetPlugin[] = [
  {
    name: "middlewares",
    kinds: ["array"],
    renderer: "node",
    node: (context) => (
      <MiddlewareEditor
        value={context.value}
        disabled={context.disabled}
        onChange={context.onChange}
      />
    ),
  },
];
