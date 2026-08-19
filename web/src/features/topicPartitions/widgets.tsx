import type { WidgetPlugin } from "../../schema/widgetPlugin";
import { PartitionRangesInput } from "./PartitionRangesInput";

export const topicPartitionWidgets: readonly WidgetPlugin[] = [
  {
    name: "partition_ranges",
    kinds: ["array"],
    renderer: "node",
    node: (context) =>
      context.node.kind === "array" ? (
        <PartitionRangesInput
          id={context.controlId}
          value={context.value}
          disabled={context.disabled}
          onChange={context.onChange}
        />
      ) : null,
  },
];
