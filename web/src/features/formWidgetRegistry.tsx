import { createWidgetRegistry } from "../schema/widgetPlugin";
import { coreFormWidgets } from "./coreFormWidgets";
import { jsonParserWidgets } from "./jsonParser/widgets";
import { middlewareWidgets } from "./middleware/widgets";
import { topicPartitionWidgets } from "./topicPartitions/widgets";
import { tableSelectionWidgets } from "./tableSelection/widgets";

export const productionWidgetRegistry = createWidgetRegistry([
  ...coreFormWidgets,
  ...jsonParserWidgets,
  ...middlewareWidgets,
  ...topicPartitionWidgets,
  ...tableSelectionWidgets,
]);
