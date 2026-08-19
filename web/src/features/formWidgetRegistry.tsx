import { createWidgetRegistry } from "../schema/widgetPlugin";
import { coreFormWidgets } from "./coreFormWidgets";
import { jsonParserWidgets } from "./jsonParser/widgets";
import { middlewareWidgets } from "./middleware/widgets";
import { topicPartitionWidgets } from "./topicPartitions/widgets";

export const productionWidgetRegistry = createWidgetRegistry([
  ...coreFormWidgets,
  ...jsonParserWidgets,
  ...middlewareWidgets,
  ...topicPartitionWidgets,
]);
