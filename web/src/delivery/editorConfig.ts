import {
  branchMatches,
  compileSchema,
  deterministicValue,
  draftSeedError,
  firstCompletionIssue,
  isFieldComplete,
  materializeBranch,
  SchemaContractError,
  type CompletionIssue,
  type CompiledNode,
} from "../schema/compiler";
import type { WidgetContracts } from "../schema/widgetDefinitions";
import {
  configuredEndpointCapabilities,
  configuredSourceSupportsDeliveryType,
  routeSupportsDeliveryType,
  sourceRecordSemantics,
} from "../recordSemantics";
import type {
  EndpointDefinition,
  JsonObject,
  JsonValue,
  UiCatalog,
} from "../types";

const compiledSchemaCache = new WeakMap<
  WidgetContracts,
  WeakMap<object, CompiledNode>
>();

export function compiledSchema(
  schema: UiCatalog["common_schema"],
  widgets: WidgetContracts,
): CompiledNode {
  let schemas = compiledSchemaCache.get(widgets);
  if (schemas === undefined) {
    schemas = new WeakMap<object, CompiledNode>();
    compiledSchemaCache.set(widgets, schemas);
  }
  const cached = schemas.get(schema);
  if (cached !== undefined) return cached;
  const compiled = compileSchema(schema, widgets);
  schemas.set(schema, compiled);
  return compiled;
}

export function validateCatalogSchemas(
  catalog: UiCatalog,
  widgets: WidgetContracts,
): void {
  const common = compiledSchema(catalog.common_schema, widgets);
  if (common.kind !== "object")
    throw new SchemaContractError("common schema must be an object");
  const commonInitial = Object.fromEntries(
    Object.entries(catalog.initial).filter(
      ([name]) => common.properties[name] !== undefined,
    ),
  );
  validateInitial("common", common, commonInitial);
  validateInitialBranchOrder("common", common, commonInitial);
  validateHiddenInitialValues("common", common, commonInitial);
  validateEditorMaterialization("common", common);
  for (const connector of catalog.connectors) {
    for (const [role, endpoint] of [
      ["source", connector.source],
      ["sink", connector.sink],
    ] as const) {
      if (endpoint === undefined) continue;
      const node = compiledSchema(endpoint.schema, widgets);
      const owner = `${connector.key} ${role}`;
      validateInitial(owner, node, endpoint.initial);
      validateInitialBranchOrder(owner, node, endpoint.initial);
      validateHiddenInitialValues(owner, node, endpoint.initial);
      validateEditorMaterialization(owner, node);
      configuredEndpointCapabilities(
        endpoint,
        node,
        endpoint.initial,
        role === "source" ? "source" : "destination",
      );
    }
  }
}

function validateInitialBranchOrder(
  owner: string,
  node: CompiledNode,
  value: JsonValue | undefined,
  path = "#",
): void {
  if (value === undefined) return;
  if (node.kind === "union") {
    const selected = node.branches.findIndex((branch) =>
      branchMatches(branch, value),
    );
    if (selected < 0) return;
    if (selected !== 0)
      throw new SchemaContractError(
        `${owner} initial value selects ${path} branch ${selected + 1}; the default branch must be first`,
      );
    validateInitialBranchOrder(owner, node.branches[0]!.node, value, path);
    return;
  }
  if (node.kind === "object" && isObject(value)) {
    for (const [name, child] of Object.entries(node.properties))
      validateInitialBranchOrder(
        owner,
        child,
        value[name],
        `${path}/${escapePointer(name)}`,
      );
    return;
  }
  if (node.kind === "array" && Array.isArray(value)) {
    value.forEach((item, index) =>
      validateInitialBranchOrder(owner, node.item, item, `${path}/${index}`),
    );
    return;
  }
  if (node.kind === "nullable" && value !== null)
    validateInitialBranchOrder(owner, node.inner, value, path);
}

function validateHiddenInitialValues(
  owner: string,
  node: CompiledNode,
  value: JsonValue | undefined,
  required = true,
  path = "#",
): void {
  const hidden = node.hidden === true;
  if (hidden) {
    if (value === undefined && !required) return;
    const issue = firstCompletionIssue(node, value, required, path);
    if (issue !== undefined)
      throw new SchemaContractError(
        `${owner} initial hidden field ${issue.path} is incomplete`,
      );
    return;
  }
  if (node.kind === "object") {
    if (!isObject(value)) return;
    for (const [name, child] of Object.entries(node.properties))
      validateHiddenInitialValues(
        owner,
        child,
        value[name],
        node.required.has(name),
        `${path}/${escapePointer(name)}`,
      );
    return;
  }
  if (node.kind === "array") {
    if (!Array.isArray(value)) return;
    value.forEach((item, index) =>
      validateHiddenInitialValues(
        owner,
        node.item,
        item,
        true,
        `${path}/${index}`,
      ),
    );
    return;
  }
  if (node.kind === "nullable") {
    if (value !== undefined && value !== null)
      validateHiddenInitialValues(
        owner,
        node.inner,
        value,
        required,
        path,
      );
    return;
  }
  if (node.kind === "union" && value !== undefined) {
    const branch = node.branches.find((candidate) =>
      branchMatches(candidate, value),
    );
    if (branch !== undefined)
      validateHiddenInitialValues(
        owner,
        branch.node,
        value,
        required,
        path,
      );
  }
}

function validateEditorMaterialization(
  owner: string,
  node: CompiledNode,
  path = "#",
  required = true,
  hiddenAncestor = false,
): void {
  const hidden = hiddenAncestor || node.hidden === true;
  const authored = deterministicValue(node);
  if (hidden && (required || authored !== undefined)) {
    const materialized = deterministicHiddenValue(node);
    const issue =
      materialized === undefined
        ? undefined
        : firstCompletionIssue(node, materialized, required, path);
    if (materialized === undefined || issue !== undefined)
      throw new SchemaContractError(
        `${owner} hidden ${required ? "required " : ""}field ${issue?.path ?? path} cannot be materialized deterministically`,
      );
    return;
  }
  if (hidden) return;
  if (node.kind === "object") {
    for (const [name, child] of Object.entries(node.properties)) {
      const childPath = `${path}/${escapePointer(name)}`;
      validateEditorMaterialization(
        owner,
        child,
        childPath,
        node.required.has(name),
        hidden,
      );
    }
    return;
  }
  if (node.kind === "array") {
    validateEditorMaterialization(
      owner,
      node.item,
      `${path}/items`,
      true,
      hidden,
    );
    return;
  }
  if (node.kind === "nullable") {
    validateEditorMaterialization(
      owner,
      node.inner,
      `${path}/nullable`,
      true,
      hidden,
    );
    return;
  }
  if (node.kind === "union") {
    node.branches.forEach((branch, index) => {
      const materialized = materializeBranch(branch);
      const matches = node.branches.filter((candidate) =>
        branchMatches(candidate, materialized),
      );
      if (matches.length !== 1 || matches[0] !== branch)
        throw new SchemaContractError(
          `${owner} union branch ${path}/branch-${index} does not materialize to one unique variant`,
        );
      validateEditorMaterialization(
        owner,
        branch.node,
        `${path}/branch-${index}`,
        true,
        hidden,
      );
    });
  }
}

/**
 * Materializes a hidden node only when its value is authored by the schema or
 * logically unique. Generic UI placeholders such as false, null, or an empty
 * string are deliberately not accepted as configuration defaults.
 */
function deterministicHiddenValue(node: CompiledNode): JsonValue | undefined {
  const direct = deterministicValue(node);
  if (direct !== undefined) return direct;
  if (node.kind === "object") {
    const value: JsonObject = {};
    for (const [name, child] of Object.entries(node.properties)) {
      if (node.required.has(name)) {
        const childValue = deterministicHiddenValue(child);
        if (childValue === undefined) return undefined;
        value[name] = childValue;
      } else if (child.defaultValue !== undefined) {
        value[name] = structuredClone(child.defaultValue);
      }
    }
    return isFieldComplete(node, value, true) ? value : undefined;
  }
  if (node.kind === "union" && node.branches.length === 1) {
    const branch = node.branches[0]!;
    const created =
      branch.constant === undefined
        ? deterministicHiddenValue(branch.node)
        : structuredClone(branch.constant);
    if (created === undefined) return undefined;
    const value =
      branch.discriminator !== undefined && isObject(created)
        ? {
            ...created,
            [branch.discriminator.key]: structuredClone(
              branch.discriminator.value,
            ),
          }
        : created;
    return isFieldComplete(node, value, true) ? value : undefined;
  }
  return undefined;
}

function escapePointer(value: string): string {
  return value.replaceAll("~", "~0").replaceAll("/", "~1");
}

function validateInitial(
  owner: string,
  node: CompiledNode,
  initial: JsonValue,
): void {
  const error = draftSeedError(node, initial);
  if (error !== undefined)
    throw new SchemaContractError(`${owner} initial value is invalid: ${error}`);
}

export function freshConfig(catalog: UiCatalog): JsonObject {
  const id = crypto.randomUUID();
  return {
    ...structuredClone(catalog.initial),
    durable_storage: {
      type: "local_file",
      path: `.transferia-server/workers/${id}/state`,
    },
    delivery_type: null,
    source: {},
    sink: {},
  };
}

export function clonedConfig(catalog: UiCatalog, current: JsonObject): JsonObject {
  const config = structuredClone(current);
  delete config.delivery_id;
  delete config.delivery_name;
  config.durable_storage = freshConfig(catalog).durable_storage!;
  return config;
}

export function clonedDeliveryName(current: string): string {
  const suffix = current.match(/^(.*?)(\d+)$/u);
  return suffix === null
    ? `${current}2`
    : `${suffix[1]}${BigInt(suffix[2]!) + 1n}`;
}

export function selectedEndpoints(
  catalog: UiCatalog,
  config: JsonObject,
  widgets: WidgetContracts,
): {
  sourceKey: string;
  sinkKey: string;
  source: EndpointDefinition | undefined;
  sink: EndpointDefinition | undefined;
  error?: string;
} {
  const sourceKey = singleKey(config.source);
  const sinkKey = singleKey(config.sink);
  const source = catalog.connectors.find(
    (connector) => connector.key === sourceKey,
  )?.source;
  const sink = catalog.connectors.find(
    (connector) => connector.key === sinkKey,
  )?.sink;
  const deliveryType = stringValue(config.delivery_type);
  let error: string | undefined;
  try {
    if (deliveryType !== "" && source !== undefined) {
      const sourceValue = endpointValue(config, "source", sourceKey);
      const sourceSchema = compiledSchema(source.schema, widgets);
      const mode = deliveryType as (typeof source.delivery_modes)[number];
      if (
        !configuredSourceSupportsDeliveryType(
          source,
          sourceSchema,
          sourceValue,
          mode,
        )
      ) {
        const title =
          catalog.connectors.find((connector) => connector.key === sourceKey)
            ?.title ?? sourceKey;
        error = `${title} does not support ${deliveryType.replaceAll("_", " ")} delivery.`;
      } else if (sink !== undefined) {
        const sourceCapabilities = configuredEndpointCapabilities(
          source,
          sourceSchema,
          sourceValue,
          "source",
        );
        const sinkCapabilities = configuredEndpointCapabilities(
          sink,
          compiledSchema(sink.schema, widgets),
          endpointValue(config, "sink", sinkKey),
          "destination",
        );
        if (
          !routeSupportsDeliveryType(
            sourceCapabilities,
            sinkCapabilities,
            mode,
            (phase) =>
              sourceRecordSemantics(
                source,
                sourceSchema,
                sourceValue,
                phase,
              ),
          )
        ) {
          const sourceTitle =
            catalog.connectors.find(
              (connector) => connector.key === sourceKey,
            )?.title ?? sourceKey;
          const sinkTitle =
            catalog.connectors.find(
              (connector) => connector.key === sinkKey,
            )?.title ?? sinkKey;
          error = `${sinkTitle} cannot accept the records produced by ${sourceTitle} for ${deliveryType.replaceAll("_", " ")} delivery.`;
        }
      }
    }
  } catch (caught) {
    error =
      caught instanceof SchemaContractError
        ? caught.message
        : "The selected endpoint capabilities are invalid.";
  }
  return {
    sourceKey,
    sinkKey,
    source,
    sink,
    ...(error === undefined ? {} : { error }),
  };
}

export function configurationReadiness(
  catalog: UiCatalog,
  config: JsonObject,
  widgets: WidgetContracts,
): {
  selection: ReturnType<typeof selectedEndpoints>;
  commonIssue: CompletionIssue | undefined;
  sourceIssue: CompletionIssue | undefined;
  sinkIssue: CompletionIssue | undefined;
  commonComplete: boolean;
  sourceComplete: boolean;
  sourceSchemaIssue: CompletionIssue | undefined;
  sourceSchemaReady: boolean;
  sinkComplete: boolean;
  sourceReady: boolean;
  complete: boolean;
} {
  const selection = selectedEndpoints(catalog, config, widgets);
  const commonIssue = firstCompletionIssue(
    compiledSchema(catalog.common_schema, widgets),
    config,
  );
  const sourceIssue =
    selection.source === undefined
      ? undefined
      : prefixIssue(
          firstCompletionIssue(
            compiledSchema(selection.source.schema, widgets),
            endpointValue(config, "source", selection.sourceKey),
          ),
          `#/source/${escapePointer(selection.sourceKey)}`,
        );
  const sinkIssue =
    selection.sink === undefined
      ? undefined
      : prefixIssue(
          firstCompletionIssue(
            compiledSchema(selection.sink.schema, widgets),
            endpointValue(config, "sink", selection.sinkKey),
          ),
          `#/sink/${escapePointer(selection.sinkKey)}`,
        );
  const sourceNode =
    selection.source === undefined
      ? undefined
      : compiledSchema(selection.source.schema, widgets);
  const sourceValue = endpointValue(config, "source", selection.sourceKey);
  const parserNode =
    sourceNode?.kind === "object" ? sourceNode.properties.parser : undefined;
  const sourceSchemaIssue =
    sourceNode === undefined
      ? undefined
      : prefixIssue(
          firstCompletionIssue(
            parserNode ?? sourceNode,
            parserNode === undefined || !isObject(sourceValue)
              ? sourceValue
              : sourceValue.parser,
          ),
          parserNode === undefined
            ? `#/source/${escapePointer(selection.sourceKey)}`
            : `#/source/${escapePointer(selection.sourceKey)}/parser`,
        );
  const commonComplete =
    selection.error === undefined && commonIssue === undefined;
  const sourceComplete =
    selection.source !== undefined && sourceIssue === undefined;
  const sourceSchemaReady =
    selection.error === undefined &&
    selection.source !== undefined &&
    sourceSchemaIssue === undefined;
  const sinkComplete =
    selection.sink !== undefined && sinkIssue === undefined;
  const sourceReady = commonComplete && sourceComplete;
  return {
    selection,
    commonIssue,
    sourceIssue,
    sourceSchemaIssue,
    sinkIssue,
    commonComplete,
    sourceComplete,
    sourceSchemaReady,
    sinkComplete,
    sourceReady,
    complete: sourceReady && sinkComplete,
  };
}

function prefixIssue(
  issue: CompletionIssue | undefined,
  prefix: string,
): CompletionIssue | undefined {
  if (issue === undefined) return undefined;
  return {
    ...issue,
    path: issue.path === "#" ? prefix : `${prefix}${issue.path.slice(1)}`,
  };
}

export function completionIssueLabel(
  root: CompiledNode,
  value: JsonValue | undefined,
  issue: CompletionIssue,
  prefix: string,
): string {
  const relative = issue.path.startsWith(prefix)
    ? issue.path.slice(prefix.length)
    : issue.path.slice(1);
  const segments = relative
    .split("/")
    .filter(Boolean)
    .map(unescapePointer);
  let node = root;
  let current = value;
  let lastSegment = segments.at(-1) ?? "configuration";
  let index = 0;
  while (index < segments.length) {
    if (node.kind === "nullable") {
      node = node.inner;
      continue;
    }
    if (node.kind === "union") {
      const candidateValue = current;
      const branch =
        candidateValue === undefined
          ? undefined
          : node.branches.find((candidate) =>
              branchMatches(candidate, candidateValue),
            );
      if (branch === undefined) break;
      node = branch.node;
      continue;
    }
    const segment = segments[index]!;
    if (node.kind === "object") {
      const child = node.properties[segment];
      if (child === undefined) break;
      node = child;
      current = isObject(current) ? current[segment] : undefined;
      lastSegment = segment;
      index += 1;
      continue;
    }
    if (node.kind === "array") {
      const itemIndex = Number(segment);
      if (!Number.isSafeInteger(itemIndex) || itemIndex < 0) break;
      node = node.item;
      current = Array.isArray(current) ? current[itemIndex] : undefined;
      index += 1;
      continue;
    }
    break;
  }
  return node.title?.trim() || humanizeFieldName(lastSegment);
}

function unescapePointer(value: string): string {
  return value.replaceAll("~1", "/").replaceAll("~0", "~");
}

function humanizeFieldName(value: string): string {
  const words = value.replaceAll("_", " ").trim();
  return words === "" ? "Configuration" : words[0]!.toUpperCase() + words.slice(1);
}

export function endpointValue(
  config: JsonObject,
  role: "source" | "sink",
  key: string,
): JsonValue {
  const container = config[role];
  return isObject(container) ? (container[key] ?? {}) : {};
}

export function isObject(value: JsonValue | undefined): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}

function singleKey(value: JsonValue | undefined): string {
  return isObject(value) ? (Object.keys(value)[0] ?? "") : "";
}

export function stringValue(value: JsonValue | undefined): string {
  return typeof value === "string" ? value : "";
}
