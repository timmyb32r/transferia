import { createPortal } from "preact/compat";
import { useEffect, useMemo, useRef, useState } from "preact/hooks";

import type {
  ConnectorDefinition,
  DeliveryMode,
  RecordSemantics,
  UiCatalog,
} from "../generated/apiContract";
import { Button } from "./Button";

const DELIVERY_MODE_LABEL: Record<DeliveryMode, string> = {
  batch: "Batch",
  stream: "Stream",
};

type CapabilityKind = "source" | "destination" | "parser" | "serializer" | "transformer";

interface CapabilityGroup {
  key: string;
  label: string;
  members: Map<CapabilityKind, Set<string>>;
  nonMembers: Map<CapabilityKind, Set<string>>;
}

const PROPERTY_LABELS: Record<string, string> = {
  "delivery_mode.batch": "Batch delivery",
  "delivery_mode.stream": "Stream delivery",
  "record_semantics.append_only": "Append-only records",
  "record_semantics.changelog": "Changelog records",
  "record_semantics.only_append_only": "Only append-only records",
  "record_semantics.only_changelog": "Only changelog records",
  "component.source": "All sources",
  "component.destination": "All destinations",
  "component.parser": "All parsers",
  "component.serializer": "All serializers",
  "component.transformer": "All transformers",
  partitioned: "Partitioned execution",
  connection_check: "Connection check",
  message_preview: "Message preview",
  playground: "Interactive playground",
};

const KIND_LABELS: Record<CapabilityKind, string> = {
  source: "Sources",
  destination: "Destinations",
  parser: "Parsers",
  serializer: "Serializers",
  transformer: "Transformers",
};

export function catalogCapabilityGroups(catalog: UiCatalog): CapabilityGroup[] {
  const groups = new Map<string, CapabilityGroup>();
  const add = (property: string, kind: CapabilityKind, title: string) => {
    const group = groups.get(property) ?? {
      key: property,
      label: PROPERTY_LABELS[property] ?? property.replaceAll("_", " "),
      members: new Map(),
      nonMembers: new Map(),
    };
    const members = group.members.get(kind) ?? new Set<string>();
    members.add(title);
    group.members.set(kind, members);
    groups.set(property, group);
  };
  for (const connector of catalog.connectors) {
    for (const [kind, endpoint] of [
      ["source", connector.source],
      ["destination", connector.sink],
    ] as const) {
      if (!endpoint) continue;
      add(`component.${kind}`, kind, connector.title);
      endpoint.delivery_modes.forEach((mode) => add(`delivery_mode.${mode}`, kind, connector.title));
      endpoint.record_semantics.forEach((semantics) => add(`record_semantics.${semantics}`, kind, connector.title));
      if (endpoint.record_semantics.length === 1) {
        add(`record_semantics.only_${endpoint.record_semantics[0]}`, kind, connector.title);
      }
      if (endpoint.partitioned) add("partitioned", kind, connector.title);
      if (endpoint.connection_check) add("connection_check", kind, connector.title);
      if (endpoint.message_preview) add("message_preview", kind, connector.title);
      collectSchemaCapabilities(endpoint.schema, add);
    }
  }
  collectSchemaCapabilities(catalog.common_schema, add);
  const universes = new Map<CapabilityKind, Set<string>>();
  for (const group of groups.values()) {
    if (!group.key.startsWith("component.")) continue;
    for (const [kind, members] of group.members) {
      universes.set(kind, new Set(members));
    }
  }
  for (const group of groups.values()) {
    for (const kind of applicableKinds(group.key)) {
      const members = group.members.get(kind) ?? new Set<string>();
      const nonMembers = new Set(
        [...(universes.get(kind) ?? [])].filter((title) => !members.has(title)),
      );
      if (nonMembers.size > 0) group.nonMembers.set(kind, nonMembers);
    }
  }
  return [...groups.values()].sort((left, right) => left.label.localeCompare(right.label));
}

function applicableKinds(property: string): CapabilityKind[] {
  if (property.startsWith("component.")) {
    return [property.slice("component.".length) as CapabilityKind];
  }
  if (property.startsWith("record_semantics.")) {
    return ["source", "destination", "parser", "serializer"];
  }
  if (property === "playground") return ["transformer"];
  return ["source", "destination"];
}

function collectSchemaCapabilities(
  value: unknown,
  add: (property: string, kind: CapabilityKind, title: string) => void,
): void {
  if (Array.isArray(value)) {
    value.forEach((item) => collectSchemaCapabilities(item, add));
    return;
  }
  if (value === null || typeof value !== "object") return;
  const object = value as Record<string, unknown>;
  const ui = object["x-ui"];
  const capabilities =
    ui && typeof ui === "object"
      ? (ui as Record<string, unknown>).capabilities
      : undefined;
  if (capabilities && typeof capabilities === "object") {
    const descriptor = capabilities as Record<string, unknown>;
    const kind = descriptor.component;
    const title = typeof object.title === "string" ? object.title : descriptor.key;
    if (
      typeof title === "string" &&
      (kind === "parser" || kind === "serializer" || kind === "transformer")
    ) {
      const properties = Array.isArray(descriptor.properties) ? descriptor.properties : [];
      const declaredSemantics = Array.isArray(descriptor.record_semantics)
        ? descriptor.record_semantics
        : [];
      const semantics = declaredSemantics.map((item) => `record_semantics.${item}`);
      const exclusive = declaredSemantics.length === 1
        ? [`record_semantics.only_${declaredSemantics[0]}`]
        : [];
      [`component.${kind}`, ...properties, ...semantics, ...exclusive].forEach((property) => {
        if (typeof property === "string") add(property, kind, title);
      });
    }
  }
  Object.values(object).forEach((child) => collectSchemaCapabilities(child, add));
}

export interface CompatibilityRoute {
  source: ConnectorDefinition;
  sink: ConnectorDefinition;
  supported: DeliveryMode[];
  unsupported: DeliveryMode[];
  partial: DeliveryMode[];
}

function semanticsForMode(
  modes: DeliveryMode[],
  semantics: RecordSemantics[],
  mode: DeliveryMode,
): RecordSemantics[] {
  if (mode === "batch") return ["append_only"];
  if (!modes.includes("batch")) return semantics;

  const streamSemantics = semantics.filter(
    (candidate) => candidate !== "append_only",
  );
  return streamSemantics.length > 0 ? streamSemantics : ["append_only"];
}

export function compatibilityRoutes(catalog: UiCatalog): CompatibilityRoute[] {
  const sources = catalog.connectors.filter(
    (connector) => connector.source !== undefined,
  );
  const sinks = catalog.connectors.filter(
    (connector) => connector.sink !== undefined,
  );
  return sources.flatMap((source) =>
    sinks.map((sink) => {
      const modes = source.source!.delivery_modes;
      const accepted = new Set(sink.sink!.record_semantics);
      const supported: DeliveryMode[] = [];
      const unsupported: DeliveryMode[] = [];
      const partial: DeliveryMode[] = [];
      for (const mode of modes) {
        const produced = semanticsForMode(
          modes,
          source.source!.record_semantics,
          mode,
        );
        const acceptedCount = produced.filter((semantics) =>
          accepted.has(semantics),
        ).length;
        if (acceptedCount === 0) unsupported.push(mode);
        else {
          supported.push(mode);
          if (acceptedCount < produced.length) partial.push(mode);
        }
      }
      return {
        source,
        sink,
        supported,
        unsupported,
        partial,
      };
    }),
  );
}

export function CompatibilityMatrixDialog({
  catalog,
  onClose,
}: {
  catalog: UiCatalog;
  onClose: () => void;
}) {
  const dialog = useRef<HTMLElement>(null);
  const restoreFocus = useRef<HTMLElement | null>(null);
  const [activeCell, setActiveCell] = useState<{
    source: string;
    sink: string;
  } | null>(null);
  const [activeTab, setActiveTab] = useState<"matrix" | "properties">("matrix");
  const [activeProperty, setActiveProperty] = useState<string | null>(null);
  const sources = useMemo(
    () => catalog.connectors.filter((connector) => connector.source),
    [catalog],
  );
  const sinks = useMemo(
    () => catalog.connectors.filter((connector) => connector.sink),
    [catalog],
  );
  const routes = useMemo(() => compatibilityRoutes(catalog), [catalog]);
  const capabilityGroups = useMemo(() => catalogCapabilityGroups(catalog), [catalog]);
  const selectedCapabilityGroup =
    capabilityGroups.find((group) => group.key === activeProperty) ??
    capabilityGroups[0];
  const route = (source: string, sink: string) =>
    routes.find(
      (candidate) =>
        candidate.source.key === source && candidate.sink.key === sink,
    )!;

  useEffect(() => {
    restoreFocus.current =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    dialog.current
      ?.querySelector<HTMLButtonElement>(
        "[aria-label='Close compatibility matrix']",
      )
      ?.focus();
    const keydown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    document.addEventListener("keydown", keydown);
    return () => {
      document.removeEventListener("keydown", keydown);
      restoreFocus.current?.focus();
    };
  }, [onClose]);

  return createPortal(
    <div class="compatibility-backdrop" onMouseDown={onClose}>
      <section
        ref={dialog}
        class="compatibility-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="compatibility-title"
        aria-describedby="compatibility-description"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header>
          <div>
            <small>LIVE CONNECTOR CATALOG</small>
            <h2 id="compatibility-title">Transfer compatibility</h2>
            <p id="compatibility-description">
              Batch and Stream are delivery modes. Compatibility also accounts
              for append-only and change-event semantics.
            </p>
          </div>
          <Button
            shape="icon"
            aria-label="Close compatibility matrix"
            onClick={onClose}
          >
            ×
          </Button>
        </header>

        <div class="compatibility-tabs" role="tablist" aria-label="Catalog view">
          <Button role="tab" aria-selected={activeTab === "matrix"} onClick={() => setActiveTab("matrix")}>Matrix</Button>
          <Button role="tab" aria-selected={activeTab === "properties"} onClick={() => setActiveTab("properties")}>Properties</Button>
        </div>

        {activeTab === "matrix" ? <div class="compatibility-legend" aria-label="Legend">
          <span class="compatibility-badge batch">Batch</span>
          <span>Batch flow is supported</span>
          <span class="compatibility-badge stream">Stream</span>
          <span>Stream flow is supported</span>
          <span class="compatibility-unavailable" aria-hidden="true">
            —
          </span>
          <span>No compatible data semantics</span>
        </div> : <div class="capability-summary">Components grouped by capabilities declared in the live catalog.</div>}

        {activeTab === "matrix" ? <div class="compatibility-table-scroll">
          <table
            class="compatibility-table"
            onMouseLeave={() => setActiveCell(null)}
          >
            <caption>Sources by destinations</caption>
            <thead>
              <tr>
                <th scope="col">Source ↓ / Destination →</th>
                {sinks.map((sink) => (
                  <th
                    scope="col"
                    key={sink.key}
                    title={sink.title}
                    class={
                      activeCell?.sink === sink.key ? "active-column" : undefined
                    }
                  >
                    {sink.title}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sources.map((source) => (
                <tr
                  key={source.key}
                  class={
                    activeCell?.source === source.key ? "active-row" : undefined
                  }
                >
                  <th scope="row">
                    <strong>{source.title}</strong>
                    <small>{source.source!.delivery_modes.join(" · ")}</small>
                  </th>
                  {sinks.map((sink) => (
                    <CompatibilityCell
                      key={sink.key}
                      route={route(source.key, sink.key)}
                      activeColumn={activeCell?.sink === sink.key}
                      activeIntersection={
                        activeCell?.source === source.key &&
                        activeCell.sink === sink.key
                      }
                      onActivate={() =>
                        setActiveCell({ source: source.key, sink: sink.key })
                      }
                    />
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div> : <div class="capability-groups">
          <nav aria-label="Properties">
            {capabilityGroups.map((group) => (
              <Button
                key={group.key}
                aria-pressed={selectedCapabilityGroup?.key === group.key}
                onClick={() => setActiveProperty(group.key)}
              >
                {group.label}
              </Button>
            ))}
          </nav>
          {selectedCapabilityGroup && (
            <section class="capability-group">
              <h3>{selectedCapabilityGroup.label}</h3>
              <div class="capability-membership-columns">
                <CapabilityMembership
                  title="Has property"
                  groups={selectedCapabilityGroup.members}
                />
                <CapabilityMembership
                  title="Does not have property"
                  groups={selectedCapabilityGroup.nonMembers}
                />
              </div>
            </section>
          )}
        </div>}

        <footer>
          Some connectors require a matching mode, such as PostgreSQL
          replication or YTsaurus dynamic tables. Configuration validation is
          authoritative.
        </footer>
      </section>
    </div>,
    document.body,
  );
}

function CapabilityMembership({
  title,
  groups,
}: {
  title: string;
  groups: Map<CapabilityKind, Set<string>>;
}) {
  const populated = [...groups.entries()].filter(([, members]) => members.size > 0);
  return (
    <section class="capability-membership">
      <h4>{title}</h4>
      {populated.length === 0 ? (
        <p>None</p>
      ) : populated.map(([kind, members]) => (
        <section key={kind}>
          <h5>{KIND_LABELS[kind]}</h5>
          <ul>{[...members].sort().map((member) => <li key={member}>{member}</li>)}</ul>
        </section>
      ))}
    </section>
  );
}

function CompatibilityCell({
  route,
  activeColumn,
  activeIntersection,
  onActivate,
}: {
  route: CompatibilityRoute;
  activeColumn: boolean;
  activeIntersection: boolean;
  onActivate: () => void;
}) {
  const sourceName = route.source.title;
  const sinkName = route.sink.title;
  const supportedLabels = route.supported.map(
    (mode) => DELIVERY_MODE_LABEL[mode],
  );
  const unsupportedLabels = route.unsupported.map(
    (mode) => DELIVERY_MODE_LABEL[mode],
  );
  const description =
    route.supported.length === 0
      ? `${sourceName} to ${sinkName}: not supported`
      : `${sourceName} to ${sinkName}: ${supportedLabels.join(" and ")} supported${
          unsupportedLabels.length === 0
            ? ""
            : `; ${unsupportedLabels.join(" and ")} not supported`
        }`;
  return (
    <td
      aria-label={description}
      title={description}
      class={[
        route.unsupported.length > 0 || route.partial.length > 0
          ? "partial"
          : "",
        activeColumn ? "active-column" : "",
        activeIntersection ? "active-intersection" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      onMouseEnter={onActivate}
    >
      {route.supported.length === 0 ? (
        <span class="compatibility-unavailable" aria-hidden="true">
          —
        </span>
      ) : (
        <span class="compatibility-badges" aria-hidden="true">
          {route.supported.map((mode) => (
            <span
              key={mode}
              class={`compatibility-badge ${mode}`}
            >
              {DELIVERY_MODE_LABEL[mode]}
            </span>
          ))}
        </span>
      )}
    </td>
  );
}
