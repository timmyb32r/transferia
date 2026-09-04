import { createPortal } from "preact/compat";
import {
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "preact/hooks";

import type {
  ConnectorDefinition,
  DeliveryMode,
  UiCatalog,
} from "../generated/apiContract";
import { AutofillResistantInput } from "./AutofillResistantField";
import { Button } from "./Button";
import { InstantTooltip } from "./InstantTooltip";
import {
  declaredSourceRecordSemantics,
  routeSupportsDeliveryType,
} from "../recordSemantics";

const DELIVERY_MODE_LABEL: Record<DeliveryMode, string> = {
  batch: "Batch",
  stream: "Stream",
  batch_and_stream: "Batch + stream",
};

const DELIVERY_MODE_BADGE: Record<DeliveryMode, string> = {
  batch: "B",
  stream: "S",
  batch_and_stream: "B+S",
};

function DeliveryModeBadge({ mode }: { mode: DeliveryMode }) {
  return (
    <InstantTooltip content={DELIVERY_MODE_LABEL[mode]} class="compatibility-badge-tooltip">
      <span class={`compatibility-badge ${mode}`} aria-hidden="true">
        {DELIVERY_MODE_BADGE[mode]}
      </span>
    </InstantTooltip>
  );
}

type CapabilityKind =
  | "source"
  | "destination"
  | "parser"
  | "serializer"
  | "transformer";

interface CapabilityGroup {
  key: string;
  label: string;
  members: Map<CapabilityKind, Set<string>>;
  nonMembers: Map<CapabilityKind, Set<string>>;
}

const ENTITY_GROUPS: ReadonlyArray<{
  key: string;
  kind: CapabilityKind;
  label: string;
}> = [
  { key: "component.source", kind: "source", label: "All sources" },
  {
    key: "component.destination",
    kind: "destination",
    label: "All destinations",
  },
  { key: "component.parser", kind: "parser", label: "All parsers" },
  {
    key: "component.transformer",
    kind: "transformer",
    label: "All transformers",
  },
  {
    key: "component.serializer",
    kind: "serializer",
    label: "All serializers",
  },
];

const PROPERTY_LABELS: Record<string, string> = {
  "delivery_mode.batch": "Batch delivery",
  "delivery_mode.stream": "Stream delivery",
  "delivery_mode.batch_and_stream": "Batch + stream delivery",
  "record_semantics.append_only": "Append-only records",
  "record_semantics.only_append_only": "Only append-only records",
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
    if (
      property === "record_semantics.changelog" ||
      property === "record_semantics.only_changelog" ||
      ((property === "delivery_mode.batch" || property === "message_preview") &&
        kind === "destination")
    )
      return;
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
      endpoint.delivery_modes.forEach((mode) =>
        add(`delivery_mode.${mode}`, kind, connector.title),
      );
      endpoint.record_semantics.forEach((semantics) =>
        add(`record_semantics.${semantics}`, kind, connector.title),
      );
      if (endpoint.record_semantics.length === 1) {
        add(
          `record_semantics.only_${endpoint.record_semantics[0]}`,
          kind,
          connector.title,
        );
      }
      if (endpoint.partitioned) add("partitioned", kind, connector.title);
      if (endpoint.connection_check)
        add("connection_check", kind, connector.title);
      if (endpoint.message_preview)
        add("message_preview", kind, connector.title);
      collectSchemaCapabilities(
        endpoint.schema,
        add,
      );
    }
  }
  collectSchemaCapabilities(catalog.common_schema, add);
  const universes = new Map<CapabilityKind, Set<string>>();
  for (const group of groups.values()) {
    if (!group.key.startsWith("component.")) continue;
    for (const [kind, members] of group.members) {
      const universe = universes.get(kind) ?? new Set<string>();
      members.forEach((member) => universe.add(member));
      universes.set(kind, universe);
    }
  }
  for (const group of groups.values()) {
    for (const kind of applicableKinds(group.key)) {
      if (group.key === "record_semantics.only_append_only") {
        continue;
      }
      if (
        (group.key === "delivery_mode.batch" || group.key === "partitioned") &&
        kind === "destination"
      ) {
        continue;
      }
      if (group.key === "message_preview" && kind === "destination") continue;
      const members = group.members.get(kind) ?? new Set<string>();
      const nonMembers = new Set(
        [...(universes.get(kind) ?? [])].filter((title) => !members.has(title)),
      );
      if (nonMembers.size > 0) group.nonMembers.set(kind, nonMembers);
    }
  }
  return [...groups.values()].sort((left, right) =>
    left.label.localeCompare(right.label),
  );
}

function applicableKinds(property: string): CapabilityKind[] {
  if (property.startsWith("component.")) {
    if (property.startsWith("component.parser.")) return ["parser"];
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
    const title =
      typeof object.title === "string" ? object.title : descriptor.key;
    if (
      typeof title === "string" &&
      (kind === "parser" || kind === "serializer" || kind === "transformer")
    ) {
      const properties = Array.isArray(descriptor.properties)
        ? descriptor.properties
        : [];
      const declaredSemantics = Array.isArray(descriptor.record_semantics)
        ? descriptor.record_semantics
        : [];
      const semantics = declaredSemantics.map(
        (item) => `record_semantics.${item}`,
      );
      const exclusive =
        declaredSemantics.length === 1
          ? [`record_semantics.only_${declaredSemantics[0]}`]
          : [];
      [
        `component.${kind}`,
        ...properties,
        ...semantics,
        ...exclusive,
      ].forEach((property) => {
        if (typeof property === "string") add(property, kind, title);
      });
    }
  }
  Object.values(object).forEach((child) =>
    collectSchemaCapabilities(child, add),
  );
}

export interface CompatibilityRoute {
  source: ConnectorDefinition;
  sink: ConnectorDefinition;
  supported: DeliveryMode[];
  unsupported: DeliveryMode[];
  partial: DeliveryMode[];
}

export function CompatibilityMatrixLauncher({
  catalog,
}: {
  catalog: UiCatalog;
}) {
  const [open, setOpen] = useState(false);

  return (
    <>
      <Button
        class="sidebar-tool-button compatibility-launcher"
        onClick={() => setOpen(true)}
      >
        Matrix
      </Button>
      {open && (
        <CompatibilityMatrixDialog
          catalog={catalog}
          onClose={() => setOpen(false)}
        />
      )}
    </>
  );
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
        const produced = declaredSourceRecordSemantics(source.source!, mode);
        const acceptedCount = produced.filter((semantics) =>
          accepted.has(semantics),
        ).length;
        if (!routeSupportsDeliveryType(source.source!, sink.sink!, mode))
          unsupported.push(mode);
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
  const [selectedSource, setSelectedSource] = useState<string | null>(null);
  const [selectedSink, setSelectedSink] = useState<string | null>(null);
  const [matrixSearch, setMatrixSearch] = useState("");
  const [activeTab, setActiveTab] = useState<
    "matrix" | "entities" | "properties"
  >("matrix");
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
  const capabilityGroups = useMemo(
    () => catalogCapabilityGroups(catalog),
    [catalog],
  );
  const entityGroups = ENTITY_GROUPS.map((definition) => ({
    ...definition,
    group: capabilityGroups.find((group) => group.key === definition.key) ?? {
      key: definition.key,
      label: definition.label,
      members: new Map<CapabilityKind, Set<string>>(),
      nonMembers: new Map<CapabilityKind, Set<string>>(),
    },
  }));
  const propertyGroups = capabilityGroups.filter(
    (group) => !group.key.startsWith("component."),
  );
  const deliveryTypeProperties = propertyGroups.filter((group) =>
    group.key.startsWith("delivery_mode."),
  );
  const otherProperties = propertyGroups.filter(
    (group) => !group.key.startsWith("delivery_mode."),
  );
  const selectedProperty =
    propertyGroups.find((group) => group.key === activeProperty) ??
    propertyGroups[0];
  const route = (source: string, sink: string) =>
    routes.find(
      (candidate) =>
        candidate.source.key === source && candidate.sink.key === sink,
    )!;
  const normalizedMatrixSearch = matrixSearch.trim().toLocaleLowerCase();
  const matchesMatrixSearch = (title: string) =>
    normalizedMatrixSearch.length > 0 &&
    title.toLocaleLowerCase().includes(normalizedMatrixSearch);

  useLayoutEffect(() => {
    const previousOverflow = document.body.style.overflow;
    const previousPaddingRight = document.body.style.paddingRight;
    const scrollbarWidth = Math.max(
      0,
      window.innerWidth - document.documentElement.clientWidth,
    );
    document.body.style.overflow = "hidden";
    if (scrollbarWidth > 0) {
      const existingPadding = Number.parseFloat(
        window.getComputedStyle(document.body).paddingRight,
      );
      document.body.style.paddingRight = `${(Number.isFinite(existingPadding) ? existingPadding : 0) + scrollbarWidth}px`;
    }
    return () => {
      document.body.style.overflow = previousOverflow;
      document.body.style.paddingRight = previousPaddingRight;
    };
  }, []);

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
            <h2 id="compatibility-title">Matrix</h2>
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

        <div
          class="compatibility-tabs"
          role="tablist"
          aria-label="Catalog view"
        >
          <Button
            role="tab"
            aria-selected={activeTab === "matrix"}
            onClick={() => setActiveTab("matrix")}
          >
            Matrix
          </Button>
          <Button
            role="tab"
            aria-selected={activeTab === "entities"}
            onClick={() => setActiveTab("entities")}
          >
            Entities
          </Button>
          <Button
            role="tab"
            aria-selected={activeTab === "properties"}
            onClick={() => setActiveTab("properties")}
          >
            Properties
          </Button>
        </div>

        {activeTab === "matrix" ? (
          <div class="compatibility-matrix-tools">
            <div class="compatibility-legend" aria-label="Legend">
              <DeliveryModeBadge mode="batch" />
              <span>Batch flow is supported</span>
              <DeliveryModeBadge mode="stream" />
              <span>Stream flow is supported</span>
              <DeliveryModeBadge mode="batch_and_stream" />
              <span>Combined snapshot and stream flow is supported</span>
              <span class="compatibility-unavailable" aria-hidden="true">
                —
              </span>
              <span>No compatible data semantics</span>
            </div>
            <label class="compatibility-search">
              <span>Find source or destination</span>
              <AutofillResistantInput
                type="search"
                value={matrixSearch}
                onInput={(event) => setMatrixSearch(event.currentTarget.value)}
                placeholder="Search matrix"
              />
            </label>
          </div>
        ) : activeTab === "entities" ? (
          <div class="capability-summary">
            Browse every catalog entity in a stable source-to-serializer order.
          </div>
        ) : (
          <div class="capability-summary">
            Select one property and one entity to inspect their exact catalog
            relationship.
          </div>
        )}

        {activeTab === "matrix" ? (
          <div class="compatibility-table-scroll">
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
                      class={[
                        activeCell?.sink === sink.key || selectedSink === sink.key
                          ? "active-column"
                          : "",
                        matchesMatrixSearch(sink.title)
                          ? "search-match-column"
                          : "",
                      ]
                        .filter(Boolean)
                        .join(" ")}
                    >
                      <button
                        type="button"
                        aria-pressed={selectedSink === sink.key}
                        onClick={() =>
                          setSelectedSink((current) =>
                            current === sink.key ? null : sink.key,
                          )
                        }
                      >
                        {sink.title}
                      </button>
                    </th>
                  ))}
                  <th scope="col">Incomplete modes</th>
                </tr>
              </thead>
              <tbody>
                {sources.map((source) => (
                  <tr
                    key={source.key}
                    class={[
                      activeCell?.source === source.key ||
                      selectedSource === source.key
                        ? "active-row"
                        : "",
                      matchesMatrixSearch(source.title) ? "search-match-row" : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                  >
                    <th scope="row">
                      <button
                        type="button"
                        aria-pressed={selectedSource === source.key}
                        onClick={() =>
                          setSelectedSource((current) =>
                            current === source.key ? null : source.key,
                          )
                        }
                      >
                        <strong>{source.title}</strong>
                        <small>{source.source!.delivery_modes.join(" · ")}</small>
                      </button>
                    </th>
                    {sinks.map((sink) => (
                      <CompatibilityCell
                        key={sink.key}
                        route={route(source.key, sink.key)}
                        activeColumn={activeCell?.sink === sink.key}
                        selectedColumn={selectedSink === sink.key}
                        searchMatchColumn={matchesMatrixSearch(sink.title)}
                        activeIntersection={
                          activeCell?.source === source.key &&
                          activeCell.sink === sink.key
                        }
                        onActivate={() =>
                          setActiveCell({ source: source.key, sink: sink.key })
                        }
                      />
                    ))}
                    <SourceModeGapCell source={source} />
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : activeTab === "entities" ? (
          <div class="entity-browser">
            {entityGroups.map(({ key, kind, label, group }) => (
              <section class="entity-list" key={key} aria-label={label}>
                <h3>{label}</h3>
                <ul class="entity-catalog-list">
                  {(group.members.get(kind)?.size ?? 0) > 0 ? (
                    [...(group.members.get(kind) ?? [])]
                      .sort((left, right) => left.localeCompare(right))
                      .map((entity) => <li key={entity}>{entity}</li>)
                  ) : (
                    <li class="entity-empty">None</li>
                  )}
                </ul>
              </section>
            ))}
          </div>
        ) : (
          <div class="property-browser">
            <nav
              class="property-list always-visible-scrollbar"
              aria-label="Properties"
            >
              <section aria-label="Delivery type">
                <h3>Delivery type</h3>
                {deliveryTypeProperties.map((group) => (
                  <Button
                    key={group.key}
                    aria-pressed={selectedProperty?.key === group.key}
                    onClick={() => setActiveProperty(group.key)}
                  >
                    {group.label}
                  </Button>
                ))}
              </section>
              <section aria-label="Other properties">
                <h3>Other properties</h3>
                {otherProperties.map((group) => (
                  <Button
                    key={group.key}
                    aria-pressed={selectedProperty?.key === group.key}
                    onClick={() => setActiveProperty(group.key)}
                  >
                    {group.label}
                  </Button>
                ))}
              </section>
            </nav>
            <section
              class="property-members"
              aria-label="Property membership"
              aria-live="polite"
            >
              {selectedProperty ? (
                <>
                  <div class="property-entity-grid property-entity-headings">
                    {ENTITY_GROUPS.map(({ kind }) => (
                      <h3 key={kind}>{KIND_LABELS[kind]}</h3>
                    ))}
                  </div>
                  <div class="property-entity-grid property-has-row">
                    {ENTITY_GROUPS.map(({ kind }) => (
                      <EntityNameList
                        key={kind}
                        label={`${KIND_LABELS[kind]} with property`}
                        names={selectedProperty.members.get(kind)}
                      />
                    ))}
                  </div>
                  <h3 class="property-missing-heading">
                    Does not have property
                  </h3>
                  <div class="property-entity-grid property-missing-row">
                    {ENTITY_GROUPS.map(({ kind }) => (
                      <EntityNameList
                        key={kind}
                        label={`${KIND_LABELS[kind]} without property`}
                        names={selectedProperty.nonMembers.get(kind)}
                      />
                    ))}
                  </div>
                </>
              ) : (
                <p>No property is available.</p>
              )}
            </section>
          </div>
        )}

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

function EntityNameList({
  label,
  names,
}: {
  label: string;
  names: Set<string> | undefined;
}) {
  const sorted = [...(names ?? [])].sort((left, right) =>
    left.localeCompare(right),
  );
  return (
    <ul aria-label={label} class="property-entity-names">
      {sorted.length > 0 ? (
        sorted.map((name) => <li key={name}>{name}</li>)
      ) : (
        <li class="entity-empty">None</li>
      )}
    </ul>
  );
}

function CompatibilityCell({
  route,
  activeColumn,
  selectedColumn,
  searchMatchColumn,
  activeIntersection,
  onActivate,
}: {
  route: CompatibilityRoute;
  activeColumn: boolean;
  selectedColumn: boolean;
  searchMatchColumn: boolean;
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
        activeColumn || selectedColumn ? "active-column" : "",
        searchMatchColumn ? "search-match-column" : "",
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
        <span class="compatibility-badges">
          {route.supported.map((mode) => (
            <DeliveryModeBadge key={mode} mode={mode} />
          ))}
        </span>
      )}
    </td>
  );
}

function SourceModeGapCell({ source }: { source: ConnectorDefinition }) {
  const modes = source.source!.delivery_modes;
  const hasSnapshot = modes.includes("batch") || modes.includes("batch_and_stream");
  const hasReplication =
    modes.includes("stream") || modes.includes("batch_and_stream");
  const gaps = [
    ...(hasSnapshot ? [] : ["Snapshot is not implemented"]),
    ...(hasReplication ? [] : ["Replication is not implemented"]),
  ];

  return (
    <td class="compatibility-mode-gaps">
      {gaps.map((gap) => (
        <InstantTooltip key={gap} content={gap}>
          <span class="compatibility-gap-check" aria-hidden="true">
            ✓
          </span>
        </InstantTooltip>
      ))}
    </td>
  );
}
