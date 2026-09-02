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
  const sources = useMemo(
    () => catalog.connectors.filter((connector) => connector.source),
    [catalog],
  );
  const sinks = useMemo(
    () => catalog.connectors.filter((connector) => connector.sink),
    [catalog],
  );
  const routes = useMemo(() => compatibilityRoutes(catalog), [catalog]);
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

        <div class="compatibility-legend" aria-label="Legend">
          <span class="compatibility-badge batch">Batch</span>
          <span>Batch flow is supported</span>
          <span class="compatibility-badge stream">Stream</span>
          <span>Stream flow is supported</span>
          <span class="compatibility-unavailable" aria-hidden="true">
            —
          </span>
          <span>No compatible data semantics</span>
        </div>

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
        </div>

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
