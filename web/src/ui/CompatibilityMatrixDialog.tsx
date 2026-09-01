import { useEffect, useMemo, useRef } from "preact/hooks";

import type {
  ConnectorDefinition,
  RecordSemantics,
  UiCatalog,
} from "../generated/apiContract";
import { Button } from "./Button";

const SEMANTICS_LABEL: Record<RecordSemantics, string> = {
  append_only: "Rows",
  changelog: "CDC",
};

export interface CompatibilityRoute {
  source: ConnectorDefinition;
  sink: ConnectorDefinition;
  supported: RecordSemantics[];
  unsupported: RecordSemantics[];
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
      const produced = source.source!.record_semantics;
      const accepted = new Set(sink.sink!.record_semantics);
      return {
        source,
        sink,
        supported: produced.filter((semantics) => accepted.has(semantics)),
        unsupported: produced.filter((semantics) => !accepted.has(semantics)),
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

  return (
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
              Rows are append-only snapshots or events. CDC includes inserts,
              updates, and deletes.
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
          <span class="compatibility-badge append-only">Rows</span>
          <span>Append-only data is supported</span>
          <span class="compatibility-badge changelog">CDC</span>
          <span>Change events are supported</span>
          <span class="compatibility-unavailable" aria-hidden="true">
            —
          </span>
          <span>No compatible data semantics</span>
        </div>

        <div class="compatibility-table-scroll">
          <table class="compatibility-table">
            <caption>Sources by destinations</caption>
            <thead>
              <tr>
                <th scope="col">Source ↓ / Destination →</th>
                {sinks.map((sink) => (
                  <th scope="col" key={sink.key} title={sink.title}>
                    {sink.title}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sources.map((source) => (
                <tr key={source.key}>
                  <th scope="row">
                    <strong>{source.title}</strong>
                    <small>{source.source!.delivery_modes.join(" · ")}</small>
                  </th>
                  {sinks.map((sink) => (
                    <CompatibilityCell
                      key={sink.key}
                      route={route(source.key, sink.key)}
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
    </div>
  );
}

function CompatibilityCell({ route }: { route: CompatibilityRoute }) {
  const sourceName = route.source.title;
  const sinkName = route.sink.title;
  const supportedLabels = route.supported.map(
    (semantics) => SEMANTICS_LABEL[semantics],
  );
  const unsupportedLabels = route.unsupported.map(
    (semantics) => SEMANTICS_LABEL[semantics],
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
      class={route.unsupported.length > 0 ? "partial" : undefined}
    >
      {route.supported.length === 0 ? (
        <span class="compatibility-unavailable" aria-hidden="true">
          —
        </span>
      ) : (
        <span class="compatibility-badges" aria-hidden="true">
          {route.supported.map((semantics) => (
            <span
              key={semantics}
              class={`compatibility-badge ${semantics.replace("_", "-")}`}
            >
              {SEMANTICS_LABEL[semantics]}
            </span>
          ))}
        </span>
      )}
    </td>
  );
}
