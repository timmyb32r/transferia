import { useState } from "preact/hooks";

import { useControlPlane } from "../../bootstrap/ApplicationServicesProvider";
import { useSourceSample } from "./SourceSampleContext";
import { isObject } from "../../schema/value";
import type { JsonObject, JsonValue } from "../../types";
import { Button } from "../../ui/Button";
import { SelectControl } from "../../ui/SelectControl";

interface PlaygroundState {
  inputRows?: JsonValue[];
  activeTab: "input" | "output";
  loading: boolean;
  loadingSample?: boolean;
  error?: string;
  columns?: Array<{ name: string; arrow_type: string; nullable: boolean }>;
  rows?: JsonValue[];
}

export function MiddlewareEditor({
  value,
  disabled,
  onChange,
}: {
  value: JsonValue;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
}) {
  const api = useControlPlane();
  const entries = Array.isArray(value) ? value : [];
  const loadSourceSample = useSourceSample();
  const [playgrounds, setPlaygrounds] = useState<
    Record<number, PlaygroundState>
  >({});
  const replace = (index: number, entry: JsonValue) =>
    onChange(
      entries.map((current, offset) => (offset === index ? entry : current)),
    );
  return (
    <section class="middleware-editor">
      <header class="middleware-heading">
        <div>
          <h3>Transforms</h3>
          <p>Apply transforms in order between parsing and the destination.</p>
        </div>
        <Button
          disabled={disabled}
          onClick={() =>
            onChange([
              ...entries,
              { datafusion: { sql: "SELECT * FROM input" } },
            ])
          }
        >
          + Add SQL transform
        </Button>
      </header>
      {entries.length === 0 && (
        <p class="middleware-empty">
          No transforms. Rows pass through unchanged.
        </p>
      )}
      {entries.map((entry, index) => {
        const object = isObject(entry) ? entry : {};
        const kind = "datafusion" in object ? "datafusion" : "filter";
        const raw = isObject(object[kind]) ? object[kind] : {};
        const playground = playgrounds[index] ?? {
          activeTab: "input" as const,
          loading: false,
        };
        const setPlayground = (next: PlaygroundState) =>
          setPlaygrounds((current) => ({ ...current, [index]: next }));
        return (
          <article class="middleware-card" key={index}>
            <div class="middleware-card-heading">
              <strong>Transform {index + 1}</strong>
              <SelectControl
                value={kind}
                disabled={disabled}
                placeholder="Select transform"
                options={[
                  { value: "datafusion", label: "DataFusion SQL" },
                  { value: "filter", label: "String filter" },
                ]}
                onChange={(next) =>
                  replace(
                    index,
                    next === "datafusion"
                      ? { datafusion: { sql: "SELECT * FROM input" } }
                      : { filter: { field: "", value: "" } },
                  )
                }
              />
              <Button
                shape="icon"
                variant="danger"
                disabled={disabled}
                aria-label={`Delete transform ${index + 1}`}
                onClick={() =>
                  onChange(entries.filter((_, offset) => offset !== index))
                }
              >
                ×
              </Button>
            </div>
            {kind === "filter" ? (
              <div class="middleware-filter-fields">
                <label>
                  <span>Column</span>
                  <input
                    autoComplete="off"
                    value={typeof raw.field === "string" ? raw.field : ""}
                    disabled={disabled}
                    onInput={(event) =>
                      replace(index, {
                        filter: { ...raw, field: event.currentTarget.value },
                      })
                    }
                  />
                </label>
                <label>
                  <span>Equals</span>
                  <input
                    autoComplete="off"
                    value={typeof raw.value === "string" ? raw.value : ""}
                    disabled={disabled}
                    onInput={(event) =>
                      replace(index, {
                        filter: { ...raw, value: event.currentTarget.value },
                      })
                    }
                  />
                </label>
              </div>
            ) : (
              <>
                <label class="middleware-sql-field">
                  <span>
                    SQL over table <code>input</code>
                  </span>
                  <textarea
                    autoComplete="off"
                    spellcheck={false}
                    value={typeof raw.sql === "string" ? raw.sql : ""}
                    disabled={disabled}
                    onInput={(event) =>
                      replace(index, {
                        datafusion: { sql: event.currentTarget.value },
                      })
                    }
                  />
                </label>
                <section class="sql-playground" aria-label="Playground">
                  <Button
                    variant="primary"
                    disabled={playground.loading || playground.loadingSample}
                    onClick={async () => {
                      const { error: _error, ...pending } = playground;
                      setPlayground({ ...pending, loading: true });
                      try {
                        const rows =
                          playground.inputRows ??
                          (loadSourceSample
                            ? await loadSourceSample()
                            : undefined);
                        if (rows === undefined)
                          throw new Error("Source sample is unavailable");
                        const result = await api.sqlPlayground({
                          sql: typeof raw.sql === "string" ? raw.sql : "",
                          rows,
                        });
                        setPlayground({
                          ...playground,
                          inputRows: rows,
                          activeTab: "output",
                          loading: false,
                          ...result,
                        });
                      } catch (error) {
                        setPlayground({
                          ...playground,
                          loading: false,
                          error:
                            error instanceof Error
                              ? error.message
                              : String(error),
                        });
                      }
                    }}
                  >
                    {playground.loading || playground.loadingSample
                      ? "Running…"
                      : "Run sample"}
                  </Button>
                  <div
                    class="editor-view-tabs sql-playground-tabs"
                    role="tablist"
                    aria-label="DataFusion sample view"
                  >
                    {(["input", "output"] as const).map((tab) => (
                      <Button
                        key={tab}
                        role="tab"
                        aria-selected={playground.activeTab === tab}
                        class={playground.activeTab === tab ? "active" : ""}
                        onClick={() =>
                          setPlayground({ ...playground, activeTab: tab })
                        }
                      >
                        {tab === "input" ? "Input" : "Output"}
                      </Button>
                    ))}
                  </div>
                  <div class="sql-playground-output" role="tabpanel">
                    {playground.error && <p role="alert">{playground.error}</p>}
                    {playground.activeTab === "input" ? (
                      <RowsTable rows={playground.inputRows ?? []} />
                    ) : playground.columns ? (
                      <table>
                        <thead>
                          <tr>
                            {playground.columns.map((column) => (
                              <th key={column.name}>
                                {column.name}
                                <small>{column.arrow_type}</small>
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {(playground.rows ?? []).map((row, rowIndex) => (
                            <tr key={rowIndex}>
                              {playground.columns?.map((column) => (
                                <td key={column.name}>
                                  {JSON.stringify(
                                    isObject(row) ? row[column.name] : null,
                                  )}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    ) : (
                      <p>Run the sample to see output.</p>
                    )}
                  </div>
                </section>
              </>
            )}
          </article>
        );
      })}
    </section>
  );
}

function RowsTable({ rows }: { rows: JsonValue[] }) {
  const columns = Array.from(
    new Set(rows.flatMap((row) => (isObject(row) ? Object.keys(row) : []))),
  );
  if (rows.length === 0) return <p>Run the sample to load input rows.</p>;
  return (
    <table>
      <thead>
        <tr>
          {columns.map((column) => (
            <th key={column}>{column}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, index) => (
          <tr key={index}>
            {columns.map((column) => (
              <td key={column}>
                {JSON.stringify(isObject(row) ? row[column] : null)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
