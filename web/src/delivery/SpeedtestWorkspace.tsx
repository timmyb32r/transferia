import type { ComponentChildren } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

import type {
  SpeedtestColumnProfileView,
  SpeedtestDatasetProfileView,
  SpeedtestEstimateResult as ApiSpeedtestEstimateResult,
  SpeedtestMeasurementView,
  SpeedtestTuneResult as ApiSpeedtestTuneResult,
  SpeedtestTuningBudgetView,
  SpeedtestTuningResultView,
} from "../generated/apiContract";
import type { JsonObject } from "../types";
import { AutofillResistantInput } from "../ui/AutofillResistantField";
import { Button } from "../ui/Button";

export type SpeedtestMeasurement = SpeedtestMeasurementView;
export type SpeedtestProfileColumn = SpeedtestColumnProfileView;
export type SpeedtestDatasetProfile = SpeedtestDatasetProfileView;
export type SpeedtestEstimateResult = ApiSpeedtestEstimateResult;
export type SpeedtestTuningBudget = SpeedtestTuningBudgetView;
export type SpeedtestTuningResult = SpeedtestTuningResultView;
export type SpeedtestTuneResult = ApiSpeedtestTuneResult;

export interface SpeedtestEstimateOptions {
  duration_seconds: number;

  cleanup_timeout_seconds: number;
}

export interface SpeedtestTuneOptions {
  trial_duration_seconds: number;

  cleanup_timeout_seconds: number;
}

type AsyncResult<T> =
  | { state: "idle" }
  | { state: "pending" }
  | { state: "ready"; value: T }
  | { state: "failed"; message: string };

const IDLE = { state: "idle" } as const;

export function SpeedtestWorkspace({
  config,
  estimate,
  tune,
}: {
  config: JsonObject;
  estimate: (
    config: JsonObject,
    options: SpeedtestEstimateOptions,
    signal: AbortSignal,
  ) => Promise<SpeedtestEstimateResult>;
  tune: (
    config: JsonObject,
    budget: SpeedtestTuningBudget,
    options: SpeedtestTuneOptions,
    signal: AbortSignal,
  ) => Promise<SpeedtestTuneResult>;
}) {
  const [activeTab, setActiveTab] = useState<"estimate" | "tune">(
    "estimate",
  );
  const [estimateResult, setEstimateResult] =
    useState<AsyncResult<SpeedtestEstimateResult>>(IDLE);
  const [tuneResult, setTuneResult] =
    useState<AsyncResult<SpeedtestTuneResult>>(IDLE);
  const [budgetType, setBudgetType] = useState<"automatic" | "time">(
    "automatic",
  );
  const [maximumTrials, setMaximumTrials] = useState("12");
  const [minutes, setMinutes] = useState("10");
  const [testDurationSeconds, setTestDurationSeconds] = useState("10");
  const [trialDurationSeconds, setTrialDurationSeconds] = useState("10");
  const [cleanupTimeoutSeconds, setCleanupTimeoutSeconds] = useState("60");
  const requestSequence = useRef(0);
  const activeRequest = useRef<AbortController>();

  useEffect(() => {
    requestSequence.current += 1;
    activeRequest.current?.abort();
    activeRequest.current = undefined;
    setEstimateResult(IDLE);
    setTuneResult(IDLE);
  }, [config]);

  useEffect(
    () => () => {
      requestSequence.current += 1;
      activeRequest.current?.abort();
    },
    [],
  );

  const pending =
    estimateResult.state === "pending" || tuneResult.state === "pending";

  const startEstimate = async () => {
    if (pending) return;
    const durationSeconds = positiveInteger(testDurationSeconds);
    const cleanupTimeout = positiveInteger(cleanupTimeoutSeconds);
    if (durationSeconds === undefined) {
      setEstimateResult({
        state: "failed",
        message: "Test duration must be a positive whole number of seconds.",
      });
      return;
    }
    if (cleanupTimeout === undefined) {
      setEstimateResult({
        state: "failed",
        message: "Cleanup timeout must be a positive whole number of seconds.",
      });
      return;
    }
    const requestId = ++requestSequence.current;
    const controller = new AbortController();
    activeRequest.current = controller;
    setEstimateResult({ state: "pending" });
    try {
      const value = await estimate(
        config,
        {
          duration_seconds: durationSeconds,
          cleanup_timeout_seconds: cleanupTimeout,
        },
        controller.signal,
      );
      if (requestId !== requestSequence.current || controller.signal.aborted)
        return;
      setEstimateResult({ state: "ready", value });
    } catch (reason) {
      if (requestId !== requestSequence.current || controller.signal.aborted)
        return;
      setEstimateResult({ state: "failed", message: errorMessage(reason) });
    } finally {
      if (requestId === requestSequence.current)
        activeRequest.current = undefined;
    }
  };

  const startTuning = async () => {
    if (pending) return;
    const parsedMinutes = Number(minutes);
    const parsedBudgetSeconds = Math.round(parsedMinutes * 60);
    const parsedMaximumTrials = positiveInteger(maximumTrials);
    const parsedTrialDuration = positiveInteger(trialDurationSeconds);
    const cleanupTimeout = positiveInteger(cleanupTimeoutSeconds);
    if (parsedTrialDuration === undefined) {
      setTuneResult({
        state: "failed",
        message: "Trial duration must be a positive whole number of seconds.",
      });
      return;
    }
    if (cleanupTimeout === undefined) {
      setTuneResult({
        state: "failed",
        message: "Cleanup timeout must be a positive whole number of seconds.",
      });
      return;
    }
    if (budgetType === "automatic" && parsedMaximumTrials === undefined) {
      setTuneResult({
        state: "failed",
        message: "Maximum trials must be a positive whole number.",
      });
      return;
    }
    if (
      budgetType === "time" &&
      (!Number.isFinite(parsedMinutes) ||
        parsedMinutes <= 0 ||
        !Number.isSafeInteger(parsedBudgetSeconds) ||
        parsedBudgetSeconds <= 0)
    ) {
      setTuneResult({
        state: "failed",
        message: "Time budget must be greater than zero minutes.",
      });
      return;
    }
    const requestId = ++requestSequence.current;
    const controller = new AbortController();
    activeRequest.current = controller;
    setTuneResult({ state: "pending" });
    try {
      const budget: SpeedtestTuningBudget =
        budgetType === "automatic"
          ? { type: "automatic", max_trials: parsedMaximumTrials! }
          : { type: "time", seconds: parsedBudgetSeconds };
      const value = await tune(
        config,
        budget,
        {
          trial_duration_seconds: parsedTrialDuration,
          cleanup_timeout_seconds: cleanupTimeout,
        },
        controller.signal,
      );
      if (requestId !== requestSequence.current || controller.signal.aborted)
        return;
      setTuneResult({ state: "ready", value });
    } catch (reason) {
      if (requestId !== requestSequence.current || controller.signal.aborted)
        return;
      setTuneResult({ state: "failed", message: errorMessage(reason) });
    } finally {
      if (requestId === requestSequence.current)
        activeRequest.current = undefined;
    }
  };

  return (
    <section class="speedtest-workspace" aria-label="Speedtest">
      <header class="speedtest-heading">
        <div>
          <h2>Speedtest</h2>
          <p>
            Measure the source and destination independently using one logical
            stream.
          </p>
        </div>
        <span class="speedtest-stream-count">1 logical stream</span>
      </header>

      <div
        class="editor-view-tabs speedtest-tabs"
        role="tablist"
        aria-label="Speedtest mode"
      >
        <Button
          role="tab"
          aria-selected={activeTab === "estimate"}
          class={activeTab === "estimate" ? "active" : ""}
          disabled={pending}
          onClick={() => setActiveTab("estimate")}
        >
          Estimate maximum performance
        </Button>
        <Button
          role="tab"
          aria-selected={activeTab === "tune"}
          class={activeTab === "tune" ? "active" : ""}
          disabled={pending}
          onClick={() => setActiveTab("tune")}
        >
          Tune optimal parameters
        </Button>
      </div>

      {activeTab === "estimate" ? (
        <section
          class="speedtest-panel"
          role="tabpanel"
          aria-label="Estimate maximum performance"
        >
          <div class="speedtest-action-column">
            <label class="speedtest-duration-field">
              <span>Duration per stage</span>
              <span class="speedtest-duration-input">
                <AutofillResistantInput
                  aria-label="Test duration in seconds"
                  type="text"
                  inputMode="numeric"
                  value={testDurationSeconds}
                  disabled={pending}
                  onInput={(event) =>
                    setTestDurationSeconds(event.currentTarget.value)
                  }
                />
                <span>seconds</span>
              </span>
            </label>
            <CleanupTimeoutField
              value={cleanupTimeoutSeconds}
              disabled={pending}
              onInput={setCleanupTimeoutSeconds}
            />
            <Button
              variant="primary"
              class="speedtest-run-button"
              pending={estimateResult.state === "pending"}
              disabled={pending && estimateResult.state !== "pending"}
              onClick={() => void startEstimate()}
            >
              Test
            </Button>
            <p>
              Reads source → discard, profiles a sample, then writes a matching
              in-flight generator → destination.
            </p>
          </div>
          <ResultSlot pending={estimateResult.state === "pending"}>
            <EstimateResult result={estimateResult} />
          </ResultSlot>
        </section>
      ) : (
        <section
          class="speedtest-panel"
          role="tabpanel"
          aria-label="Tune optimal parameters"
        >
          <div class="speedtest-action-column">
            <div
              class="speedtest-budget-selector"
              role="radiogroup"
              aria-label="Tuning budget"
            >
              <Button
                role="radio"
                aria-checked={budgetType === "automatic"}
                class={budgetType === "automatic" ? "active" : ""}
                disabled={pending}
                onClick={() => setBudgetType("automatic")}
              >
                Automatic
              </Button>
              <Button
                role="radio"
                aria-checked={budgetType === "time"}
                class={budgetType === "time" ? "active" : ""}
                disabled={pending}
                onClick={() => setBudgetType("time")}
              >
                Time budget
              </Button>
            </div>
            <label class="speedtest-time-field">
              <span>Maximum trials</span>
              <AutofillResistantInput
                type="text"
                inputMode="numeric"
                value={maximumTrials}
                disabled={pending || budgetType !== "automatic"}
                onInput={(event) =>
                  setMaximumTrials(event.currentTarget.value)
                }
              />
            </label>
            <CleanupTimeoutField
              value={cleanupTimeoutSeconds}
              disabled={pending}
              onInput={setCleanupTimeoutSeconds}
            />
            <label class="speedtest-time-field">
              <span>Minutes</span>
              <AutofillResistantInput
                type="text"
                inputMode="decimal"
                value={minutes}
                disabled={pending || budgetType !== "time"}
                onInput={(event) => setMinutes(event.currentTarget.value)}
              />
            </label>
            <label class="speedtest-duration-field">
              <span>Trial duration</span>
              <span class="speedtest-duration-input">
                <AutofillResistantInput
                  aria-label="Trial duration in seconds"
                  type="text"
                  inputMode="numeric"
                  value={trialDurationSeconds}
                  disabled={pending}
                  onInput={(event) =>
                    setTrialDurationSeconds(event.currentTarget.value)
                  }
                />
                <span>seconds</span>
              </span>
            </label>
            <Button
              variant="primary"
              class="speedtest-run-button"
              pending={tuneResult.state === "pending"}
              disabled={pending && tuneResult.state !== "pending"}
              onClick={() => void startTuning()}
            >
              Tune
            </Button>
            <p>
              Source and destination candidates are evaluated in parallel from
              their connector-authored defaults using only declared safe
              parameters. The time budget starts after the one-time empirical
              profile and source baseline; each of those is bounded by the
              trial duration.
            </p>
          </div>
          <ResultSlot pending={tuneResult.state === "pending"}>
            <TuneResult result={tuneResult} />
          </ResultSlot>
        </section>
      )}
    </section>
  );
}

function ResultSlot({
  pending,
  children,
}: {
  pending: boolean;
  children: ComponentChildren;
}) {
  return (
    <div
      class="speedtest-result-slot"
      role="status"
      aria-live="polite"
      aria-busy={pending}
    >
      {children}
    </div>
  );
}

function CleanupTimeoutField({
  value,
  disabled,
  onInput,
}: {
  value: string;
  disabled: boolean;
  onInput: (value: string) => void;
}) {
  return (
    <label class="speedtest-duration-field">
      <span>Cleanup timeout</span>
      <span class="speedtest-duration-input">
        <AutofillResistantInput
          aria-label="Cleanup timeout in seconds"
          type="text"
          inputMode="numeric"
          value={value}
          disabled={disabled}
          onInput={(event) => onInput(event.currentTarget.value)}
        />
        <span>seconds</span>
      </span>
    </label>
  );
}

function EstimateResult({
  result,
}: {
  result: AsyncResult<SpeedtestEstimateResult>;
}) {
  if (result.state === "idle")
    return <p class="speedtest-placeholder">Run the test to see throughput.</p>;
  if (result.state === "pending")
    return (
      <p class="speedtest-placeholder">
        <span class="spinner" /> Measuring source and destination…
      </p>
    );
  if (result.state === "failed")
    return (
      <p class="speedtest-error" role="alert">
        Speedtest failed: {result.message}
      </p>
    );
  return (
    <div class="speedtest-results">
      <div class="speedtest-measurements">
        <MeasurementCard title="Source read" value={result.value.source} />
        <MeasurementCard
          title="Destination write"
          value={result.value.destination}
        />
      </div>
      <h3>In-flight generator profile</h3>
      <p>
        {formatInteger(result.value.profile.sampled_rows)} sampled rows ·{" "}
        {formatInteger(result.value.profile.sampled_deliveries)} deliveries ·{" "}
        {formatBytes(result.value.profile.sampled_arrow_bytes)} Arrow ·{" "}
        {formatBytes(result.value.profile.sample_limit_bytes)} sample limit ·{" "}
        {result.value.profile.datasets.length} datasets
      </p>
      {result.value.profile.truncated && (
        <p class="speedtest-profile-warning" role="note">
          The in-flight profile reached the configured pipeline-memory sample
          limit. Destination throughput uses this bounded observed workload;
          rare later shapes may not be represented.
        </p>
      )}
      {result.value.profile.datasets.map((dataset) => (
        <section class="speedtest-dataset-profile" key={dataset.dataset}>
          <h4>
            {dataset.dataset}
            {dataset.is_dlq ? " · dead-letter queue" : ""}
          </h4>
          <small>
            {formatInteger(dataset.sampled_rows)} rows ·{" "}
            {formatBytes(dataset.sampled_arrow_bytes)} Arrow
          </small>
          <table class="speedtest-profile-table">
            <thead>
              <tr>
                <th>Column</th>
                <th>Arrow type</th>
                <th>Observed distribution</th>
              </tr>
            </thead>
            <tbody>
              {dataset.columns.map((column) => (
                <tr key={column.name}>
                  <td>{column.name}</td>
                  <td>{column.arrow_type}</td>
                  <td>{profileDescription(column)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ))}
    </div>
  );
}

function TuneResult({ result }: { result: AsyncResult<SpeedtestTuneResult> }) {
  if (result.state === "idle")
    return (
      <p class="speedtest-placeholder">
        Choose a budget and start parameter tuning.
      </p>
    );
  if (result.state === "pending")
    return (
      <p class="speedtest-placeholder">
        <span class="spinner" /> Tuning source and destination in parallel…
      </p>
    );
  if (result.state === "failed")
    return (
      <p class="speedtest-error" role="alert">
        Tuning failed: {result.message}
      </p>
    );
  return (
    <div class="speedtest-tuning-results">
      <TuningCard title="Source" result={result.value.source} />
      <TuningCard title="Destination" result={result.value.destination} />
    </div>
  );
}

function MeasurementCard({
  title,
  value,
}: {
  title: string;
  value: SpeedtestMeasurement;
}) {
  return (
    <article class="speedtest-metric-card">
      <h3>{title}</h3>
      <strong>{formatRate(value.rows_per_second)}</strong>
      <span>
        {formatByteRate(value.bytes_per_second)} · {formatCount(value.rows)} rows
        · {formatBytes(value.arrow_bytes)} Arrow in{" "}
        {formatDuration(value.duration_ms)}
        {value.completed ? " · completed" : " · bounded sample"}
      </span>
    </article>
  );
}

function TuningCard({
  title,
  result,
}: {
  title: string;
  result: SpeedtestTuningResult;
}) {
  return (
    <article class="speedtest-tuning-card">
      <h3>{title}</h3>
      <p>
        <strong>{formatRate(result.optimized_rows_per_second)}</strong>
        <span class="speedtest-gain">
          +{formatNumber(result.gain_percent)}%
        </span>
      </p>
      <small>
        Default {formatRate(result.baseline_rows_per_second)} · {result.trials}{" "}
        trials
      </small>
      <dl>
        {Object.entries(result.parameters).map(([name, value]) => (
          <div key={name}>
            <dt>{name}</dt>
            <dd>{JSON.stringify(value)}</dd>
          </div>
        ))}
      </dl>
    </article>
  );
}

function profileDescription(column: SpeedtestProfileColumn): string {
  const values: string[] = [];
  if (column.numeric_min !== undefined && column.numeric_max !== undefined)
    values.push(`${column.numeric_min}…${column.numeric_max}`);
  if (column.temporal_min !== undefined && column.temporal_max !== undefined)
    values.push(`${column.temporal_min}…${column.temporal_max}`);
  if (column.min_length !== undefined && column.max_length !== undefined)
    values.push(`length ${column.min_length}…${column.max_length}`);
  if (column.cardinality !== undefined)
    values.push(`estimated cardinality ${formatInteger(column.cardinality)}`);
  values.push(`nulls ${formatInteger(column.null_count)}`);
  return values.join(" · ") || "—";
}

function formatRate(value: number): string {
  return `${formatNumber(value)} rows/s`;
}

function formatDuration(milliseconds: number): string {
  return `${formatNumber(milliseconds / 1_000)} s`;
}

function formatByteRate(value: number): string {
  return `${formatNumber(value / (1024 * 1024))} MiB/s`;
}

function formatBytes(value: number | string): string {
  const bytes = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(bytes)) return `${value} bytes`;
  return `${formatNumber(bytes / (1024 * 1024))} MiB`;
}

function formatCount(value: string): string {
  try {
    return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(
      BigInt(value),
    );
  } catch {
    return value;
  }
}

function formatInteger(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(
    value,
  );
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 2,
  }).format(value);
}

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}

function positiveInteger(value: string): number | undefined {
  if (!/^[1-9][0-9]*$/u.test(value)) return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}
