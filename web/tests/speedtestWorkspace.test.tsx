// @vitest-environment jsdom

import { act, cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  SpeedtestWorkspace,
  type SpeedtestEstimateResult,
  type SpeedtestTuneResult,
} from "../src/delivery/SpeedtestWorkspace";

const ESTIMATE_RESULT: SpeedtestEstimateResult = {
  logical_streams: 1,
  source: {
    rows_per_second: 1_250_000,
    bytes_per_second: 200_000_000,
    rows: "2500000",
    arrow_bytes: "400000000",
    duration_ms: 2_000,
    completed: false,
  },
  destination: {
    rows_per_second: 875_000,
    bytes_per_second: 140_000_000,
    rows: "1750000",
    arrow_bytes: "280000000",
    duration_ms: 2_000,
    completed: true,
  },
  profile: {
    sampled_rows: 10_000,
    sampled_arrow_bytes: 1_500_000,
    sampled_deliveries: 2,
    sample_limit_bytes: 16_777_216,
    truncated: false,
    datasets: [
      {
        dataset: "events",
        is_dlq: false,
        sampled_rows: 10_000,
        sampled_arrow_bytes: 1_500_000,
        columns: [
          {
            name: "id",
            arrow_type: "Int64",
            numeric_min: "1",
            numeric_max: "9999",
            null_count: 0,
          },
          {
            name: "message",
            arrow_type: "Utf8",
            min_length: 4,
            max_length: 128,
            cardinality: 3_000,
            null_count: 7,
          },
        ],
      },
    ],
  },
};

const TUNE_RESULT: SpeedtestTuneResult = {
  source: {
    baseline_rows_per_second: 1_000,
    optimized_rows_per_second: 1_200,
    gain_percent: 20,
    trials: 12,
    parameters: { concurrency: 4 },
    trial_history: [],
  },
  destination: {
    baseline_rows_per_second: 800,
    optimized_rows_per_second: 1_000,
    gain_percent: 25,
    trials: 10,
    parameters: { batch_rows: 100_000 },
    trial_history: [],
  },
};

describe("SpeedtestWorkspace", () => {
  afterEach(cleanup);

  it("starts on maximum-performance estimation and gives immediate stable pending feedback", async () => {
    const request = deferred<SpeedtestEstimateResult>();
    const estimate = vi.fn().mockReturnValue(request.promise);
    const view = render(
      <SpeedtestWorkspace
        config={{ source: { test: {} }, sink: { test: {} } }}
        estimate={estimate}
        tune={vi.fn()}
      />,
    );
    const testButton = view.getByRole("button", { name: "Test" });
    const resultSlot = view.getByRole("status");

    expect(
      view
        .getByRole("tab", { name: "Estimate maximum performance" })
        .getAttribute("aria-selected"),
    ).toBe("true");
    fireEvent.click(testButton);

    expect(testButton.getAttribute("aria-busy")).toBe("true");
    expect(resultSlot.getAttribute("aria-busy")).toBe("true");
    expect(view.getByText("Measuring source and destination…")).toBeTruthy();
    fireEvent.click(testButton);
    expect(estimate).toHaveBeenCalledOnce();
    expect(estimate.mock.calls[0]?.[1]).toEqual({
      duration_seconds: 10,
      cleanup_timeout_seconds: 60,
    });

    await act(async () => request.resolve(ESTIMATE_RESULT));

    expect(view.getByRole("button", { name: "Test" })).toBe(testButton);
    expect(view.getByRole("status")).toBe(resultSlot);
    expect(resultSlot.getAttribute("aria-busy")).toBe("false");
    expect(view.getByText(/1[,.]250[,.]000 rows\/s/)).toBeTruthy();
    expect(view.getByText(/875[,.]000 rows\/s/)).toBeTruthy();
    expect(view.getByText("message")).toBeTruthy();
    expect(view.getByText(/length 4…128/)).toBeTruthy();
    expect(view.getByText(/estimated cardinality 3[,.]000/)).toBeTruthy();
  });

  it("aborts an in-flight measurement and ignores its stale result when config changes", async () => {
    const request = deferred<SpeedtestEstimateResult>();
    let observedSignal: AbortSignal | undefined;
    const estimate = vi.fn(
      (_config: unknown, _options: unknown, signal: AbortSignal) => {
        observedSignal = signal;
        return request.promise;
      },
    );
    const view = render(
      <SpeedtestWorkspace
        config={{ source: { first: {} } }}
        estimate={estimate}
        tune={vi.fn()}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: "Test" }));
    view.rerender(
      <SpeedtestWorkspace
        config={{ source: { second: {} } }}
        estimate={estimate}
        tune={vi.fn()}
      />,
    );
    await act(async () => Promise.resolve());

    expect(observedSignal?.aborted).toBe(true);
    expect(view.getByText("Run the test to see throughput.")).toBeTruthy();
    await act(async () => request.resolve(ESTIMATE_RESULT));
    expect(view.queryByText(/1[,.]250[,.]000 rows\/s/)).toBeNull();
  });

  it("uses automatic tuning by default and renders both optimized parameter sets", async () => {
    const tune = vi.fn().mockResolvedValue(TUNE_RESULT);
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={vi.fn()} tune={tune} />,
    );

    fireEvent.click(view.getByRole("tab", { name: "Tune optimal parameters" }));
    expect(
      view
        .getByRole("radio", { name: "Automatic" })
        .getAttribute("aria-checked"),
    ).toBe("true");
    fireEvent.click(view.getByRole("button", { name: "Tune" }));
    await act(async () => Promise.resolve());

    expect(tune).toHaveBeenCalledOnce();
    expect(tune.mock.calls[0]?.[1]).toEqual({
      type: "automatic",
      max_trials: 12,
    });
    expect(tune.mock.calls[0]?.[2]).toEqual({
      trial_duration_seconds: 10,
      cleanup_timeout_seconds: 60,
    });
    expect(view.getByText("+20%")).toBeTruthy();
    expect(view.getByText("+25%")).toBeTruthy();
    expect(view.getByText("concurrency")).toBeTruthy();
    expect(view.getByText("batch_rows")).toBeTruthy();
  });

  it("converts a user-visible minute budget to seconds and keeps the input autofill-resistant", async () => {
    const tune = vi.fn().mockResolvedValue(TUNE_RESULT);
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={vi.fn()} tune={tune} />,
    );
    fireEvent.click(view.getByRole("tab", { name: "Tune optimal parameters" }));
    fireEvent.click(view.getByRole("radio", { name: "Time budget" }));
    const input = view.getByLabelText("Minutes") as HTMLInputElement;

    expect(input.disabled).toBe(false);
    expect(input.autocomplete).toBe("none");
    expect(input.name).toMatch(/^tf-/u);
    fireEvent.input(input, { target: { value: "2.5" } });
    fireEvent.click(view.getByRole("button", { name: "Tune" }));
    await act(async () => Promise.resolve());

    expect(tune.mock.calls[0]?.[1]).toEqual({ type: "time", seconds: 150 });
  });

  it("passes explicit positive integer measurement durations without changing the result slot", async () => {
    const estimate = vi.fn().mockResolvedValue(ESTIMATE_RESULT);
    const tune = vi.fn().mockResolvedValue(TUNE_RESULT);
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={estimate} tune={tune} />,
    );
    const estimateSlot = view.getByRole("status");
    const testDuration = view.getByLabelText(
      "Test duration in seconds",
    ) as HTMLInputElement;

    expect(testDuration.value).toBe("10");
    expect(testDuration.autocomplete).toBe("none");
    fireEvent.input(testDuration, { target: { value: "17" } });
    fireEvent.click(view.getByRole("button", { name: "Test" }));
    await act(async () => Promise.resolve());
    expect(estimate.mock.calls[0]?.[1]).toEqual({
      duration_seconds: 17,
      cleanup_timeout_seconds: 60,
    });
    expect(view.getByRole("status")).toBe(estimateSlot);

    fireEvent.click(view.getByRole("tab", { name: "Tune optimal parameters" }));
    const tuneSlot = view.getByRole("status");
    const trialDuration = view.getByLabelText(
      "Trial duration in seconds",
    ) as HTMLInputElement;
    expect(trialDuration.value).toBe("10");
    expect(trialDuration.autocomplete).toBe("none");
    fireEvent.input(trialDuration, { target: { value: "23" } });
    fireEvent.click(view.getByRole("button", { name: "Tune" }));
    await act(async () => Promise.resolve());
    expect(tune.mock.calls[0]?.[2]).toEqual({
      trial_duration_seconds: 23,
      cleanup_timeout_seconds: 60,
    });
    expect(view.getByRole("status")).toBe(tuneSlot);
  });

  it("rejects invalid test and trial durations before starting server work", () => {
    const estimate = vi.fn();
    const tune = vi.fn();
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={estimate} tune={tune} />,
    );
    const estimateSlot = view.getByRole("status");

    fireEvent.input(view.getByLabelText("Test duration in seconds"), {
      target: { value: "1.5" },
    });
    fireEvent.click(view.getByRole("button", { name: "Test" }));
    expect(estimate).not.toHaveBeenCalled();
    expect(view.getByRole("status")).toBe(estimateSlot);
    expect(view.getByRole("alert").textContent).toContain(
      "Test duration must be a positive whole number of seconds.",
    );

    fireEvent.click(view.getByRole("tab", { name: "Tune optimal parameters" }));
    const tuneSlot = view.getByRole("status");
    fireEvent.input(view.getByLabelText("Trial duration in seconds"), {
      target: { value: "0" },
    });
    fireEvent.click(view.getByRole("button", { name: "Tune" }));
    expect(tune).not.toHaveBeenCalled();
    expect(view.getByRole("status")).toBe(tuneSlot);
    expect(view.getByRole("alert").textContent).toContain(
      "Trial duration must be a positive whole number of seconds.",
    );
  });

  it("rejects an invalid cleanup timeout before starting server work", () => {
    const estimate = vi.fn();
    const tune = vi.fn();
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={estimate} tune={tune} />,
    );

    fireEvent.input(view.getByLabelText("Cleanup timeout in seconds"), {
      target: { value: "0" },
    });
    fireEvent.click(view.getByRole("button", { name: "Test" }));

    expect(estimate).not.toHaveBeenCalled();
    expect(view.getByRole("alert").textContent).toContain(
      "Cleanup timeout must be a positive whole number of seconds.",
    );

    fireEvent.click(view.getByRole("tab", { name: "Tune optimal parameters" }));
    fireEvent.click(view.getByRole("button", { name: "Tune" }));
    expect(tune).not.toHaveBeenCalled();
    expect(view.getByRole("alert").textContent).toContain(
      "Cleanup timeout must be a positive whole number of seconds.",
    );
  });

  it("rejects an invalid time budget locally without starting server work", () => {
    const tune = vi.fn();
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={vi.fn()} tune={tune} />,
    );
    fireEvent.click(view.getByRole("tab", { name: "Tune optimal parameters" }));
    fireEvent.click(view.getByRole("radio", { name: "Time budget" }));
    fireEvent.input(view.getByLabelText("Minutes"), {
      target: { value: "0" },
    });
    fireEvent.click(view.getByRole("button", { name: "Tune" }));

    expect(tune).not.toHaveBeenCalled();
    expect(view.getByRole("alert").textContent).toContain(
      "Time budget must be greater than zero minutes.",
    );

    fireEvent.input(view.getByLabelText("Minutes"), {
      target: { value: "0.001" },
    });
    fireEvent.click(view.getByRole("button", { name: "Tune" }));
    expect(tune).not.toHaveBeenCalled();
  });

  it("keeps automatic tuning explicitly bounded by a validated trial count", () => {
    const tune = vi.fn();
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={vi.fn()} tune={tune} />,
    );
    fireEvent.click(view.getByRole("tab", { name: "Tune optimal parameters" }));
    const maximumTrials = view.getByLabelText(
      "Maximum trials",
    ) as HTMLInputElement;

    expect(maximumTrials.value).toBe("12");
    expect(maximumTrials.autocomplete).toBe("none");
    fireEvent.input(maximumTrials, { target: { value: "0" } });
    fireEvent.click(view.getByRole("button", { name: "Tune" }));

    expect(tune).not.toHaveBeenCalled();
    expect(view.getByRole("alert").textContent).toContain(
      "Maximum trials must be a positive whole number.",
    );
  });

  it("surfaces server failures in the reserved result region", async () => {
    const estimate = vi.fn().mockRejectedValue(new Error("source timed out"));
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={estimate} tune={vi.fn()} />,
    );
    const slot = view.getByRole("status");

    fireEvent.click(view.getByRole("button", { name: "Test" }));
    await act(async () => Promise.resolve());

    expect(view.getByRole("status")).toBe(slot);
    expect(view.getByRole("alert").textContent).toContain("source timed out");
  });

  it("aborts an in-flight request when the workspace unmounts", async () => {
    const request = deferred<SpeedtestEstimateResult>();
    let observedSignal: AbortSignal | undefined;
    const estimate = vi.fn(
      (_config: unknown, _options: unknown, signal: AbortSignal) => {
        observedSignal = signal;
        return request.promise;
      },
    );
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={estimate} tune={vi.fn()} />,
    );

    fireEvent.click(view.getByRole("button", { name: "Test" }));
    view.unmount();
    await act(async () => Promise.resolve());

    expect(observedSignal?.aborted).toBe(true);
  });

  it("warns when the bounded in-flight profile was truncated", async () => {
    const estimate = vi.fn().mockResolvedValue({
      ...ESTIMATE_RESULT,
      profile: { ...ESTIMATE_RESULT.profile, truncated: true },
    });
    const view = render(
      <SpeedtestWorkspace config={{}} estimate={estimate} tune={vi.fn()} />,
    );

    fireEvent.click(view.getByRole("button", { name: "Test" }));
    await act(async () => Promise.resolve());

    expect(view.getByRole("note").textContent).toContain(
      "reached the configured pipeline-memory sample limit",
    );
    expect(view.getByText(/2 deliveries/)).toBeTruthy();
    expect(view.getByText(/16 MiB sample limit/)).toBeTruthy();
  });
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}
