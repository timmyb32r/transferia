// @vitest-environment jsdom

import { cleanup, render } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";

import { PerformanceAdviceWorkspace } from "../src/delivery/PerformanceAdviceWorkspace";
import type { DiscoveryResult } from "../src/types";

afterEach(cleanup);

function discovery(
  performance_advice: DiscoveryResult["performance_advice"],
): DiscoveryResult {
  return {
    source: "ytsaurus",
    sink: "clickhouse",
    pipeline_count: 1,
    performance_advice,
    datasets: [],
    sink_limits: { sink: "clickhouse", supported_arrow_types: [] },
  };
}

describe("PerformanceAdviceWorkspace", () => {
  it("keeps the workspace stable before discovery", () => {
    const view = render(<PerformanceAdviceWorkspace result={undefined} />);

    expect(view.getByRole("region", { name: "Performance advice" })).toBeTruthy();
    expect(view.getByText(/Validate the delivery/)).toBeTruthy();
  });

  it("renders structured physical-layout advice", () => {
    const view = render(
      <PerformanceAdviceWorkspace
        result={discovery([
          {
            code: "YT_SCAN_HAS_NON_COLUMNAR_CHUNKS",
            severity: "warning",
            summary: "Table contains non-columnar chunks",
            explanation: "Only 12 of 16 chunks are columnar.",
            remediation: "Physically rewrite every chunk.",
            config_paths: ["source.ytsaurus.tables"],
          },
        ])}
      />,
    );

    expect(view.getByText("YT_SCAN_HAS_NON_COLUMNAR_CHUNKS")).toBeTruthy();
    expect(view.getByText("Table contains non-columnar chunks")).toBeTruthy();
    expect(view.getByText("source.ytsaurus.tables")).toBeTruthy();
  });

  it("reports an explicitly empty recommendation set", () => {
    const view = render(<PerformanceAdviceWorkspace result={discovery([])} />);

    expect(view.getByText(/No performance recommendations/)).toBeTruthy();
  });
});
