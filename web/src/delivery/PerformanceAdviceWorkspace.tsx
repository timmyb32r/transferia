import type { DiscoveryResult } from "../types";

export function PerformanceAdviceWorkspace({
  result,
}: {
  result: DiscoveryResult | undefined;
}) {
  return (
    <section class="performance-advice-workspace" aria-label="Performance advice">
      <header class="performance-advice-header">
        <div>
          <h2>Performance advice</h2>
          <p>
            Recommendations derived from the discovered physical source layout.
          </p>
        </div>
      </header>
      {result === undefined ? (
        <p class="performance-advice-empty">
          Validate the delivery to inspect its physical layout and generate
          performance advice.
        </p>
      ) : result.performance_advice.length === 0 ? (
        <p class="performance-advice-empty success">
          No performance recommendations were reported for this configuration.
        </p>
      ) : (
        <ul class="performance-advice-list">
          {result.performance_advice.map((advice) => (
            <li
              key={`${advice.code}:${advice.config_paths.join(",")}`}
              class={`performance-advice-card ${advice.severity}`}
            >
              <div class="performance-advice-title">
                <span class="performance-advice-severity">
                  {advice.severity === "warning" ? "Warning" : "Info"}
                </span>
                <code>{advice.code}</code>
              </div>
              <h3>{advice.summary}</h3>
              <p>{advice.explanation}</p>
              <p class="performance-advice-remediation">
                <strong>Recommended action:</strong> {advice.remediation}
              </p>
              {advice.config_paths.length > 0 && (
                <div class="performance-advice-paths" aria-label="Related settings">
                  {advice.config_paths.map((path) => (
                    <code key={path}>{path}</code>
                  ))}
                </div>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
