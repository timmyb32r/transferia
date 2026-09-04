import { useEffect, useRef, useState } from "preact/hooks";

import { LatestJob } from "../effects";
import type { DynamicOptions } from "../generated/apiContract";
import { SelectControl } from "../ui/SelectControl";
import { useFormEnvironment } from "./formEnvironment";

export function DynamicSelectControl({
  id,
  source,
  dependencies,
  value,
  disabled,
  onChange,
}: {
  id?: string | undefined;
  source: string;
  dependencies: Record<string, string>;
  value: string;
  disabled: boolean;
  onChange: (value: string) => void;
}) {
  const { options: loadOptions } = useFormEnvironment();
  const [options, setOptions] = useState<
    Array<{ value: string; label: string }>
  >([]);
  const [loaded, setLoaded] = useState(false);
  const [status, setStatus] = useState<string>();
  const optionsJob = useRef(
    new LatestJob<string, string, DynamicOptions>(),
  ).current;
  const dependencyKey = JSON.stringify(dependencies);
  const load = (force = false) => {
    if (!force && (loaded || status === "Loading…")) return;
    setStatus("Loading…");
    void optionsJob
      .run(`${source}:${dependencyKey}`, source, (key, signal) =>
        loadOptions({ key, dependencies, signal }),
      )
      .then((result) => {
        if (result === undefined) return;
        setOptions(result.value.options);
        setLoaded(true);
        setStatus(result.value.warning);
      })
      .catch((error: unknown) =>
        setStatus(error instanceof Error ? error.message : String(error)),
      );
  };
  useEffect(() => {
    optionsJob.cancel();
    setOptions([]);
    setLoaded(false);
    setStatus(undefined);
    if (value !== "") load(true);
    return () => optionsJob.cancel();
  }, [source, dependencyKey]);
  useEffect(() => {
    if (value !== "") load();
  }, [value]);
  const visibleOptions =
    value !== "" && !options.some((option) => option.value === value)
      ? [{ value, label: `${value} (currently configured)` }, ...options]
      : options;
  return (
    <div class="dynamic-select">
      <SelectControl
        id={id}
        value={value}
        disabled={disabled}
        loading={status === "Loading…"}
        placeholder={status ?? "Not selected"}
        options={visibleOptions}
        searchable
        onOpen={load}
        onChange={onChange}
      />
      <div
        class={`dynamic-select-status${status !== undefined && status !== "Loading…" ? " error" : ""}`}
        aria-live="polite"
        role={status !== undefined && status !== "Loading…" ? "alert" : undefined}
      >
        {status !== undefined && status !== "Loading…" ? status : ""}
      </div>
    </div>
  );
}
