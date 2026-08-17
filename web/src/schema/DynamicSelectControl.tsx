import { useEffect, useRef, useState } from "preact/hooks";

import { api } from "../api";
import { LatestJob } from "../effects";
import { SelectControl } from "../ui/SelectControl";

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
  const [options, setOptions] = useState<
    Array<{ value: string; label: string }>
  >([]);
  const [loaded, setLoaded] = useState(false);
  const [status, setStatus] = useState<string>();
  const optionsJob = useRef(
    new LatestJob<string, string, Awaited<ReturnType<typeof api.options>>>(),
  ).current;
  const dependencyKey = JSON.stringify(dependencies);
  const load = (force = false) => {
    if (!force && (loaded || status === "Loading…")) return;
    setStatus("Loading…");
    void optionsJob
      .run(`${source}:${dependencyKey}`, source, (key, signal) =>
        api.options(key, dependencies, false, signal),
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
      {status !== undefined && status !== "Loading…" && (
        <div class="field-hint error">{status}</div>
      )}
    </div>
  );
}
