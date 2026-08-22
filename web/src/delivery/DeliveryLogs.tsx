import { useEffect, useRef, useState } from "preact/hooks";

import { useControlPlane } from "../bootstrap/ApplicationServicesProvider";
import type { WorkerLogView } from "../types";
import { Button } from "../ui/Button";
import { SelectControl } from "../ui/SelectControl";

const MAX_VIEWER_CHARACTERS = 1024 * 1024;

export function DeliveryLogs({ deliveryId }: { deliveryId: string }) {
  const api = useControlPlane();
  const [workers, setWorkers] = useState<WorkerLogView[]>([]);
  const [workerId, setWorkerId] = useState("");
  const [text, setText] = useState("");
  const [cursor, setCursor] = useState<number>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [follow, setFollow] = useState(true);
  const viewport = useRef<HTMLPreElement>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const response = await api.deliveryLogs(deliveryId);
        if (cancelled) return;
        setWorkers(response.workers);
        setWorkerId((current) => {
          if (response.workers.some((worker) => worker.worker_id === current))
            return current;
          return (
            response.workers.find((worker) => worker.active)?.worker_id ??
            response.workers[0]?.worker_id ??
            ""
          );
        });
        setError(undefined);
      } catch (reason) {
        if (!cancelled) setError(errorMessage(reason));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), 3000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [deliveryId]);

  useEffect(() => {
    setText("");
    setCursor(undefined);
    if (workerId === "") return;
    let cancelled = false;
    let nextCursor: number | undefined;
    let timer: number | undefined;
    const read = async () => {
      try {
        const chunk = await api.deliveryLog(deliveryId, workerId, nextCursor);
        if (cancelled) return;
        nextCursor = chunk.next_offset;
        setCursor(chunk.next_offset);
        setText((current) => {
          const prefix = chunk.truncated_before
            ? "… earlier log output was omitted …\n"
            : "";
          const previous = chunk.truncated_before ? "" : current;
          return (prefix + previous + chunk.text).slice(-MAX_VIEWER_CHARACTERS);
        });
        setError(undefined);
      } catch (reason) {
        if (!cancelled) setError(errorMessage(reason));
      } finally {
        if (!cancelled) timer = window.setTimeout(() => void read(), 1000);
      }
    };
    void read();
    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [deliveryId, workerId]);

  useEffect(() => {
    if (!follow || viewport.current === null) return;
    viewport.current.scrollTop = viewport.current.scrollHeight;
  }, [text, follow]);

  return (
    <section class="delivery-logs" aria-label="Worker logs">
      <div class="delivery-logs-toolbar">
        <label>
          <span>Worker</span>
          <SelectControl
            id="delivery-log-worker"
            value={workerId}
            disabled={workers.length === 0}
            placeholder="No worker logs"
            clearable={false}
            options={workers.map((worker) => ({
              value: worker.worker_id,
              label: `${worker.worker_id} · ${formatBytes(worker.size_bytes)}${worker.active ? " · running" : ""}`,
            }))}
            onChange={setWorkerId}
          />
        </label>
        <label class="delivery-logs-follow">
          <input
            type="checkbox"
            checked={follow}
            onChange={(event) => setFollow(event.currentTarget.checked)}
          />
          Follow tail
        </label>
        <Button
          disabled={text === ""}
          onClick={() => {
            setText("");
            setCursor(undefined);
          }}
        >
          Clear view
        </Button>
        <span class="delivery-logs-progress-slot">
          {loading && (
            <span class="spinner" aria-label="Loading worker logs" />
          )}
        </span>
        {cursor !== undefined && (
          <small>{cursor.toLocaleString()} bytes read</small>
        )}
      </div>
      {error !== undefined && <div class="notice error">{error}</div>}
      <pre ref={viewport} class="delivery-log-viewer" tabIndex={0}>
        {text || (loading ? "Loading logs…" : "No log output yet.")}
      </pre>
    </section>
  );
}

function errorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
}
