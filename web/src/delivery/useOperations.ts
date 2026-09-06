import { useCallback, useEffect, useRef, useState } from "preact/hooks";

import type { OperationKey, OperationState } from "../application/operations";

const PROGRESS_REVEAL_DELAY_MS = 200;
const PROGRESS_MIN_VISIBLE_MS = 500;

interface ProgressTiming {
  requestId: number;
  label?: string;
  revealTimer?: number;
  removeTimer?: number;
  visibleAt?: number;
}

export function useOperations() {
  const [operations, setOperations] = useState<
    Partial<Record<OperationKey, OperationState>>
  >({});
  const sequence = useRef(0);
  const progressTimings = useRef(new Map<OperationKey, ProgressTiming>());

  const cancelTiming = useCallback((key: OperationKey) => {
    const timing = progressTimings.current.get(key);
    if (timing?.revealTimer !== undefined)
      window.clearTimeout(timing.revealTimer);
    if (timing?.removeTimer !== undefined)
      window.clearTimeout(timing.removeTimer);
    progressTimings.current.delete(key);
  }, []);

  useEffect(
    () => () => {
      for (const key of progressTimings.current.keys()) cancelTiming(key);
    },
    [cancelTiming],
  );

  const begin = useCallback((key: OperationKey, label?: string): number => {
    const requestId = ++sequence.current;
    cancelTiming(key);
    setOperations((current) => ({
      ...current,
      [key]: { requestId },
    }));
    if (label !== undefined) {
      const timing: ProgressTiming = { requestId, label };
      timing.revealTimer = window.setTimeout(() => {
        const currentTiming = progressTimings.current.get(key);
        if (currentTiming?.requestId !== requestId) return;
        delete currentTiming.revealTimer;
        currentTiming.visibleAt = performance.now();
        setOperations((current) =>
          current[key]?.requestId === requestId
            ? { ...current, [key]: { requestId, label: currentTiming.label ?? label } }
            : current,
        );
      }, PROGRESS_REVEAL_DELAY_MS);
      progressTimings.current.set(key, timing);
    }
    return requestId;
  }, [cancelTiming]);

  const finish = useCallback(
    (
      key: OperationKey,
      requestId: number,
      error?: string,
      success?: string,
    ) => {
      const timing = progressTimings.current.get(key);
      if (timing?.requestId === requestId && timing.revealTimer !== undefined) {
        window.clearTimeout(timing.revealTimer);
        delete timing.revealTimer;
      }
      setOperations((current) => {
        if (current[key]?.requestId !== requestId) return current;
        if (error !== undefined) {
          cancelTiming(key);
          return { ...current, [key]: { requestId, error } };
        }
        if (success !== undefined) {
          cancelTiming(key);
          return { ...current, [key]: { requestId, success } };
        }
        if (timing?.requestId === requestId && timing.visibleAt !== undefined) {
          const remaining = Math.max(
            0,
            PROGRESS_MIN_VISIBLE_MS - (performance.now() - timing.visibleAt),
          );
          if (remaining > 0) {
            timing.removeTimer = window.setTimeout(() => {
              progressTimings.current.delete(key);
              setOperations((latest) => {
                if (latest[key]?.requestId !== requestId) return latest;
                const next = { ...latest };
                delete next[key];
                return next;
              });
            }, remaining);
            return current;
          }
        }
        cancelTiming(key);
        const next = { ...current };
        delete next[key];
        return next;
      });
    },
    [cancelTiming],
  );

  const update = useCallback((key: OperationKey, requestId: number, label: string) => {
    const timing = progressTimings.current.get(key);
    if (timing?.requestId !== requestId || timing.removeTimer !== undefined) return;
    timing.label = label;
    if (timing.visibleAt === undefined) return;
    setOperations(current => current[key]?.requestId === requestId && !current[key]?.error && !current[key]?.success
      ? { ...current, [key]: { requestId, label } } : current);
  }, []);

  const clearErrors = useCallback(() => {
    setOperations((current) =>
      Object.fromEntries(
        Object.entries(current).filter(([, operation]) => !operation?.error),
      ),
    );
  }, []);

  const clear = useCallback((key: OperationKey) => {
    cancelTiming(key);
    setOperations((current) => {
      if (current[key] === undefined) return current;
      const next = { ...current };
      delete next[key];
      return next;
    });
  }, [cancelTiming]);

  const reset = useCallback(
    (next: Partial<Record<OperationKey, OperationState>>) => {
      for (const key of progressTimings.current.keys()) cancelTiming(key);
      setOperations(next);
    },
    [cancelTiming],
  );

  return {
    operations,
    beginOperation: begin,
    updateOperation: update,
    finishOperation: finish,
    clearErrors,
    clearOperation: clear,
    resetOperations: reset,
  };
}
