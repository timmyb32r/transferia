import { useCallback, useRef, useState } from "preact/hooks";

import type { OperationKey, OperationState } from "../application/operations";

export function useOperations() {
  const [operations, setOperations] = useState<
    Partial<Record<OperationKey, OperationState>>
  >({});
  const sequence = useRef(0);

  const begin = useCallback((key: OperationKey, label?: string): number => {
    const requestId = ++sequence.current;
    setOperations((current) => ({
      ...current,
      [key]: { requestId, ...(label === undefined ? {} : { label }) },
    }));
    return requestId;
  }, []);

  const finish = useCallback(
    (key: OperationKey, requestId: number, error?: string) => {
      setOperations((current) => {
        if (current[key]?.requestId !== requestId) return current;
        if (error !== undefined)
          return { ...current, [key]: { requestId, error } };
        const next = { ...current };
        delete next[key];
        return next;
      });
    },
    [],
  );

  const clearErrors = useCallback(() => {
    setOperations((current) =>
      Object.fromEntries(
        Object.entries(current).filter(([, operation]) => !operation?.error),
      ),
    );
  }, []);

  const clear = useCallback((key: OperationKey) => {
    setOperations((current) => {
      if (current[key] === undefined) return current;
      const next = { ...current };
      delete next[key];
      return next;
    });
  }, []);

  return {
    operations,
    beginOperation: begin,
    finishOperation: finish,
    clearErrors,
    clearOperation: clear,
    resetOperations: setOperations,
  };
}
