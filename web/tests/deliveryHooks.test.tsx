// @vitest-environment jsdom

import { act, cleanup } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";
import { useDeliveryJobs } from "../src/delivery/useDeliveryJobs";
import { useDeliveryMutations } from "../src/delivery/useDeliveryMutations";
import { useDeliveryPolling } from "../src/delivery/useDeliveryPolling";
import { useDiscovery } from "../src/delivery/useDiscovery";
import { useOperations } from "../src/delivery/useOperations";
import { useYamlEditor } from "../src/delivery/useYamlEditor";
import { LatestJob } from "../src/effects";
import type { EditorState } from "../src/state";
import type {
  DeliveryRecord,
  DeliverySummary,
  DiscoveryResult,
} from "../src/types";
import { renderHook } from "./support/render";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("delivery controllers", () => {
  it("does not let an old operation completion clear a newer operation", () => {
    vi.useFakeTimers();
    const { result } = renderHook(() => useOperations());

    let oldRequest = 0;
    let currentRequest = 0;
    act(() => {
      oldRequest = result.current.beginOperation("save", "old");
      currentRequest = result.current.beginOperation("save", "current");
      result.current.finishOperation("save", oldRequest);
      vi.advanceTimersByTime(200);
    });

    expect(result.current.operations.save).toEqual({
      requestId: currentRequest,
      label: "current",
    });
  });

  it("never flashes progress for fast operations and keeps slow progress readable", () => {
    vi.useFakeTimers();
    const { result } = renderHook(() => useOperations());

    let fastRequest = 0;
    act(() => {
      fastRequest = result.current.beginOperation("parseYaml", "Applying YAML…");
      result.current.finishOperation("parseYaml", fastRequest);
      vi.advanceTimersByTime(1_000);
    });
    expect(result.current.operations.parseYaml).toBeUndefined();

    let slowRequest = 0;
    act(() => {
      slowRequest = result.current.beginOperation("parseYaml", "Applying YAML…");
      vi.advanceTimersByTime(200);
    });
    expect(result.current.operations.parseYaml?.label).toBe("Applying YAML…");

    act(() => {
      result.current.finishOperation("parseYaml", slowRequest);
      vi.advanceTimersByTime(499);
    });
    expect(result.current.operations.parseYaml?.label).toBe("Applying YAML…");

    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(result.current.operations.parseYaml).toBeUndefined();
  });

  it("cancels every editor-scoped latest job as one operation", async () => {
    const { result } = renderHook(() => useDeliveryJobs());
    const context = { sessionId: "session", localRevision: 1 };
    const pending = result.current.yaml.run(
      context,
      {},
      (_input, signal) =>
        new Promise<{ yaml: string }>((resolve) => {
          signal.addEventListener("abort", () => resolve({ yaml: "stale" }), {
            once: true,
          });
        }),
    );

    act(() => result.current.cancelEditorJobs());

    await expect(pending).resolves.toBeUndefined();
  });

  it("polls a clean persisted editor and publishes its captured revision", async () => {
    vi.useFakeTimers();
    const record = delivery();
    const summary: DeliverySummary = {
      id: record.id,
      name: record.name,
      description: record.description,
      revision: record.revision,
      validation: record.validation,
      runtime: record.runtime,
      updated_at_ms: record.updated_at_ms,
    };
    vi.spyOn(api, "deliveries").mockResolvedValue([summary]);
    vi.spyOn(api, "delivery").mockResolvedValue(record);
    const onDeliveries = vi.fn();
    const onRuntime = vi.fn();
    const editor: EditorState = {
      sessionId: "session",
      editing: false,
      id: record.id,
      persistedRevision: 3,
      recordVersion: "4",
      localRevision: 7,
      savedLocalRevision: 7,
      name: record.name,
      description: record.description,
      config: {},
      validation: record.validation,
      runtime: record.runtime,
    };
    const listJob = new LatestJob<void, undefined, DeliverySummary[]>();
    const pollJob = new LatestJob<string, string, DeliveryRecord>();

    renderHook(() =>
      useDeliveryPolling({
        editor,
        listJob,
        pollJob,
        onDeliveries,
        onRuntime,
        onError: vi.fn(),
      }),
    );

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });

    expect(onDeliveries).toHaveBeenCalledWith([summary]);
    expect(onRuntime).toHaveBeenCalledWith("session", 7, record);
  });

  it("waits for a slow polling cycle before scheduling the next one", async () => {
    vi.useFakeTimers();
    let finishList!: (value: DeliverySummary[]) => void;
    const deliveries = vi.spyOn(api, "deliveries").mockImplementation(
      () =>
        new Promise((resolve) => {
          finishList = resolve;
        }),
    );
    const editor = newEditor();
    const listJob = new LatestJob<void, undefined, DeliverySummary[]>();
    const pollJob = new LatestJob<string, string, DeliveryRecord>();

    renderHook(() =>
      useDeliveryPolling({
        editor,
        listJob,
        pollJob,
        onDeliveries: vi.fn(),
        onRuntime: vi.fn(),
        onError: vi.fn(),
      }),
    );

    await act(async () => {
      await vi.advanceTimersByTimeAsync(8_000);
    });
    expect(deliveries).toHaveBeenCalledOnce();

    await act(async () => {
      finishList([]);
      await Promise.resolve();
      await vi.advanceTimersByTimeAsync(1_999);
    });
    expect(deliveries).toHaveBeenCalledOnce();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(deliveries).toHaveBeenCalledTimes(2);
  });

  it("keeps a successful save authoritative when sidebar refresh fails", async () => {
    const saved = delivery();
    vi.spyOn(api, "create").mockResolvedValue(saved);
    vi.spyOn(api, "deliveries").mockRejectedValue(new Error("list offline"));
    const onPersisted = vi.fn();
    const editor = newEditor();
    const { result } = renderHook(() => {
      const jobs = useDeliveryJobs();
      const operations = useOperations();
      return useDeliveryMutations({
        editor,
        jobs,
        operations,
        onDeliveries: vi.fn(),
        onPersisted,
        onRuntime: vi.fn(),
        onDiscovery: vi.fn(),
        isCurrentContext: () => true,
      });
    });

    let resultValue: DeliveryRecord | undefined;
    await act(async () => {
      resultValue = await result.current.save();
    });

    expect(resultValue).toEqual(saved);
    expect(onPersisted).toHaveBeenCalledWith(
      { sessionId: "session", localRevision: 1 },
      saved,
    );
  });

  it("publishes discovery only through the debounced controller", async () => {
    vi.useFakeTimers();
    const discovered = {
      source: "source",
      sink: "sink",
      pipeline_count: 1,
      datasets: [],
      sink_limits: { sink: "sink", supported_arrow_types: [] },
    };
    vi.spyOn(api, "discover").mockResolvedValue(discovered);
    const editor = newEditor();
    const { result } = renderHook(() => {
      const jobs = useDeliveryJobs();
      const operations = useOperations();
      return useDiscovery({
        editor,
        structurallyComplete: true,
        job: jobs.discovery,
        operations,
        isCurrentContext: () => true,
      });
    });

    expect(api.discover).not.toHaveBeenCalled();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(450);
    });

    expect(result.current.discovery).toEqual(discovered);
  });

  it("keeps the previous schema visible while refreshed discovery is pending", async () => {
    vi.useFakeTimers();
    const previous: DiscoveryResult = {
      source: "source",
      sink: "sink",
      pipeline_count: 1,
      datasets: [],
      sink_limits: { sink: "sink", supported_arrow_types: [] },
    };
    let resolveRefresh!: (value: DiscoveryResult) => void;
    const refresh = new Promise<DiscoveryResult>((resolve) => {
      resolveRefresh = resolve;
    });
    vi.spyOn(api, "discover")
      .mockResolvedValueOnce(previous)
      .mockReturnValueOnce(refresh);
    const initial = newEditor();
    const { result, rerender } = renderHook(
      ({
        editor,
        structurallyComplete,
      }: {
        editor: EditorState;
        structurallyComplete: boolean;
      }) => {
        const jobs = useDeliveryJobs();
        const operations = useOperations();
        return useDiscovery({
          editor,
          structurallyComplete,
          job: jobs.discovery,
          operations,
          isCurrentContext: () => true,
        });
      },
      { initialProps: { editor: initial, structurallyComplete: true } },
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(450);
    });
    expect(result.current.discovery).toEqual(previous);

    rerender({
      editor: { ...initial, localRevision: 2 },
      structurallyComplete: true,
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(450);
    });
    expect(result.current.discovery).toEqual(previous);

    await act(async () => {
      resolveRefresh({ ...previous, source: "updated" });
      await refresh;
    });
    expect(result.current.discovery?.source).toBe("updated");

    rerender({
      editor: { ...initial, localRevision: 3 },
      structurallyComplete: false,
    });
    expect(result.current.discovery).toBeUndefined();
  });

  it("round-trips the current YAML draft through its controller", async () => {
    vi.useFakeTimers();
    vi.spyOn(api, "yaml").mockResolvedValue({ yaml: "source: {}" });
    vi.spyOn(api, "parseYaml").mockResolvedValue({
      config: { source: {} },
    });
    const applyConfig = vi.fn();
    const editor = newEditor();
    const { result } = renderHook(() => {
      const jobs = useDeliveryJobs();
      const operations = useOperations();
      return useYamlEditor({
        enabled: true,
        editable: true,
        editor,
        jobs,
        operations,
        isCurrentContext: () => true,
        applyConfig,
      });
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(120);
    });
    await act(async () => result.current.showYaml());
    act(() => result.current.editYaml("source: {}"));
    await act(async () => result.current.applyYamlAndShowUi());

    expect(applyConfig).toHaveBeenCalledWith({ source: {} });
    expect(result.current.activeView).toBe("ui");
  });

  it("switches a read-only YAML view back to UI without parsing it", async () => {
    vi.useFakeTimers();
    vi.spyOn(api, "yaml").mockResolvedValue({ yaml: "source: {}" });
    const parseYaml = vi.spyOn(api, "parseYaml");
    const applyConfig = vi.fn();
    const editor = { ...newEditor(), editing: false };
    const { result } = renderHook(() => {
      const jobs = useDeliveryJobs();
      const operations = useOperations();
      return useYamlEditor({
        enabled: true,
        editable: false,
        editor,
        jobs,
        operations,
        isCurrentContext: () => true,
        applyConfig,
      });
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(120);
    });
    await act(async () => result.current.showYaml());
    await act(async () => result.current.applyYamlAndShowUi());

    expect(result.current.activeView).toBe("ui");
    expect(parseYaml).not.toHaveBeenCalled();
    expect(applyConfig).not.toHaveBeenCalled();
  });
});

function delivery(): DeliveryRecord {
  return {
    id: "delivery",
    name: "Delivery",
    description: "",
    config: {},
    revision: 3,
    record_version: "4",
    validation: { state: "draft" },
    runtime: { state: "stopped" },
    created_at_ms: 1,
    updated_at_ms: 2,
  };
}

function newEditor(): EditorState {
  return {
    sessionId: "session",
    editing: true,
    localRevision: 1,
    name: "Delivery",
    description: "",
    config: {},
    validation: { state: "draft" },
    runtime: { state: "stopped" },
  };
}
