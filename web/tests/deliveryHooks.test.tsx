// @vitest-environment jsdom

import { act, cleanup, renderHook } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../src/api";
import { useDeliveryJobs } from "../src/delivery/useDeliveryJobs";
import { useDeliveryMutations } from "../src/delivery/useDeliveryMutations";
import { useDeliveryPolling } from "../src/delivery/useDeliveryPolling";
import { useDiscovery } from "../src/delivery/useDiscovery";
import { useOperations } from "../src/delivery/useOperations";
import { useYamlEditor } from "../src/delivery/useYamlEditor";
import { LatestJob } from "../src/effects";
import type { EditorState } from "../src/state";
import type { DeliveryRecord, DeliverySummary } from "../src/types";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("delivery controllers", () => {
  it("does not let an old operation completion clear a newer operation", () => {
    const { result } = renderHook(() => useOperations());

    let oldRequest = 0;
    let currentRequest = 0;
    act(() => {
      oldRequest = result.current.beginOperation("save", "old");
      currentRequest = result.current.beginOperation("save", "current");
      result.current.finishOperation("save", oldRequest);
    });

    expect(result.current.operations.save).toEqual({
      requestId: currentRequest,
      label: "current",
    });
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
      }),
    );

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });

    expect(onDeliveries).toHaveBeenCalledWith([summary]);
    expect(onRuntime).toHaveBeenCalledWith("session", 7, record);
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
