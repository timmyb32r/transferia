import { describe, expect, it } from "vitest";

import { editorReducer, isDirty } from "../src/state";

describe("editor state", () => {
  it("tracks persisted and edited revisions independently", () => {
    const fresh = editorReducer(
      {
        sessionId: "initial",
        localRevision: 0,
        name: "",
        description: "",
        config: {},
        validation: { state: "draft" },
        runtime: { state: "stopped" },
      },
      { type: "new", sessionId: "new", config: { source: {} } },
    );
    const edited = editorReducer(fresh, { type: "name", name: "orders" });
    expect(isDirty(edited)).toBe(true);

    const saved = editorReducer(edited, {
      type: "persisted",
      sessionId: edited.sessionId,
      savedLocalRevision: edited.localRevision,
      delivery: {
        id: "delivery-1",
        name: "orders",
        description: "",
        config: edited.config,
        revision: 1,
        validation: { state: "draft" },
        runtime: { state: "stopped" },
        record_version: "1",
        created_at_ms: 1,
        updated_at_ms: 1,
      },
    });
    expect(isDirty(saved)).toBe(false);
    expect(saved.persistedRevision).toBe(1);
  });

  it("marks only the submitted snapshot as saved", () => {
    const state = {
      sessionId: "editor-1",
      localRevision: 2,
      savedLocalRevision: 0,
      name: "newer local name",
      description: "",
      config: {},
      validation: { state: "draft" as const },
      runtime: { state: "stopped" as const },
    };

    const saved = editorReducer(state, {
      type: "persisted",
      sessionId: "editor-1",
      savedLocalRevision: 1,
      delivery: delivery("delivery-1", 1),
    });

    expect(saved.savedLocalRevision).toBe(1);
    expect(isDirty(saved)).toBe(true);
    expect(saved.name).toBe("newer local name");
  });

  it("ignores responses from an older editor session", () => {
    const state = {
      sessionId: "editor-2",
      localRevision: 0,
      savedLocalRevision: 0,
      id: "delivery-2",
      persistedRevision: 4,
      name: "current",
      description: "",
      config: {},
      validation: { state: "draft" as const },
      runtime: { state: "stopped" as const },
    };

    const next = editorReducer(state, {
      type: "runtime",
      sessionId: "editor-1",
      expectedLocalRevision: 0,
      delivery: delivery("delivery-1", 9),
    });

    expect(next).toBe(state);
  });

  it("ignores an older record version with the same config revision", () => {
    const state = {
      sessionId: "editor-2",
      localRevision: 0,
      savedLocalRevision: 0,
      id: "delivery-2",
      persistedRevision: 4,
      recordVersion: "8",
      name: "current",
      description: "",
      config: {},
      validation: { state: "ready" as const, revision: 4 },
      runtime: { state: "running" as const, run_id: "run-2", pid: 42 },
    };
    const stale = delivery("delivery-2", 4);
    stale.record_version = "7";

    const next = editorReducer(state, {
      type: "runtime",
      sessionId: "editor-2",
      expectedLocalRevision: 0,
      delivery: stale,
    });

    expect(next).toBe(state);
  });

  it("ignores runtime responses for an older local revision", () => {
    const state = {
      sessionId: "editor-2",
      localRevision: 3,
      savedLocalRevision: 2,
      id: "delivery-2",
      persistedRevision: 4,
      name: "current",
      description: "",
      config: {},
      validation: { state: "draft" as const },
      runtime: { state: "stopped" as const },
    };

    const next = editorReducer(state, {
      type: "runtime",
      sessionId: "editor-2",
      expectedLocalRevision: 2,
      delivery: delivery("delivery-2", 5),
    });

    expect(next).toBe(state);
  });
});

function delivery(id: string, revision: number) {
  return {
    id,
    name: "saved",
    description: "",
    config: {},
    revision,
    validation: { state: "draft" as const },
    runtime: { state: "stopped" as const },
    record_version: "1",
    created_at_ms: 1,
    updated_at_ms: 1,
  };
}
