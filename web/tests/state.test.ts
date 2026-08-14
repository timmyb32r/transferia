import { describe, expect, it } from "vitest";

import { editorReducer, isDirty } from "../src/state";

describe("editor state", () => {
  it("tracks persisted and edited revisions independently", () => {
    const fresh = editorReducer(
      {
        editRevision: 0,
        name: "",
        config: {},
        validation: { state: "draft" },
        runtime: { state: "stopped" },
      },
      { type: "new", config: { source: {} } },
    );
    const edited = editorReducer(fresh, { type: "name", name: "orders" });
    expect(isDirty(edited)).toBe(true);

    const saved = editorReducer(edited, {
      type: "persisted",
      delivery: {
        id: "delivery-1",
        name: "orders",
        config: edited.config,
        revision: 1,
        validation: { state: "draft" },
        runtime: { state: "stopped" },
        created_at_ms: 1,
        updated_at_ms: 1,
      },
    });
    expect(isDirty(saved)).toBe(false);
    expect(saved.persistedRevision).toBe(1);
  });
});
