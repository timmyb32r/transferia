import { describe, expect, it } from "vitest";

import { TaskRegistry } from "../src/application/tasks";

describe("TaskRegistry", () => {
  it("invalidates every task in an editor scope without touching global work", async () => {
    const tasks = new TaskRegistry();
    const editorA = tasks.latest<void, undefined, string>("revision");
    const editorB = tasks.latest<void, undefined, string>("revision");
    const global = tasks.latest<void, undefined, string>("global");
    const editorAResult = editorA.run(undefined, undefined, async () => "a");
    const editorBResult = editorB.run(undefined, undefined, async () => "b");
    const globalResult = global.run(undefined, undefined, async () => "global");

    tasks.cancel("revision");

    await expect(editorAResult).resolves.toBeUndefined();
    await expect(editorBResult).resolves.toBeUndefined();
    await expect(globalResult).resolves.toMatchObject({ value: "global" });
  });
});
