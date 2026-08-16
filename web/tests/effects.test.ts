import { describe, expect, it } from "vitest";

import { LatestJob } from "../src/effects";

describe("LatestJob", () => {
  it("never publishes an older response after a newer request starts", async () => {
    const job = new LatestJob<number, number, number>();
    let finishOld: ((value: number) => void) | undefined;
    const old = job.run(
      1,
      1,
      () =>
        new Promise((resolve) => {
          finishOld = resolve;
        }),
    );
    const latest = job.run(2, 2, async (value) => value);
    finishOld?.(1);
    await expect(old).resolves.toBeUndefined();
    await expect(latest).resolves.toEqual({
      requestId: 2,
      context: 2,
      value: 2,
    });
  });

  it("suppresses an older rejection after a newer request starts", async () => {
    const job = new LatestJob<string, undefined, string>();
    let rejectOld: ((reason: Error) => void) | undefined;
    const old = job.run(
      "old-session",
      undefined,
      () =>
        new Promise((_, reject) => {
          rejectOld = reject;
        }),
    );
    const latest = job.run("new-session", undefined, async () => "latest");
    rejectOld?.(new Error("stale failure"));

    await expect(old).resolves.toBeUndefined();
    await expect(latest).resolves.toMatchObject({
      context: "new-session",
      value: "latest",
    });
  });

  it("still reports a failure from the latest request", async () => {
    const job = new LatestJob<string, undefined, string>();

    await expect(
      job.run("current", undefined, async () => {
        throw new Error("current failure");
      }),
    ).rejects.toThrow("current failure");
  });
});
