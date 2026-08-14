import { describe, expect, it } from "vitest";

import { LatestJob } from "../src/effects";

describe("LatestJob", () => {
  it("never publishes an older response after a newer request starts", async () => {
    const job = new LatestJob<number, number>();
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
    await expect(latest).resolves.toEqual({ revision: 2, value: 2 });
  });
});
