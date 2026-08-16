import { describe, expect, it } from "vitest";

import {
  closestArrowType,
  isStringArrowType,
  parsePartitionIds,
  reconcileSystemColumnKeys,
} from "../src/schema/formLogic";

describe("schema form logic", () => {
  it("expands validated partition lists and inclusive ranges", () => {
    expect(parsePartitionIds("1-5,7")).toEqual({
      value: [1, 2, 3, 4, 5, 7],
    });
    expect(parsePartitionIds("3-1").error).toMatch(/ends before/);
    expect(parsePartitionIds("1,1").error).toMatch(/selected twice/);
    expect(parsePartitionIds("1-").error).toMatch(/Invalid partition range/);
  });

  it("selects a lossless default Arrow type for each JSON type", () => {
    expect(closestArrowType("string")).toBe("Utf8");
    expect(closestArrowType("number")).toBe("Float64");
    expect(closestArrowType("boolean")).toBe("Boolean");
    expect(isStringArrowType("Utf8")).toBe(true);
    expect(isStringArrowType("Int64")).toBe(false);
  });

  it("keeps parser keys referentially consistent with system columns", () => {
    expect(
      reconcileSystemColumnKeys(
        { topic: "_system_topic", partition: "_system_partition" },
        { topic: "topic_name", partition: null },
        ["id", "_system_topic", "_system_partition"],
      ),
    ).toEqual(["id", "topic_name"]);
  });
});
