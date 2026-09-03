import { describe, expect, test } from "vitest";

import { murmur3X64_128 } from "../scripts/murmur3-x64-128.mjs";

const bytes = (value) => new TextEncoder().encode(value);

describe("Murmur3 x64 128 asset fingerprints", () => {
  test("matches the Rust murmur3 reference vectors", () => {
    expect(murmur3X64_128(bytes(""))).toBe("00000000000000000000000000000000");
    expect(murmur3X64_128(bytes("hello"))).toBe("5b1e906a48ae1d19cbd8a7b341bd9b02");
    expect(murmur3X64_128(bytes("hello world"))).toBe(
      "ab97467d60eb63b1533f6046eb7f610e",
    );
  });

  test("fingerprints arbitrary bytes without text conversion", () => {
    const payload = Uint8Array.from({ length: 257 }, (_, index) => index & 0xff);
    expect(murmur3X64_128(payload)).toBe("4a7d408be569a1f8096d5f99f9897da6");
  });
});
