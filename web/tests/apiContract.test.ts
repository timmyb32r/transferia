import { describe, expect, it } from "vitest";

import fixture from "../../contracts/server-api.fixture.json";
import { decodeApi } from "../src/api/contractDecoder";

describe("generated Rust server API contract", () => {
  it("decodes the shared Rust serialization fixture", () => {
    const delivery = decodeApi(
      "delivery_response",
      fixture.delivery_record,
      "fixture.delivery_record",
    );
    const discovery = decodeApi(
      "discovery_response",
      fixture.discovery_result,
      "fixture.discovery_result",
    );
    const error = decodeApi(
      "error_response",
      fixture.error_envelope,
      "fixture.error_envelope",
    );

    expect(delivery.runtime).toEqual({
      state: "running",
      run_id: "run-7",
      pid: 42,
    });
    expect(discovery.datasets[0]?.columns[1]).not.toHaveProperty("max_length");
    expect(error.error.code).toBe("not_found");
  });

  it("rejects a response that violates a generated Rust DTO schema", () => {
    expect(() =>
      decodeApi(
        "delivery_response",
        { ...fixture.delivery_record, record_version: 11 },
        "delivery",
      ),
    ).toThrow("delivery.record_version");
  });
});
