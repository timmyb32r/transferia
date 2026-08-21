import { describe, expect, it } from "vitest";

import fixture from "../../crates/transferia-server-contracts/contracts/server-api.fixture.json";
import routes from "../../crates/transferia-server-contracts/contracts/server-api.routes.json";
import { decodeApi } from "../src/api/contractDecoder";
import { API_ROUTES } from "../src/generated/apiContract";

describe("generated Rust server API contract", () => {
  it("decodes the shared Rust serialization fixture", () => {
    const catalog = decodeApi(
      "catalog_response",
      fixture.catalog,
      "fixture.catalog",
    );
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

    expect(catalog.connectors[0]?.sink?.connection_check).toBe(true);
    expect(delivery.runtime).toEqual({
      state: "running",
      run_id: "run-7",
      pid: 42,
    });
    expect(discovery.datasets[0]?.intermediate_columns[1]).not.toHaveProperty(
      "max_length",
    );
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

  it("generates every frontend method, path, and response from the Rust route manifest", () => {
    expect(API_ROUTES).toEqual(
      Object.fromEntries(
        routes.map(({ name, ...route }) => [name, route]),
      ),
    );
  });
});
