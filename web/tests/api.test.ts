import { afterEach, describe, expect, it, vi } from "vitest";

import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";

describe("control-plane API", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("reports the backend error envelope message exactly", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: { code: "conflict", message: "Draft changed" },
          }),
          { status: 409, headers: { "content-type": "application/json" } },
        ),
      ),
    );

    await expect(api.delivery("delivery/1")).rejects.toThrow("Draft changed");
    expect(fetch).toHaveBeenCalledWith(
      "/api/v1/deliveries/delivery%2F1",
      expect.anything(),
    );
  });

  it("preserves a non-JSON upstream error body", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response("gateway unavailable", {
          status: 502,
          statusText: "Bad Gateway",
        }),
      ),
    );

    await expect(api.deliveries()).rejects.toThrow("gateway unavailable");
  });

  it("uses the HTTP status when an error response has no body", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response(null, { status: 503, statusText: "Unavailable" }),
        ),
    );

    await expect(api.deliveries()).rejects.toThrow("503 Unavailable");
  });

  it("sends JSON mutations with an exact content type and body", async () => {
    const record = delivery("delivery-1", "Saved");
    const request = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify(record), { status: 200 }));
    vi.stubGlobal("fetch", request);

    await api.create("Saved", "Description", { source: {} });

    const [, init] = request.mock.calls[0]! as [string, RequestInit];
    expect(init.method).toBe("POST");
    expect(new Headers(init.headers).get("content-type")).toBe(
      "application/json",
    );
    expect(init.body).toBe(
      JSON.stringify({
        name: "Saved",
        description: "Description",
        config: { source: {} },
      }),
    );
  });

  it("sends dependent option context in the request body", async () => {
    const request = vi
      .fn()
      .mockResolvedValue(
        new Response(JSON.stringify({ options: [] }), { status: 200 }),
      );
    vi.stubGlobal("fetch", request);

    await api.options({
      key: "databases",
      query: "cdc/pro",
      dependencies: { cluster_id: "mdb1" },
    });

    expect(request).toHaveBeenCalledOnce();
    const [path, init] = request.mock.calls[0] as [string, RequestInit];
    expect(path).toBe("/api/v1/options/databases");
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({
      refresh: false,
      query: "cdc/pro",
      dependencies: { cluster_id: "mdb1" },
    });
  });

  it("runs SQL playground samples through the typed API contract", async () => {
    const request = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          columns: [
            {
              name: "id",
              arrow_type: "Int64",
              nullable: false,
              primary_key: false,
              low_cardinality: false,
            },
          ],
          rows: [{ id: 6 }],
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", request);

    await expect(
      api.sqlPlayground({
        sql: "SELECT id * 2 AS id FROM input",
        rows: [{ id: 3 }],
      }),
    ).resolves.toMatchObject({ rows: [{ id: 6 }] });
    expect(request).toHaveBeenCalledWith(
      "/api/v1/playground/sql",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("sends endpoint credentials only in the connection-check POST body", async () => {
    const request = vi
      .fn()
      .mockResolvedValue(
        new Response(
          JSON.stringify({ status: "verified", message: null, options: {} }),
          { status: 200 },
        ),
      );
    vi.stubGlobal("fetch", request);

    await api.checkConnection({
      connector: "clickhouse",
      role: "sink",
      config: { username: "user", password: "secret" },
    });

    expect(request).toHaveBeenCalledWith(
      "/api/v1/check-connection",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          connector: "clickhouse",
          role: "sink",
          config: { username: "user", password: "secret" },
        }),
      }),
    );
  });

  it("binds stop to both the record and worker run", async () => {
    const record = delivery("delivery-1", "Saved");
    const request = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify(record), { status: 200 }));
    vi.stubGlobal("fetch", request);

    await api.stop("delivery-1", 7, "11", "run-a");

    const [, init] = request.mock.calls[0]! as [string, RequestInit];
    expect(init.body).toBe(
      JSON.stringify({
        expected_revision: 7,
        expected_record_version: "11",
        expected_run_id: "run-a",
      }),
    );
  });

  it("deletes with the current optimistic-concurrency token", async () => {
    const record = delivery("delivery-1", "Saved");
    const request = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify(record), { status: 200 }));
    vi.stubGlobal("fetch", request);

    await api.delete("delivery-1", 7, "11");

    expect(request).toHaveBeenCalledWith(
      "/api/v1/deliveries/delivery-1",
      expect.objectContaining({
        method: "DELETE",
        body: JSON.stringify({
          expected_revision: 7,
          expected_record_version: "11",
        }),
      }),
    );
  });

  it("rejects malformed successful responses at the network boundary", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            ...delivery("delivery-1", "Saved"),
            record_version: 1,
          }),
          { status: 200 },
        ),
      ),
    );

    await expect(api.delivery("delivery-1")).rejects.toThrow(
      /record_version: expected string/,
    );
  });

  it("rejects response fields that are absent from the Rust DTO", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            ...delivery("delivery-1", "Saved"),
            legacy_status: "active",
          }),
          { status: 200 },
        ),
      ),
    );

    await expect(api.delivery("delivery-1")).rejects.toThrow(
      /legacy_status: unknown field/,
    );
  });
});

function delivery(id: string, name: string) {
  return {
    id,
    name,
    description: "",
    revision: 1,
    validation: { state: "draft" as const },
    runtime: { state: "stopped" as const },
    record_version: "1",
    config: {},
    created_at_ms: 1,
    updated_at_ms: 1,
  };
}
