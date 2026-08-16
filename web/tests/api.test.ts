import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../src/api";

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
            error: { code: "revision_conflict", message: "Draft changed" },
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

  it("binds stop to both the record and worker run", async () => {
    const record = delivery("delivery-1", "Saved");
    const request = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify(record), { status: 200 }));
    vi.stubGlobal("fetch", request);

    await api.stop("delivery-1", 7, 11, "run-a");

    const [, init] = request.mock.calls[0]! as [string, RequestInit];
    expect(init.body).toBe(
      JSON.stringify({
        expected_revision: 7,
        expected_record_version: 11,
        expected_run_id: "run-a",
      }),
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
    record_version: 1,
    config: {},
    created_at_ms: 1,
    updated_at_ms: 1,
  };
}
