import { describe, expect, it } from "vitest";

import fixture from "../../contracts/server-api.json";
import type {
  ApiErrorEnvelope,
  DeliveryRecord,
  DiscoveryResult,
  RuntimeState,
} from "../src/types";

type Exact<Left, Right> = [Left] extends [Right]
  ? [Right] extends [Left]
    ? true
    : false
  : false;

type ExpectedRuntimeState =
  | { state: "stopped" }
  | { state: "starting"; run_id: string }
  | { state: "running"; run_id: string; pid: number }
  | { state: "stopping"; run_id: string }
  | { state: "failed"; run_id: string; message: string };

type ExpectedDeliveryRecord = {
  id: string;
  name: string;
  description: string;
  revision: number;
  record_version: number;
  validation:
    | { state: "draft" }
    | { state: "ready"; revision: number }
    | { state: "invalid"; revision: number; message: string };
  runtime: ExpectedRuntimeState;
  updated_at_ms: number;
  config: import("../src/types").JsonObject;
  created_at_ms: number;
};

type ExpectedDiscoveryResult = {
  source: string;
  sink: string;
  datasets: Array<{
    role: "Main" | "DeadLetterQueue";
    name: string;
    columns: Array<{
      name: string;
      arrow_type: string;
      nullable: boolean;
      primary_key: boolean;
      low_cardinality: boolean;
      max_length?: number;
    }>;
  }>;
  sink_limits: {
    sink: string;
    dataset_name: {
      syntax:
        | "any_non_empty_utf8"
        | "ascii_identifier"
        | "object_store_path_segment";
      max_utf8_bytes: number | null;
    } | null;
    column_name: {
      syntax:
        | "any_non_empty_utf8"
        | "ascii_identifier"
        | "object_store_path_segment";
      max_utf8_bytes: number | null;
    } | null;
    supported_arrow_types: Array<
      | "utf8"
      | "binary"
      | "signed_integer"
      | "unsigned_integer"
      | "floating_point"
      | "boolean"
      | "date32"
      | "date64"
      | "timestamp"
    >;
    object_key: {
      max_utf8_bytes: number;
      normalized_relative_path: boolean;
    } | null;
  };
};

type ExpectedErrorEnvelope = {
  error: {
    code:
      | "invalid_request"
      | "payload_too_large"
      | "not_found"
      | "conflict"
      | "validation_failed"
      | "internal_error";
    message: string;
  };
};

const exactTypeAssertions: {
  runtime: Exact<RuntimeState, ExpectedRuntimeState>;
  delivery: Exact<DeliveryRecord, ExpectedDeliveryRecord>;
  discovery: Exact<DiscoveryResult, ExpectedDiscoveryResult>;
  error: Exact<ApiErrorEnvelope, ExpectedErrorEnvelope>;
} = {
  runtime: true,
  delivery: true,
  discovery: true,
  error: true,
};

describe("Rust server API contract", () => {
  it("shares a concrete fixture with Rust serialization tests", () => {
    expect(exactTypeAssertions).toEqual({
      runtime: true,
      delivery: true,
      discovery: true,
      error: true,
    });
    expect(fixture.delivery_record.runtime).toEqual({
      state: "running",
      run_id: "run-7",
      pid: 42,
    });
    expect(fixture.discovery_result.datasets[0]?.columns[1]).not.toHaveProperty(
      "max_length",
    );
    expect(fixture.error_envelope.error.code).toBe("not_found");
  });
});
