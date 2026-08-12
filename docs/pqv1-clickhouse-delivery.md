# PQv1 → ClickHouse delivery semantics

## Current runtime contract

The implemented PQv1 → ClickHouse path is **at-least-once**, not exactly-once.
ClickHouse reports a successful INSERT before the source cookie is committed. If
the process loses the commit response or stops between those operations, the
same source data can be inserted again after restart.

The runtime currently sends ordinary `INSERT ... VALUES` requests. It does not
set `insert_deduplication_token`, maintain a watermark table, issue a DELETE
before INSERT, or configure `non_replicated_deduplication_window`. The
compatibility report printed at startup is the authoritative description of
this guarantee.

Consequences for operators:

- destination tables and downstream queries must tolerate duplicates;
- ambiguous ClickHouse transport failures are retried and can duplicate rows;
- source offsets are committed only after ClickHouse reports success, so the
  pipeline prefers duplicates over data loss;
- changing parser or table configuration while uncommitted data can replay may
  change the rows produced by that replay.

Existing destination tables must use exactly `MergeTree` or
`ReplicatedMergeTree`. Transforming engines such as `ReplacingMergeTree`,
`SummingMergeTree`, `CollapsingMergeTree`, and `AggregatingMergeTree` are
rejected because a successful INSERT followed by a background merge can change
or remove source rows.

The current DLQ stores the lossless source payload in `raw_base64`. Deployments
with the historical `raw_bytes` DLQ must create a new empty DLQ table (or move
the old one aside) after replay into the old layout is impossible. Renaming
`raw_bytes` to `raw_base64` is invalid because historical values were not base64.

Connection and request deadlines are configurable. The connect timeout is a
strict caller deadline and never blocks a Tokio worker. The bundled native
client's socket connect cannot be cancelled, so its current socket call may
continue for up to another 30 seconds on the blocking pool. An internal deadline
then stops the remaining connection work, and reconnects reuse that single
in-flight attempt. TLS is rejected because this client cannot verify server
certificates; use a verified local TLS tunnel when encryption is required.

## Possible exactly-once designs

The following are design options only; none is implemented:

1. Stable source identity columns plus a persistent ClickHouse watermark.
2. A deterministic insert token backed by a ClickHouse deduplication window
   whose retention is part of the operating contract.
3. An idempotent staging/merge protocol with persistent batch state.

Any implementation must cover main and DLQ writes atomically from the source's
point of view, define recovery after every ambiguous response, validate the
existing table schema, and include crash/replay integration tests. Until those
conditions hold, documentation and compatibility checks must continue to call
the path at-least-once.
