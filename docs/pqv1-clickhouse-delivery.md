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

Connection and request deadlines are configurable, but the bundled native
client performs a blocking TCP connect with its own 30-second bound. A smaller
configured connect timeout therefore is not a strict wall-clock interrupt.
TLS is rejected because this client cannot verify server certificates; use a
verified local TLS tunnel when encryption is required.

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
