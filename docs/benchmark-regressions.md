# September 19 connector regressions

The competitor comparison exposed five Transferia failures. These fixes cover
the connector contracts behind them; they do not constitute a rerun of the
performance comparison.

| Observed failure | Current contract | Regression |
| --- | --- | --- |
| PostgreSQL CDC: prepared statement disappeared through a transaction pooler | Idle requests send Parse/Bind/Execute together; metadata and temporary plugin probes stay within a transaction | `e2e_transaction_pooling` resets backend session state at every idle protocol boundary and first reproduces SQLSTATE 26000 with the old request pattern |
| MySQL TLS: ambiguous rustls crypto provider panic | The vendored driver selects its enabled backend locally for both client and certificate verifier | `e2e_tls` connects to MySQL with both crypto backends enabled and no process default; untrusted certificates and hostname mismatches still fail |
| PostgreSQL NUMERIC sent as text bytes into binary NUMERIC COPY | Constrained NUMERIC uses exact Decimal128 coefficients and PostgreSQL's base-10000 wire format | `e2e_copy_formats` round-trips both COPY formats, domains, NULL, fractions, negative scale and the maximum unsigned 64-bit integer |
| ClickHouse Decimal column rejected a PostgreSQL string projection | Supported PostgreSQL NUMERIC retains its decimal type through discovery, snapshot and CDC | `sink_temporal_e2e` checks exact Decimal128 output through Native, Parquet and ArrowStream |
| Naive timestamps displayed three or four hours later in ClickHouse | Explicit upstream timezone conversion is required; naive values fail discovery and runtime validation | Unit and real-service temporal tests; see [timestamp contract](clickhouse-timestamps.md) |

The new edge-case tests also exposed a native ClickHouse INSERT race: encoding
started before the server supplied its column header. The client now waits for
that header, preserving DateTime64 at second precision as well as decimal types.
Local encoding failures retain their permanent error type instead of being
masked as retryable connection failures. The service test verifies zero added
rows after a rejected block.

## Numeric boundaries

Native PostgreSQL NUMERIC support requires a declared precision from 1 to 38 and
a scale representable by Arrow Decimal128 (i8, no greater than precision).
Domains retain their effective typmod. Unbounded, wider or otherwise unsupported
NUMERIC needs the existing explicit batch `unsupported_types: to_string` policy;
strict discovery and CDC reject it. When discovery selects Decimal128, NaN and
infinities cannot be represented and fail rather than becoming NULL. The batch
text path for unsupported NUMERIC retains their PostgreSQL spelling. Parsing
never passes through floating point. Snapshot COPY still uses PostgreSQL's text
projection internally, then decodes the exact coefficient and declared scale.
CDC preserves numeric JSON literals and requests wal2json numeric strings so
non-finite values reach validation instead of being replaced by JSON null.

ClickHouse supports this decimal path only for nonnegative scales. Existing
ClickHouse Decimal(P,S) destinations must have the same scale and at least the
source precision; their 32/64/128-bit storage width is not substituted for P.
The runtime boundary also rejects a coefficient outside its declared precision
before buffering or INSERT, including when an Arrow array has valid type
metadata but invalid values.

An existing PostgreSQL destination must accept the declared source domain
without rounding or implicit conversion. In particular, Utf8 cannot target
NUMERIC, a numeric scale cannot narrow, and timestamp precision cannot decrease.
The check runs during preparation and again under a relation lock in the write
transaction. Contract failures produce no commit acknowledgement; transient
transport errors retain the existing retry classification.

## Ownership and cost

CDC ownership uses transaction-scoped advisory locks on one additional dedicated
connection. A READ COMMITTED transaction pins that backend through transaction
pooling without retaining a data snapshot or row locks between requests. Reads
and acknowledgements check ownership. A lost lease fails closed and is never
silently reacquired. A server idle-in-transaction timeout can terminate this
connection; no server or pooler settings are changed automatically.

PostgreSQL destination validation adds a lock and catalog request per distinct
destination per delivery, held through commit to prevent concurrent DDL from
invalidating the checked types. These are correctness costs; throughput has not
been rebenchmarked. The session-reset fixture proves the exercised protocol
contract, not compatibility with every PgBouncer/Odyssey configuration.

## Focused verification

The service tests require Docker and fail when it is unavailable; none are
silently skipped. Ordinary development completion still uses `just check-affected`.

```sh
cargo test -p transferia-connector-mysql --test e2e_tls
cargo test -p transferia-connector-postgres --lib --test e2e_copy_formats --test e2e_transaction_pooling --test e2e_replication --test e2e_snapshot_and_replication
cargo test -p transferia-connector-clickhouse --lib --test sink_temporal_e2e
cargo test -p transferia-registry --lib
cargo test --test clickbench_clickhouse_contract
cd web && ./node_modules/.bin/vitest run tests/catalogReadiness.test.tsx tests/appearance.test.tsx
```

The catalog tests also cover `parallel_table_snapshot` as a typed source
capability. Placing it in the generic component-properties list had previously
made endpoint registration fail; the frontend and backend now enforce the same
capability contract while retaining the About-page property.
