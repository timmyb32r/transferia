# Measurement coverage

This is the predeclared matrix. Missing and failed cases have no throughput value.

| Phase | Expected | Verified | Failed | Not run |
|---|---:|---:|---:|---:|
| primary | 264 | 264 | 0 | 0 |
| p16 | 6 | 6 | 0 | 0 |
| scale | 44 | 24 | 2 | 18 |

## Incomplete cases

| Phase | Route | Tool | Dataset | Parts | Repetition | Status |
|---|---|---|---|---:|---:|---|
| scale | pg-pg | rust | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | rust_ranges | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | go | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | seatunnel | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | sling | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | datax | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | airbyte | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | flink | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | debezium | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | debezium_bulk | narrow10m | 1 | 1 | failed |
| scale | pg-pg | inlong | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | meltano | narrow10m | 1 | 1 | not_run |
| scale | pg-pg | sqoop | narrow10m | 1 | 1 | failed |
| scale | pg-pg | estuary | narrow10m | 1 | 1 | not_run |
| scale | pg-ch | rust | narrow10m | 1 | 1 | not_run |
| scale | pg-ch | rust_ranges | narrow10m | 1 | 1 | not_run |
| scale | pg-ch | go | narrow10m | 1 | 1 | not_run |
| scale | pg-ch | sling | narrow10m | 1 | 1 | not_run |
| scale | pg-ch | spark | narrow10m | 1 | 1 | not_run |
| scale | pg-ch | datax | narrow10m | 1 | 1 | not_run |

PG→CH was scoped to six products (plus the separately labeled Rust exact-range variant). No claim is made that the other seven products lack ClickHouse support; they have no measured adapter in this campaign.
