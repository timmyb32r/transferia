# PostgreSQL, MySQL, and OpenSearch throughput tournament

Measured on the dedicated benchmark host on 2026-09-03. The source and
destination ceilings were measured independently by the delivery Speedtest in
one logical stream. The short grid selected finalists; each reported finalist
is the arithmetic mean of three 10-second confirmation windows. Raw API
responses are in `results/20260903/`.

The fixture contained 2,000,000 deterministic rows in local PostgreSQL 17.6
and MariaDB 10.6 containers. OpenSearch 2.19.1 contained 200,000 immutable
documents in an index with four primary shards and no replicas. The host-side
pipeline memory limit was 512 MiB.

## Result

| Endpoint | Earlier default | Earlier rows/s | Selected default | Selected rows/s | Gain |
|---|---:|---:|---:|---:|---:|
| PostgreSQL source | binary COPY, 65,536 rows | 1,221,945 | unchanged | 1,221,945 | 0.0% |
| PostgreSQL sink | binary COPY | 479,760 | unchanged | 479,760 | 0.0% |
| MySQL source | text protocol, 65,536 rows | 1,053,779 | binary protocol, 16,384 rows | 1,641,759 | +55.8% |
| MySQL sink | 1,000 rows/INSERT | 212,060 | unchanged | 212,060 | 0.0% |
| OpenSearch source | 10,000 rows/page, concurrency 4 | 1,083,432 | unchanged | 1,083,432 | 0.0% |
| OpenSearch sink | 10,000 rows/bulk, concurrency 4 | 53,571 | 20,000 rows/bulk, concurrency 8 | 56,689 | +5.8% |

PostgreSQL binary COPY remained the clear sink winner: the 65,536-row source
setting was stable, while reducing it to 16,384 changed sink throughput by only
about 2% and did not justify more flushes. Text `COPY FROM` was materially
slower. Both formats remain explicit advanced choices.

MySQL's prepared-statement binary result protocol produced exactly the same
Arrow schema and values as the text protocol across the all-types MySQL and
MariaDB E2E fixture, while increasing source throughput by 45–58% across the
grid. Three confirmations at 16,384 rows averaged 1,641,759 rows/s with 1.3%
relative standard deviation; 65,536 rows averaged 1,551,572 rows/s. The sink
knee remained 1,000 rows/INSERT. Larger 12,000–16,000-row statements were
slower and were rejected as defaults.

OpenSearch rejected search pages above its ordinary 10,000-row
`index.max_result_window`; those points are invalid, not slow, and are excluded
from the tuning domain. The 10,000/4 source default averaged 1,083,432 rows/s
and beat 5,000/4 by 5.9% in confirmations. Concurrency above the four primary
shards cannot create more useful readers.

For the OpenSearch sink, 2,500 rows with concurrency 8 reached the raw maximum
of 59,684 rows/s, but required eight times as many bulk requests per row as the
20,000-row candidate for only 5.3% more throughput. The selected 20,000/8
setting averaged 56,689 rows/s, improved the previous default by 5.8%, and
halved bulk request rate at the measured throughput. The service reported no
search or write rejections, an empty queue, 31% heap use, and no old-generation
GC after the tournament.

## Reproduction

`setup_opensearch_fixture.py` creates an exclusive deterministic fixture,
verifies its row count, and write-blocks it before measurement.
`run_server_tournament.py` contains the exact endpoint documents and bounded
candidate sets. Credentials are accepted only through
`TRANSFERIA_BENCH_PASSWORD`; result files never store them.
