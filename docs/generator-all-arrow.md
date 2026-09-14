# All Arrow datatypes generator preset

Select **All Arrow datatypes** (`preset: {type: all_arrow_datatypes}`) to generate
a deterministic type-coverage table: a unique UInt64 `id` and representative
columns for every Arrow 57 datatype family except **Null**. The Null type is
deliberately excluded because it has no typed values; nullable columns are a
separate concept. This is a compatibility fixture,
not a realistic performance distribution or a promise that every sink supports
these types. Normal discovery rejects incompatible destinations before writing.

Coverage includes all integer/float/decimal widths, boolean, binary
and string offset/view layouts, fixed binary, dates, every valid time unit,
timestamps with and without UTC, durations, all three interval layouts, lists
(ordinary/large/fixed/view), struct, sparse/dense union, dictionary, map and
run-end encoding. Parameterized types use concrete examples, not every possible
precision, timezone, child type or nesting combination.

Every column has a non-null representative value.
Numeric/temporal samples use exactly representable zero; strings, intervals and
nested values are populated. Values repeat; only `id` advances with `start_row`.
The fixed logical benchmark width is 4096 bytes per row (including conservative
buffer allowance), not a serialized payload size. Data-size amounts must be
divisible by this width, as for other fixed-width generator presets. Memory
reservation additionally includes 32 KiB per batch for array objects and seeds.

## Whole-preset sink scenarios

About → Destination types → Unsupported types lists concrete rejections from
the production type resolver, including its exact reason. The catalog and this
preset share `transferia_registry::arrow_examples::types`; there is no separate
handwritten unsupported-type list. JSON transport results are explicitly labelled
JSON and do not describe other serializers or runtime value constraints.

`cargo test --test e2e_all_arrow_sinks --all-features` covers every registered
public sink, starting from production configuration parsing, installation
resolution and generator discovery. A catalog guard requires a scenario for
any newly registered sink.

Discard runs all 17 rows through the real source/parser/pipeline/sink and checks
the consumed row count. ClickHouse, Kafka, Logbroker, MySQL, OpenSearch,
PostgreSQL, YDB and YTsaurus currently reject Float16 during sink validation;
Iceberg rejects Date64. These are the first unsupported types in the full
preset, not an exhaustive list of each sink's limitations. S3 must reject the
missing source-coordinate system columns. No sink scenario tests the Null type.
A listening socket fails the negative scenarios on any destination connection.
Configuration parsing errors and unrelated failures do not satisfy the assertions.

These are negative startup E2Es for incompatible whole-preset routes, not
successful wire roundtrips and not per-type support coverage. Each connector's
existing service-backed E2Es still verify its supported write/storage path.
The tests never project away unsupported columns or silently cast them.
