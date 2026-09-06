# ClickHouse source type conversion

The ClickHouse source has **Unsupported source types** in Advanced options:

- **to_string** (`unsupported_types: to_string`, default) applies ClickHouse
  `toString` to that entire column. For example, a tuple with an unsupported
  member becomes one string, not a tuple with silently converted members.
- **Fail delivery** (`unsupported_types: fail`) rejects a column that
  cannot be represented by the source Arrow reader during discovery.

This setting is independent of the Native/Parquet snapshot reader. Supported
columns retain their typed representation even when `to_string` is selected.
There is no opaque-binary fallback.

Omitting `unsupported_types` selects `to_string` in both YAML and the editor;
an explicitly configured `fail` is never replaced. PostgreSQL has the same
batch default, using its text cast instead of ClickHouse `toString`. PostgreSQL
stream and batch-and-stream deliveries default to `fail`; explicitly requesting
`to_string` for these modes is rejected because replication does not support it.

```yaml
unsupported_types: to_string
```

The conversion changes the logical type and is not guaranteed to be reversible.
The output is UTF-8 text; SQL NULL remains NULL. Known non-nullable scalar types
remain non-nullable; types whose nullability the reader cannot determine use a
nullable string. ClickHouse must accept the projection during discovery. If its
`toString` implementation does not support a type, or produces non-UTF-8 bytes,
the delivery fails explicitly. No replacement characters, substituted NULLs,
skipped rows, base64 encoding, or aggregate finalization are performed.

## Typed representation

| ClickHouse family | Source representation |
| --- | --- |
| Bool; signed/unsigned integers through 64 bits; Float32/64 | Corresponding Arrow scalar |
| String; FixedString(N) | Binary; fixed-size binary, preserving all bytes |
| Date, Date32 | Date32 |
| DateTime, DateTime64(0…9, timezone) | Timestamp with the declared timezone and checked exact tick conversion |
| Decimal(P,S), Decimal32/64/128/256(S) | Decimal128/256 with the declared precision and scale |
| Enum8, Enum16 | Int32-keyed dictionary of all declared labels; source codes retained in type metadata |
| Nullable, LowCardinality, Array, Tuple, Map | Recursive Arrow representation; tuple names, map order/duplicates and nulls preserved |
| Point, Ring, Polygon, MultiPolygon | Corresponding nested coordinate structure |
| Wider integers, UUID/IP addresses, newer/unsupported families such as Dynamic, Variant, JSON, AggregateFunction, Time/Time64 and QBit | Explicit policy above; never driver-specific bytes masquerading as an ordinary scalar |

The complete source declaration and whether conversion occurred are recorded in
`transferia.clickhouse.source_type` Arrow extension metadata. This retains enum
codes, decimal parameters, wrappers and other source-type distinctions that the
physical Arrow storage alone does not express. Intermediate expression-only
ClickHouse types are not table-source columns.

Discovery validates table and column names, type mapping and the actual SELECT
projection before destination preparation. Runtime queries assert the original
ClickHouse type declarations, including when Parquet or `toString` would hide a
type change. Native block declarations and unhinted Parquet schemas are checked
before representation decoding. Decimal/timestamp overflow, unknown enum codes
and invalid dictionary indices fail before a batch is emitted.

Regression fixtures cover both readers, the INFORMATION_SCHEMA `Enum8('NO' = 0,
'YES' = 1)` case, full enum cardinalities, nested modifiers, precision, explicit
string conversion, NULL, malformed payloads and type drift. Running these tests
requires the repository's explicit test gate; the normal development gate only
compiles the affected code.
