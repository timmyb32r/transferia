# ClickHouse timestamp contract

ClickHouse stores timestamps as instants with a column timezone. An Arrow
`Timestamp(unit, None)` represents a wall-clock value without enough information
to identify an instant. The destination therefore rejects timezone-naive
timestamps during discovery, before destination preparation, and again at the
runtime boundary before buffering, INSERT, or acknowledgement.

Apply an explicit upstream conversion to a timezone-aware timestamp before
delivery. There is no sink option that assumes UTC or inherits the ClickHouse
server timezone. Precreating a UTC destination column does not bypass this
validation. PostgreSQL `timestamp without time zone` consequently needs an
explicit conversion; PostgreSQL `timestamp with time zone` is already an instant.

For timezone-aware input, generated DDL uses `DateTime64` with the exact Arrow
unit (precision 0, 3, 6, or 9) and declared timezone. Existing destination columns
must declare the same timezone and precision explicitly. An omitted destination
timezone is rejected: the native type parser's UTC fallback does not establish
the actual server timezone. Unknown timezone names are rejected during DDL
validation. Native, Parquet, and ArrowStream transports retain the input epoch
ticks and NULLs; timestamps are not converted through formatted local strings.

Native INSERT waits for the server's column header before encoding the batch.
Without that barrier, a seconds timestamp could race into the driver's inferred
unsigned `DateTime` representation instead of the destination's signed
`DateTime64(0)`. Local encoding failures retain their original typed error and
discard the connection; they do not become retryable channel-closure errors.

The September 19 comparison exposed this ambiguity: a naive PostgreSQL value
written as epoch ticks into an implicit-zone ClickHouse column displayed
three hours later in January 1971 and four hours later in July 2013. That input
now fails validation instead of silently changing its interpretation. The old
explicit-UTC destination workaround also requires an explicit upstream
conversion under the current contract.

Regression coverage includes startup and runtime rejection, no INSERT or commit
after rejection, explicit destination timezone checks, and a pinned ClickHouse
server configured for `Europe/Moscow`. The real-service test verifies all three
wire formats, both UTC and Moscow columns, the historical offsets above,
microsecond fractions, NULL, pre-epoch values, and seconds beyond the unsigned
32-bit range.

```sh
cargo test -p transferia-connector-clickhouse --lib --test sink_temporal_e2e
```
