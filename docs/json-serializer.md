# S3 JSON encoding (NDJSON)

The active S3 sink uses `JsonBatchEncoder` to write Arrow rows directly into
newline-delimited JSON object buffers. The encoder is independent from the JSON
parser: it only implements the Arrow-to-bytes direction needed by S3.

## Contract

- Every Arrow row produces one compact JSON object followed by `\n`.
- NULL columns are emitted explicitly as `"column":null`.
- `NaN`, positive infinity, and negative infinity are emitted as JSON `null`.
- An empty batch produces no bytes.
- Strings escape quotes, backslashes, newlines, carriage returns, tabs, and all
  JSON control characters.
- Date and timestamp arrays are encoded as their integer Arrow representation.
- Unsupported Arrow types are rejected before any row is written.

`JsonBatchEncoder` accepts a column projection callback so the S3 sink can omit
internal system columns without coupling the encoder to parser configuration.

Performance claims belong in reproducible benchmarks; this document intentionally
does not attach unmeasured speedup factors to the manual encoder.
