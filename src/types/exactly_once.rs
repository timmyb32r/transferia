/// A single column of a composite uniqueness key.
#[derive(Debug, Clone)]
pub struct ExactlyOnceColumn {
    pub name: alloc::sync::Arc<str>,
}

impl ExactlyOnceColumn {
    #[must_use]
    pub const fn new(name: alloc::sync::Arc<str>) -> Self {
        Self { name }
    }
}

/// Composite uniqueness key. Columns physically reside in the `RecordBatch`;
/// the descriptor names their roles.
#[derive(Debug, Clone)]
pub struct ExactlyOnceKey {
    /// Partition-space column:
    ///   YDS: Int64 (partition id)
    ///   S3:  Utf8  (full S3 object key).
    pub partition: ExactlyOnceColumn,
    /// Monotonic offset within a partition: Int64.
    pub offset: ExactlyOnceColumn,
}

/// Partition key value — the key type of the waterline `HashMap`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PartitionKey {
    Int(i64),
    /// S3: full object key (not just the base filename).
    Str(String),
}

impl ExactlyOnceKey {
    #[must_use]
    pub const fn new(partition: ExactlyOnceColumn, offset: ExactlyOnceColumn) -> Self {
        Self { partition, offset }
    }
}

impl PartitionKey {
    /// SQL literal for use in `WHERE partition = {val}`.
    ///
    /// `Int` → number; `Str` → `unhex('<hex-encoded-bytes>')`.
    /// Hex encoding avoids manual escaping:
    ///   - CH literals apply C-style unescape → backslash is lost;
    ///   - clickhouse-arrow 0.2.1 escapes only `'`, not `\`.
    ///   - unhex('...') delegates correctness to `hex::encode`, not manual escape.
    #[must_use]
    pub fn to_sql_literal(&self) -> String {
        match *self {
            Self::Int(v) => v.to_string(),
            Self::Str(ref v) => format!("unhex('{}')", hex::encode(v.as_bytes())),
        }
    }
}
