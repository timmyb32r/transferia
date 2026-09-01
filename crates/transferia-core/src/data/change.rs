/// Canonical row-level changelog operation.
///
/// Codes intentionally match Debezium so queue serialization never needs a
/// second operation mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperation {
    Create,
    SnapshotRead,
    Update,
    Delete,
}

impl ChangeOperation {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Create => "c",
            Self::SnapshotRead => "r",
            Self::Update => "u",
            Self::Delete => "d",
        }
    }
}
