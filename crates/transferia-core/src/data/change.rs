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

    #[must_use]
    pub const fn from_code(code: &str) -> Option<Self> {
        match code.as_bytes() {
            b"c" => Some(Self::Create),
            b"r" => Some(Self::SnapshotRead),
            b"u" => Some(Self::Update),
            b"d" => Some(Self::Delete),
            _ => None,
        }
    }

    #[must_use]
    pub const fn writes_current_value(self) -> bool {
        matches!(self, Self::Create | Self::SnapshotRead | Self::Update)
    }
}
