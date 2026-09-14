use arrow::datatypes::DataType;

pub const META_PRIMARY_KEY: &str = "transferia.primary_key";
pub const META_LOW_CARDINALITY: &str = "transferia.low_cardinality";
pub const META_MAX_LENGTH: &str = "transferia.max_length";
pub const META_ALWAYS_PRESENT_ON_UPDATE: &str = "transferia.always_present_on_update";
pub const META_UPDATE_VALUE_PRESENCE: &str = "transferia.update_value_presence";
pub const META_DELETE_VALUE_PRESENCE: &str = "transferia.delete_value_presence";
pub const META_ARROW_EXTENSION_NAME: &str = "ARROW:extension:name";
pub const META_ARROW_EXTENSION_METADATA: &str = "ARROW:extension:metadata";
/// Identifies an incoming control column that is never part of stored user data.
pub const META_SYSTEM_ROLE: &str = "transferia.system_role";
pub const SYSTEM_ROLE_SOURCE_DATABASE: &str = "source.database";
/// Logical row ordering, independent of the transport cursor (for snapshot/CDC handoffs).
pub const SYSTEM_ROLE_SOURCE_VERSION: &str = "source.version";
pub const SYSTEM_ROLE_SOURCE_SCHEMA: &str = "source.schema";
pub const SYSTEM_ROLE_SOURCE_TABLE: &str = "source.table";
pub const SYSTEM_ROLE_SOURCE_TRANSACTION_ID: &str = "source.transaction_id";
/// Exact `MySQL` binlog-origin metadata, separate from durable replay coordinates.
pub const SYSTEM_ROLE_SOURCE_SERVER_ID: &str = "source.server_id";
pub const SYSTEM_ROLE_SOURCE_GTID: &str = "source.gtid";
pub const SYSTEM_ROLE_SOURCE_BINLOG_FILE: &str = "source.binlog_file";
pub const SYSTEM_ROLE_SOURCE_BINLOG_POSITION: &str = "source.binlog_position";
pub const SYSTEM_ROLE_SOURCE_BINLOG_ROW: &str = "source.binlog_row";
pub const SYSTEM_ROLE_SOURCE_TIMESTAMP_MS: &str = "source.timestamp_ms";
pub const SYSTEM_ROLE_SOURCE_TIMESTAMP_US: &str = "source.timestamp_us";
pub const SYSTEM_ROLE_SOURCE_TIMESTAMP_NS: &str = "source.timestamp_ns";
pub const SYSTEM_ROLE_EVENT_TIMESTAMP_MS: &str = "event.timestamp_ms";
pub const SYSTEM_ROLE_EVENT_TIMESTAMP_US: &str = "event.timestamp_us";
pub const SYSTEM_ROLE_EVENT_TIMESTAMP_NS: &str = "event.timestamp_ns";
/// Marks the one Arrow column carrying the row-level changelog operation.
pub const META_CHANGE_OPERATION: &str = "transferia.change_operation";
/// Names the current-value column paired with an old-value column.
pub const META_OLD_VALUE_OF: &str = "transferia.old_value_of";
/// Names the primary-key column paired with an old-key CDC control column.
pub const META_OLD_KEY_OF: &str = "transferia.old_key_of";
pub const ARROW_JSON_EXTENSION_NAME: &str = "arrow.json";

/// Sink-neutral runtime schema exchanged between source and sink connectors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatasetSchema {
    pub columns: Vec<SchemaColumn>,
}

impl DatasetSchema {
    #[must_use]
    pub const fn new(columns: Vec<SchemaColumn>) -> Self {
        Self { columns }
    }
}

/// Availability of a normalized source value, independent of SQL nullability.
///
/// `Guaranteed` includes a present SQL NULL, but never an omitted value replaced
/// with NULL. Sources must prove this after reconstruction, before Arrow creation.
/// Derived columns must not inherit a guarantee without proving their semantics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ValuePresence {
    #[default]
    MayBeAbsent,
    Guaranteed,
}

impl ValuePresence {
    /// Absence of the metadata key means no guarantee, including older schemas.
    #[must_use]
    pub const fn metadata_value(self) -> Option<&'static str> {
        match self {
            Self::MayBeAbsent => None,
            Self::Guaranteed => Some("guaranteed"),
        }
    }
}

/// One logical column expressed in Arrow types, before sink-specific mapping.
#[derive(Debug, Clone)]
// These are independent column properties, not mutually exclusive states.
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent schema properties"
)]
pub struct SchemaColumn {
    pub name: String,
    /// Source-authored native declaration for inspection, not an Arrow type or
    /// a destination constraint. Derived columns have no native declaration.
    /// Excluded from schema compatibility and runtime Arrow field metadata.
    pub source_type: Option<String>,
    pub data_type: DataType,
    pub nullable: bool,
    /// The new tuple of every UPDATE contains this column (SQL NULL counts as
    /// present). Does not describe DELETE/old tuples or imply non-nullability.
    /// False means no guarantee, not that the column is necessarily omitted.
    pub always_present_on_update: bool,
    /// Availability of the current (new) value in a normalized UPDATE row.
    pub update_value_presence: ValuePresence,
    /// Availability of the deleted value in a normalized DELETE row.
    /// No claim is made about UPDATE old-value control columns.
    pub delete_value_presence: ValuePresence,
    pub primary_key: bool,
    pub low_cardinality: bool,
    pub max_length: Option<usize>,
    pub arrow_extension_name: Option<&'static str>,
    /// Canonical connector-authored payload for an Arrow extension type.
    ///
    /// Arrow readers that do not understand the extension may still consume
    /// the declared storage type, but must preserve this metadata verbatim.
    pub arrow_extension_metadata: Option<String>,
    /// Connector-neutral semantic role of an incoming control column.
    pub system_role: Option<String>,
    /// Current-value column paired with this CDC old-value control column.
    ///
    /// Old-value columns are transport metadata. They are present in the
    /// incoming Arrow schema but are never part of the destination's stored
    /// projection.
    pub old_value_of: Option<String>,
    /// Primary-key column paired with this CDC old-key control column.
    ///
    /// Old-key columns preserve the identity before an UPDATE changes its
    /// primary key. They are incoming transport metadata and are never stored.
    pub old_key_of: Option<String>,
}

impl SchemaColumn {
    #[must_use]
    pub const fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            source_type: None,
            data_type,
            nullable,
            always_present_on_update: false,
            update_value_presence: ValuePresence::MayBeAbsent,
            delete_value_presence: ValuePresence::MayBeAbsent,
            primary_key: false,
            low_cardinality: false,
            max_length: None,
            arrow_extension_name: None,
            arrow_extension_metadata: None,
            system_role: None,
            old_value_of: None,
            old_key_of: None,
        }
    }

    #[must_use]
    pub fn with_source_type(mut self, declaration: impl Into<String>) -> Self {
        self.source_type = Some(declaration.into());
        self
    }

    #[must_use]
    pub const fn with_arrow_extension(mut self, name: &'static str) -> Self {
        self.arrow_extension_name = Some(name);
        self
    }

    #[must_use]
    pub fn with_arrow_extension_metadata(
        mut self,
        name: &'static str,
        metadata: impl Into<String>,
    ) -> Self {
        self.arrow_extension_name = Some(name);
        self.arrow_extension_metadata = Some(metadata.into());
        self
    }

    #[must_use]
    pub fn with_old_value_of(mut self, current_column: String) -> Self {
        self.old_value_of = Some(current_column);
        self
    }

    #[must_use]
    pub fn with_old_key_of(mut self, current_column: String) -> Self {
        self.old_key_of = Some(current_column);
        self
    }

    #[must_use]
    pub fn with_system_role(mut self, role: impl Into<String>) -> Self {
        self.system_role = Some(role.into());
        self
    }

    #[must_use]
    pub const fn with_constraints(
        mut self,
        primary_key: bool,
        low_cardinality: bool,
        max_length: Option<usize>,
    ) -> Self {
        self.primary_key = primary_key;
        self.low_cardinality = low_cardinality;
        self.max_length = max_length;
        self
    }

    #[must_use]
    pub fn arrow_metadata(&self) -> std::collections::HashMap<String, String> {
        let mut metadata = std::collections::HashMap::new();
        for (key, presence) in [
            (META_UPDATE_VALUE_PRESENCE, self.update_value_presence),
            (META_DELETE_VALUE_PRESENCE, self.delete_value_presence),
        ] {
            if let Some(value) = presence.metadata_value() {
                metadata.insert(key.into(), value.into());
            }
        }
        if self.always_present_on_update {
            metadata.insert(META_ALWAYS_PRESENT_ON_UPDATE.into(), "true".into());
        }
        if self.primary_key {
            metadata.insert(META_PRIMARY_KEY.into(), "true".into());
        }
        if self.low_cardinality {
            metadata.insert(META_LOW_CARDINALITY.into(), "true".into());
        }
        if let Some(max_length) = self.max_length {
            metadata.insert(META_MAX_LENGTH.into(), max_length.to_string());
        }
        if let Some(extension_name) = self.arrow_extension_name {
            metadata.insert(META_ARROW_EXTENSION_NAME.into(), extension_name.into());
        }
        if let Some(extension_metadata) = &self.arrow_extension_metadata {
            metadata.insert(
                META_ARROW_EXTENSION_METADATA.into(),
                extension_metadata.clone(),
            );
        }
        if let Some(role) = &self.system_role {
            metadata.insert(META_SYSTEM_ROLE.into(), role.clone());
        }
        if let Some(current_column) = &self.old_value_of {
            metadata.insert(META_OLD_VALUE_OF.into(), current_column.clone());
        }
        if let Some(current_column) = &self.old_key_of {
            metadata.insert(META_OLD_KEY_OF.into(), current_column.clone());
        }
        metadata
    }
}

// Native declarations are provenance, not the physical Arrow schema. Including
// them would reject otherwise compatible tables renamed into one output.
impl PartialEq for SchemaColumn {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.data_type == other.data_type
            && self.nullable == other.nullable
            && self.always_present_on_update == other.always_present_on_update
            && self.update_value_presence == other.update_value_presence
            && self.delete_value_presence == other.delete_value_presence
            && self.primary_key == other.primary_key
            && self.low_cardinality == other.low_cardinality
            && self.max_length == other.max_length
            && self.arrow_extension_name == other.arrow_extension_name
            && self.arrow_extension_metadata == other.arrow_extension_metadata
            && self.system_role == other.system_role
            && self.old_value_of == other.old_value_of
            && self.old_key_of == other.old_key_of
    }
}

impl Eq for SchemaColumn {}

#[cfg(test)]
#[path = "../tests/schema.rs"]
mod tests;
