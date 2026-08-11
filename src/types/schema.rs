use arrow::datatypes::DataType;

/// Sink-neutral runtime schema exchanged between source and sink providers.
#[derive(Debug, Clone, Default)]
pub struct DatasetSchema {
    pub columns: Vec<SchemaColumn>,
}

impl DatasetSchema {
    #[must_use]
    pub const fn new(columns: Vec<SchemaColumn>) -> Self {
        Self { columns }
    }
}

/// One logical column expressed in Arrow types, before sink-specific mapping.
#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl SchemaColumn {
    #[must_use]
    pub const fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
}
