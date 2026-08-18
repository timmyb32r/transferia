use arrow::datatypes::DataType;

pub const META_PRIMARY_KEY: &str = "transferia.primary_key";
pub const META_LOW_CARDINALITY: &str = "transferia.low_cardinality";
pub const META_MAX_LENGTH: &str = "transferia.max_length";
pub const META_ARROW_EXTENSION_NAME: &str = "ARROW:extension:name";
pub const ARROW_JSON_EXTENSION_NAME: &str = "arrow.json";

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
    pub primary_key: bool,
    pub low_cardinality: bool,
    pub max_length: Option<usize>,
    pub arrow_extension_name: Option<&'static str>,
}

impl SchemaColumn {
    #[must_use]
    pub const fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
            primary_key: false,
            low_cardinality: false,
            max_length: None,
            arrow_extension_name: None,
        }
    }

    #[must_use]
    pub const fn with_arrow_extension(mut self, name: &'static str) -> Self {
        self.arrow_extension_name = Some(name);
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
        metadata
    }
}
