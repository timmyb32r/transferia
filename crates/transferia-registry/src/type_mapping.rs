//! Offline examples evaluated with the very same resolvers used by discovery/DDL.
//! Inputs are examples, never an independently maintained conversion table.
use schemars::JsonSchema;
use serde::Serialize;
use transferia_core::data::schema::SchemaColumn;

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypeMapping {
    pub context: String,
    pub rows: Vec<TypeMappingRow>,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypeMappingRow {
    pub input: String,
    pub output: Option<String>,
    pub error: Option<String>,
}

impl TypeMappingRow {
    pub fn evaluate(input: impl Into<String>, result: anyhow::Result<String>) -> Self {
        let (output, error) = match result {
            Ok(output) => (Some(output), None),
            Err(error) => (None, Some(format!("{error:#}"))),
        };
        Self {
            input: input.into(),
            output,
            error,
        }
    }
}

/// Representative parameter values are deliberately explicit in each row.
/// Rejections remain visible; absence from this list does not mean unsupported.
pub fn destination_mapping(
    context: &str,
    resolve: impl Fn(&SchemaColumn) -> anyhow::Result<String>,
) -> TypeMapping {
    let mut types = Vec::new();
    for (_, data_type) in crate::arrow_examples::types() {
        if !types.contains(&data_type) {
            types.push(data_type);
        }
    }
    TypeMapping {
        context: context.to_owned(),
        rows: types
            .into_iter()
            .map(|data_type| {
                let input = format!("{data_type:?}");
                TypeMappingRow::evaluate(
                    input,
                    resolve(&SchemaColumn::new("value".to_owned(), data_type, false)),
                )
            })
            .collect(),
    }
}
