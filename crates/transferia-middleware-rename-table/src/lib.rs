use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;
use schemars::JsonSchema;
use serde::Deserialize;
use transferia_core::{DatasetSchema, TableData};
use transferia_delivery_contracts::middleware::Middleware;
use transferia_registry::{MiddlewareRegistration, RegistryBuilder};

/// Explicitly rename the unqualified table name; preserve namespace and data.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum RenameTableConfig {
    Exact { name: String },
    Regex { pattern: String, replacement: String },
}

enum ReplacementPart {
    Literal(String),
    Capture(usize),
}

pub struct RenameTableMiddleware {
    rename: Rename,
}

enum Rename {
    Exact(Arc<str>),
    Regex { pattern: Regex, replacement: Vec<ReplacementPart> },
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.trim().is_empty(), "Rename table: resulting name must not be empty");
    anyhow::ensure!(!name.contains('\0'), "Rename table: resulting name must not contain NUL");
    Ok(())
}

impl RenameTableMiddleware {
    pub fn new(config: RenameTableConfig) -> anyhow::Result<Self> {
        let rename = match config {
            RenameTableConfig::Exact { name } => {
                validate_name(&name)?;
                Rename::Exact(name.into())
            }
            RenameTableConfig::Regex { pattern, replacement } => {
                anyhow::ensure!(!pattern.is_empty(), "Rename table: regex must not be empty");
                anyhow::ensure!(!replacement.contains('\0'), "Rename table: replacement must not contain NUL");
                let pattern = Regex::new(&pattern)?;
                let replacement = parse_replacement(&pattern, &replacement)?;
                Rename::Regex { pattern, replacement }
            }
        };
        Ok(Self { rename })
    }

    fn renamed(&self, name: &Arc<str>) -> anyhow::Result<Arc<str>> {
        match &self.rename {
            Rename::Exact(target) => Ok(Arc::clone(target)),
            Rename::Regex { pattern, replacement } => {
                let mut output = String::new();
                let mut offset = 0;
                let mut matched = false;
                for captures in pattern.captures_iter(name) {
                    let entire = captures.get(0).ok_or_else(|| anyhow::anyhow!("Rename table: regex returned no whole match"))?;
                    matched = true;
                    output.push_str(&name[offset..entire.start()]);
                    for part in replacement {
                        match part {
                            ReplacementPart::Literal(text) => output.push_str(text),
                            ReplacementPart::Capture(index) => {
                                let capture = captures.get(*index).ok_or_else(|| anyhow::anyhow!(
                                    "Rename table: capture {index} did not participate for table {name:?}"
                                ))?;
                                output.push_str(capture.as_str());
                            }
                        }
                    }
                    offset = entire.end();
                }
                if !matched { return Ok(Arc::clone(name)); }
                output.push_str(&name[offset..]);
                validate_name(&output)?;
                Ok(output.into())
            }
        }
    }
}

fn parse_replacement(pattern: &Regex, text: &str) -> anyhow::Result<Vec<ReplacementPart>> {
    let mut parts = Vec::new();
    let mut rest = text;
    while let Some(dollar) = rest.find('$') {
        parts.push(ReplacementPart::Literal(rest[..dollar].to_owned()));
        rest = &rest[dollar + 1..];
        if let Some(tail) = rest.strip_prefix('$') {
            parts.push(ReplacementPart::Literal("$".to_owned()));
            rest = tail;
            continue;
        }
        let reference;
        if let Some(braced) = rest.strip_prefix('{') {
            let end = braced.find('}').ok_or_else(|| anyhow::anyhow!("Rename table: unclosed capture reference"))?;
            reference = &braced[..end];
            rest = &braced[end + 1..];
        } else {
            let end = rest.find(|c: char| !c.is_ascii_alphanumeric() && c != '_').unwrap_or(rest.len());
            reference = &rest[..end];
            rest = &rest[end..];
        }
        anyhow::ensure!(!reference.is_empty(), "Rename table: use $$ for a literal dollar");
        let index = reference.parse::<usize>().ok().filter(|index| *index < pattern.captures_len())
            .or_else(|| pattern.capture_names().position(|name| name == Some(reference)))
            .ok_or_else(|| anyhow::anyhow!("Rename table: unknown capture {reference:?}"))?;
        parts.push(ReplacementPart::Capture(index));
    }
    parts.push(ReplacementPart::Literal(rest.to_owned()));
    Ok(parts)
}

#[async_trait]
impl Middleware for RenameTableMiddleware {
    fn output_table_name(&self, _namespace: Option<&str>, name: &str) -> anyhow::Result<Arc<str>> {
        self.renamed(&Arc::from(name))
    }
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        Ok(schema.clone())
    }

    async fn process(&self, mut data: TableData) -> anyhow::Result<TableData> {
        data.table = self.renamed(&data.table)?;
        Ok(data)
    }
}

pub fn register(builder: &mut RegistryBuilder) -> anyhow::Result<()> {
    builder.register_middleware(MiddlewareRegistration::new::<RenameTableConfig, _, _>(
        "rename_table", "Rename table",
        || serde_json::json!({ "mode": "exact", "name": "" }),
        |config| Ok(Box::new(RenameTableMiddleware::new(config)?)),
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
