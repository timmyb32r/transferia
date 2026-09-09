use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;
use schemars::JsonSchema;
use serde::Deserialize;
use transferia_core::{DatasetSchema, TableData};
use transferia_delivery_contracts::middleware::Middleware;
use transferia_registry::{qualified_table_name, MiddlewareRegistration, RegistryBuilder, TableIdentity};

/// Rename the full qualified identity, or explicitly preserve its namespace.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum RenameTableConfig {
    Exact {
        name: String,
        #[serde(default)]
        last_part_only: bool,
    },
    Regex {
        pattern: String,
        replacement: String,
        #[serde(default)]
        last_part_only: bool,
    },
}

enum ReplacementPart {
    Literal(String),
    Capture(usize),
}

pub struct RenameTableMiddleware {
    rename: Rename,
    last_part_only: bool,
}

enum Rename {
    Exact { namespace: Option<Arc<str>>, name: Arc<str> },
    Regex { pattern: Regex, replacement: Vec<ReplacementPart> },
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.trim().is_empty(), "Rename table: resulting name must not be empty");
    anyhow::ensure!(!name.contains('\0'), "Rename table: resulting name must not contain NUL");
    Ok(())
}

impl RenameTableMiddleware {
    pub fn new(config: RenameTableConfig) -> anyhow::Result<Self> {
        let (rename, last_part_only) = match config {
            RenameTableConfig::Exact { name, last_part_only } => {
                validate_name(&name)?;
                let (namespace, name) = if last_part_only {
                    (None, Arc::from(name))
                } else {
                    parse_identity(&name)?
                };
                (Rename::Exact { namespace, name }, last_part_only)
            }
            RenameTableConfig::Regex { pattern, replacement, last_part_only } => {
                anyhow::ensure!(!pattern.is_empty(), "Rename table: regex must not be empty");
                anyhow::ensure!(!replacement.contains('\0'), "Rename table: replacement must not contain NUL");
                let pattern = Regex::new(&pattern)?;
                let replacement = parse_replacement(&pattern, &replacement)?;
                (Rename::Regex { pattern, replacement }, last_part_only)
            }
        };
        Ok(Self { rename, last_part_only })
    }

    fn renamed(&self, namespace: &Option<Arc<str>>, name: &Arc<str>) -> anyhow::Result<(Option<Arc<str>>, Arc<str>)> {
        match &self.rename {
            Rename::Exact { namespace: target_namespace, name: target } => Ok((
                if self.last_part_only { namespace.clone() } else { target_namespace.clone() },
                Arc::clone(target),
            )),
            Rename::Regex { pattern, replacement } => {
                let input = if self.last_part_only {
                    std::borrow::Cow::Borrowed(name.as_ref())
                } else {
                    std::borrow::Cow::Owned(qualified_table_name(namespace.as_deref(), name))
                };
                let mut output = String::new();
                let mut offset = 0;
                let mut matched = false;
                for captures in pattern.captures_iter(&input) {
                    let entire = captures.get(0).ok_or_else(|| anyhow::anyhow!("Rename table: regex returned no whole match"))?;
                    matched = true;
                    output.push_str(&input[offset..entire.start()]);
                    for part in replacement {
                        match part {
                            ReplacementPart::Literal(text) => output.push_str(text),
                            ReplacementPart::Capture(index) => {
                                let capture = captures.get(*index).ok_or_else(|| anyhow::anyhow!(
                                    "Rename table: capture {index} did not participate for table {input:?}"
                                ))?;
                                output.push_str(capture.as_str());
                            }
                        }
                    }
                    offset = entire.end();
                }
                if !matched { return Ok((namespace.clone(), Arc::clone(name))); }
                output.push_str(&input[offset..]);
                validate_name(&output)?;
                if self.last_part_only {
                    Ok((namespace.clone(), output.into()))
                } else {
                    parse_identity(&output)
                }
            }
        }
    }
}

fn parse_identity(value: &str) -> anyhow::Result<(Option<Arc<str>>, Arc<str>)> {
    let identity = TableIdentity::from_qualified_name(value)?;
    Ok(((!identity.namespace.is_empty()).then(|| Arc::from(identity.namespace)), Arc::from(identity.name)))
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
    fn requires_preview_identity_validation(&self) -> bool {
        matches!(self.rename, Rename::Regex { .. })
    }
    fn output_table_identity(&self, namespace: Option<&str>, name: &str) -> anyhow::Result<(Option<Arc<str>>, Arc<str>)> {
        self.renamed(&namespace.map(Arc::from), &Arc::from(name))
    }
    fn validate_preview_identity(&self, namespace: Option<&str>, name: &str) -> anyhow::Result<()> {
        if let Rename::Regex { pattern, .. } = &self.rename {
            let full_name = qualified_table_name(namespace, name);
            let input = if self.last_part_only { name } else { &full_name };
            anyhow::ensure!(pattern.is_match(input),
                "Rename table: pattern {:?} does not match table {:?} (regex input: {:?}). Preview requires a match for every matched table.",
                pattern.as_str(), full_name, input);
        }
        // Reject invalid replacement outputs in unsampled tables as well.
        self.output_table_identity(namespace, name)?;
        Ok(())
    }
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        Ok(schema.clone())
    }

    async fn process(&self, mut data: TableData) -> anyhow::Result<TableData> {
        (data.namespace, data.table) = self.renamed(&data.namespace, &data.table)?;
        Ok(data)
    }
}

pub fn register(builder: &mut RegistryBuilder) -> anyhow::Result<()> {
    builder.register_middleware(MiddlewareRegistration::new::<RenameTableConfig, _, _>(
        "rename_table", "Rename table",
        || serde_json::json!({ "mode": "exact", "name": "", "last_part_only": false }),
        |config| Ok(Box::new(RenameTableMiddleware::new(config)?)),
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
