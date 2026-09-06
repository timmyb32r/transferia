//! Connector-neutral table rules. Compile once, then use the same classifier
//! for catalog previews, startup selection and newly encountered tables.

use std::collections::BTreeSet;

use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::TableIdentity;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PatternMode {
    #[default]
    Glob,
    Regex,
}

impl PatternMode {
    /// Construct an exact expression for a suggestion, without changing the
    /// underlying database identifiers.
    #[must_use]
    pub fn exact_pattern(self, table: &TableIdentity) -> String {
        let name = table.qualified_name();
        match self {
            Self::Regex => regex::escape(&name),
            Self::Glob => name.chars().fold(String::new(), |mut result, ch| {
                if matches!(ch, '\\' | '*' | '?' | '.') {
                    result.push('\\');
                }
                result.push(ch);
                result
            }),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableRule {
    pub include: String,

    #[serde(default)]
    pub exclude: Option<String>,

    #[serde(default)]
    pub include_mode: PatternMode,
    #[serde(default)]
    pub exclude_mode: PatternMode,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TableSelection {
    Selected { rules: Vec<TableRule> },
    All {
        #[serde(default)]
        exclude: Option<String>,
        #[serde(default)]
        exclude_mode: PatternMode,
    },
}

#[derive(Clone, Copy, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PatternField {
    Include,
    Exclude,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid table rule at card index {card}, {field:?}: {reason}")]
pub struct PatternError {
    pub card: usize,
    pub field: PatternField,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConflictKind {
    MultipleIncludes,
    IncludeExclude,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SelectionIssue {
    NoRules,
    EmptyMatch {
        card: usize,
    },
    Conflict {
        table: TableIdentity,
        first_card: usize,
        second_card: usize,
        #[serde(rename = "conflict")]
        kind: ConflictKind,
    },
}

#[derive(Clone, Debug, Default, Serialize, JsonSchema)]
pub struct CardMatches {
    pub selected: Vec<TableIdentity>,
    pub excluded: Vec<TableIdentity>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct SelectionPreview {
    pub cards: Vec<CardMatches>,
    pub issues: Vec<SelectionIssue>,
}

impl SelectionPreview {
    /// Startup must not consume a partially valid preview. Return table
    /// identities only after every rule and cross-card conflict was checked.
    pub fn selected_tables(&self) -> anyhow::Result<Vec<TableIdentity>> {
        anyhow::ensure!(self.cards.iter().any(|card| !card.selected.is_empty()),
            "No tables selected. A delivery must select at least one table.");
        anyhow::ensure!(self.issues.is_empty(), "Invalid table selection: {:?}", self.issues);
        Ok(self.cards.iter().flat_map(|card| card.selected.iter().cloned()).collect())
    }
}

pub struct TableClassification {
    pub selected_by: Vec<usize>,
    pub excluded_by: Vec<usize>,
    pub issues: Vec<SelectionIssue>,
}

struct CompiledRule {
    include: Regex,
    exclude: Option<Regex>,
}

pub struct CompiledSelection {
    rules: Vec<CompiledRule>,
}

impl TableSelection {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Selected { rules } if rules.is_empty())
    }

    pub fn compile(&self) -> Result<CompiledSelection, PatternError> {
        let all_rule;
        let source_rules = match self {
            Self::Selected { rules } => rules.as_slice(),
            Self::All { exclude, exclude_mode } => {
                all_rule = [TableRule { include: "*".into(), include_mode: PatternMode::Glob,
                    exclude: exclude.clone(), exclude_mode: *exclude_mode }];
                &all_rule
            }
        };
        let rules = source_rules.iter().enumerate().map(|(card, rule)| {
            let compile = |text: &str, field, mode| compile_pattern(text, mode)
                .map_err(|reason| PatternError { card, field, reason });
            Ok(CompiledRule {
                include: compile(&rule.include, PatternField::Include, rule.include_mode)?,
                exclude: rule.exclude.as_deref().filter(|text| !text.is_empty())
                    .map(|text| compile(text, PatternField::Exclude, rule.exclude_mode)).transpose()?,
            })
        }).collect::<Result<Vec<_>, PatternError>>()?;
        Ok(CompiledSelection { rules })
    }
}

impl CompiledSelection {
    /// Does not apply the catalog-wide empty-match policy. A runtime caller
    /// must reject conflicts before admitting or acknowledging this table.
    #[must_use]
    pub fn classify(&self, table: &TableIdentity) -> TableClassification {
        let name = table.qualified_name();
        let mut result = TableClassification {
            selected_by: Vec::new(), excluded_by: Vec::new(), issues: Vec::new(),
        };
        for (index, rule) in self.rules.iter().enumerate() {
            if !rule.include.is_match(&name) {
                continue;
            }
            if rule.exclude.as_ref().is_some_and(|exclude| exclude.is_match(&name)) {
                result.excluded_by.push(index);
            } else {
                result.selected_by.push(index);
            }
        }
        for (position, &first) in result.selected_by.iter().enumerate() {
            for &second in result.selected_by.iter().skip(position + 1) {
                result.issues.push(conflict(table, first, second, ConflictKind::MultipleIncludes));
            }
            for &second in &result.excluded_by {
                result.issues.push(conflict(table, first, second, ConflictKind::IncludeExclude));
            }
        }
        result
    }

    pub fn resolve(&self, catalog: &[TableIdentity]) -> anyhow::Result<SelectionPreview> {
        let mut seen = BTreeSet::new();
        let mut result = SelectionPreview {
            cards: vec![CardMatches::default(); self.rules.len()], issues: Vec::new(),
        };
        if self.rules.is_empty() {
            result.issues.push(SelectionIssue::NoRules);
        }
        for table in catalog {
            anyhow::ensure!(!table.namespace.is_empty() && !table.name.is_empty(),
                "Table catalog contains an empty namespace or table name");
            anyhow::ensure!(seen.insert(table), "Table catalog repeats {:?}", table);
            let classification = self.classify(table);
            for index in classification.selected_by {
                result.cards[index].selected.push(table.clone());
            }
            for index in classification.excluded_by {
                result.cards[index].excluded.push(table.clone());
            }
            result.issues.extend(classification.issues);
        }
        for (card, matches) in result.cards.iter_mut().enumerate() {
            matches.selected.sort();
            matches.excluded.sort();
            if matches.selected.is_empty() {
                result.issues.push(SelectionIssue::EmptyMatch { card });
            }
        }
        Ok(result)
    }
}

fn conflict(table: &TableIdentity, first: usize, second: usize, kind: ConflictKind) -> SelectionIssue {
    SelectionIssue::Conflict {
        table: table.clone(), first_card: first.min(second), second_card: first.max(second), kind,
    }
}

fn compile_pattern(text: &str, mode: PatternMode) -> Result<Regex, String> {
    if text.is_empty() {
        return Err("Include must not be empty".into());
    }
    let expression = match mode {
        PatternMode::Regex => text.to_owned(),
        PatternMode::Glob => {
            let mut expression = String::new();
            let mut chars = text.chars();
            while let Some(ch) = chars.next() {
                match ch {
                    '*' => expression.push_str("(?s:.*)"),
                    '?' => expression.push_str("(?s:.)"),
                    '\\' => {
                        let escaped = chars.next().ok_or("Trailing escape character")?;
                        expression.push_str(&regex::escape(&escaped.to_string()));
                    }
                    literal => expression.push_str(&regex::escape(&literal.to_string())),
                }
            }
            expression
        }
    };
    Regex::new(&format!(r"\A(?:{expression})\z")).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests;
