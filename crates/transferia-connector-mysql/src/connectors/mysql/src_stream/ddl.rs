use sqlparser::ast::{AlterTableOperation, ObjectName, ObjectNamePart, RenameTableNameKind, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use transferia_registry::table_selection::CompiledSelection;
use transferia_registry::TableIdentity;

fn identity(name: &ObjectName, database: &str) -> Option<TableIdentity> {
    let parts = name.0.iter().map(|part| match part {
        ObjectNamePart::Identifier(identifier) => Some(identifier.value.clone()),
        _ => None,
    }).collect::<Option<Vec<_>>>()?;
    match parts.as_slice() {
        [name] if !database.is_empty() => Some(TableIdentity { namespace: database.into(), name: name.clone() }),
        [namespace, name] => Some(TableIdentity { namespace: namespace.clone(), name: name.clone() }),
        _ => None,
    }
}

/// Only a proven creation of an empty permanent table can be admitted without
/// a snapshot. Ambiguous/no-op/populating forms remain unsupported.
pub(super) fn created_table(query: &[u8], database: &[u8]) -> Option<TableIdentity> {
    let query = std::str::from_utf8(query).ok()?;
    let database = std::str::from_utf8(database).ok()?;
    if query.contains("/*!") || query.contains("/*M!") { return None; }
    let mut statements = Parser::parse_sql(&MySqlDialect {}, query).ok()?;
    if statements.len() != 1 { return None; }
    let Statement::CreateTable(table) = statements.pop()? else { return None; };
    if table.temporary || table.if_not_exists || table.or_replace || table.query.is_some()
        || table.clone.is_some() || table.external || table.dynamic {
        return None;
    }
    identity(&table.name, database)
}

/// Diagnostic only: this function never authorizes skipping a Query event.
/// Unrecognized statements still fail closed in the binlog decoder.
pub(super) fn rename_error(query: &[u8], database: &[u8], selection: &CompiledSelection) -> Option<String> {
    let query = std::str::from_utf8(query).ok()?;
    let database = std::str::from_utf8(database).ok()?;
    // Executable comments need server-version/SQL-mode-aware interpretation.
    // Do not let a generic SQL parser turn them into an apparently empty query.
    if query.contains("/*!") || query.contains("/*M!") { return None; }
    let statements = Parser::parse_sql(&MySqlDialect {}, query).ok()?;
    let mut renames = Vec::new();
    for statement in statements {
        match statement {
            Statement::RenameTable(tables) => {
                for table in tables {
                    renames.push((identity(&table.old_name, database)?, identity(&table.new_name, database)?));
                }
            }
            Statement::AlterTable { name, operations, .. } => {
                let old = identity(&name, database)?;
                for operation in operations {
                    if let AlterTableOperation::RenameTable { table_name } = operation {
                        let (RenameTableNameKind::To(name) | RenameTableNameKind::As(name)) = table_name;
                        renames.push((old.clone(), identity(&name, &old.namespace)?));
                    }
                }
            }
            _ => {}
        }
    }
    for (old, new) in renames {
        let classification = selection.classify(&new);
        if let Some(card) = classification.selected_by.first() {
            return Some(format!(
                "MySQL rename from {:?} to {:?} affects table rule {}. An existing table may contain rows absent from the binlog at the rename position; automatic admission through RENAME is not supported. No rename progress was acknowledged. Configure a fresh snapshot; watermark-based DBLog is future work",
                old.qualified_name(), new.qualified_name(), card + 1,
            ));
        }
    }
    None
}

#[cfg(test)]
#[path = "tests/ddl.rs"]
mod tests;
