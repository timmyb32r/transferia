mod config;
mod connector;
mod writer;

pub(crate) fn type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    transferia_registry::type_mapping::destination_mapping(
        "Evaluated by MySQL DDL resolver. Non-null columns without source-type extensions; preserved MySQL extensions may select a different physical declaration. Runtime range/encoding validation still applies.",
        connector::mysql_sql_type,
    )
}

pub use config::MySqlSinkConfig;
pub use connector::MySqlSinkConnector;

#[cfg(test)]
mod tests;
