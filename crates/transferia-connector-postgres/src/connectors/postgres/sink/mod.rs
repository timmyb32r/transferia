mod config;
mod connector;
mod copy_binary;
mod copy_text;
mod writer;

pub(crate) fn type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    transferia_registry::type_mapping::destination_mapping(
        "Evaluated by PostgreSQL DDL/COPY type resolver. Non-null columns without extensions; runtime value validation still applies.",
        |column| connector::postgres_sql_type(&column.data_type).map(str::to_owned),
    )
}

pub use config::PostgresSinkConfig;
pub use connector::PostgresSinkConnector;

#[cfg(test)]
mod tests;
