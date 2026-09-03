use arrow::datatypes::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::connectors::postgres::source::DiscoveredTable;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostgresSystemIdentity {
    pub(crate) system_identifier: u64,

    pub(crate) database: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostgresSourceIdentity {
    pub(crate) system_identifier: u64,

    pub(crate) database: String,

    pub(crate) database_oid: u32,
}

impl PostgresSourceIdentity {
    pub(crate) fn system(&self) -> PostgresSystemIdentity {
        PostgresSystemIdentity {
            system_identifier: self.system_identifier,
            database: self.database.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthoritativeTableIdentity {
    schema: String,

    name: String,

    relation_oid: u32,

    columns: Vec<AuthoritativeColumnIdentity>,

    replica_identity: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthoritativeColumnIdentity {
    name: String,

    postgres_type_oid: u32,

    arrow_type: CanonicalArrowType,

    nullable: bool,

    primary_key: bool,

    low_cardinality: bool,

    max_length: Option<u64>,

    arrow_extension_name: Option<String>,

    system_role: Option<String>,

    old_value_of: Option<String>,

    old_key_of: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", deny_unknown_fields)]
enum CanonicalArrowType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt32,
    Float32,
    Float64,
    Binary,
    Utf8,
    Date32,
    TimestampMicrosecond { timezone: Option<String> },
}

pub(crate) fn authoritative_table_identities(
    tables: &[DiscoveredTable],
) -> anyhow::Result<Vec<AuthoritativeTableIdentity>> {
    tables
        .iter()
        .map(AuthoritativeTableIdentity::from_discovered)
        .collect()
}

impl AuthoritativeTableIdentity {
    fn from_discovered(table: &DiscoveredTable) -> anyhow::Result<Self> {
        anyhow::ensure!(
            table.schema.columns.len() == table.type_oids.len(),
            "PostgreSQL table '{}.{}' has {} schema columns but {} type OIDs",
            table.config.schema,
            table.config.name,
            table.schema.columns.len(),
            table.type_oids.len(),
        );
        let columns = table
            .schema
            .columns
            .iter()
            .zip(&table.type_oids)
            .map(|(column, oid)| {
                Ok(AuthoritativeColumnIdentity {
                    name: column.name.clone(),
                    postgres_type_oid: *oid,
                    arrow_type: CanonicalArrowType::from_data_type(&column.data_type)?,
                    nullable: column.nullable,
                    primary_key: column.primary_key,
                    low_cardinality: column.low_cardinality,
                    max_length: column.max_length.map(u64::try_from).transpose()?,
                    arrow_extension_name: column.arrow_extension_name.map(str::to_owned),
                    system_role: column.system_role.clone(),
                    old_value_of: column.old_value_of.clone(),
                    old_key_of: column.old_key_of.clone(),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            schema: table.config.schema.clone(),
            name: table.config.name.clone(),
            relation_oid: table.relation_oid,
            columns,
            replica_identity: table.replica_identity.clone(),
        })
    }
}

impl CanonicalArrowType {
    fn from_data_type(data_type: &DataType) -> anyhow::Result<Self> {
        Ok(match data_type {
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt32 => Self::UInt32,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Binary => Self::Binary,
            DataType::Utf8 => Self::Utf8,
            DataType::Date32 => Self::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => Self::TimestampMicrosecond {
                timezone: timezone.as_ref().map(ToString::to_string),
            },
            other => anyhow::bail!("unsupported PostgreSQL authoritative Arrow type {other:?}"),
        })
    }
}
