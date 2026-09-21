mod common;
mod numeric;
pub mod sink;
pub mod source;
pub mod src_batch;
pub mod src_batch_and_stream;
pub mod src_stream;
mod temporal;

pub(crate) fn source_type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    use tokio_postgres::types::Type;
    use transferia_registry::type_mapping::{TypeMapping, TypeMappingRow};
    TypeMapping {
        context: "Evaluated by PostgreSQL discovery. Constrained NUMERIC uses exact Decimal128 when its precision and scale fit. Other NUMERIC and pseudo types require batch unsupported_types=to_string; unsupported non-finite decimal values fail before publication. Other non-native types use canonical text. Examples do not replace value, timezone or COPY validation.".to_owned(),
        rows: [Type::BOOL, Type::CHAR, Type::INT2, Type::INT4, Type::INT8, Type::OID,
            Type::FLOAT4, Type::FLOAT8, Type::BYTEA, Type::TEXT, Type::VARCHAR, Type::BPCHAR,
            Type::NAME, Type::DATE, Type::TIMESTAMP, Type::TIMESTAMPTZ,
            Type::UUID, Type::JSON, Type::JSONB, Type::INTERVAL, Type::INET, Type::INT4_ARRAY,
            Type::INT4_RANGE, Type::RECORD]
            .into_iter().map(|t| TypeMappingRow::evaluate(t.name(), common::postgres_to_arrow(&t).map(|a| format!("{a:?}"))))
            .chain([(20, 0), (24, 4), (4, -3)].into_iter().map(|(precision, scale)| {
                TypeMappingRow::evaluate(format!("numeric({precision},{scale})"), numeric::data_type(((precision << 16) | (scale & 0x7ff)) + 4).map(|a| format!("{a:?}")))
            }))
            .chain([TypeMappingRow::evaluate("numeric", numeric::data_type(-1).map(|a| format!("{a:?}")))])
            .collect(),
    }
}

pub use common::{
    check_connection, check_network_connection, connect, list_tables, PostgresConnectionCheckConfig,
    PostgresConnectionConfig, PostgresCopyFormat,
};
pub use sink::PostgresSinkConnector;
pub use source::PostgresSourceConnector;
