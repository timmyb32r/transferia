#[cfg(any(
    feature = "provider-clickhouse",
    feature = "provider-kafka",
    feature = "provider-logbroker",
    feature = "provider-postgres",
    feature = "provider-s3",
    feature = "provider-ytsaurus"
))]
pub(crate) mod address;
pub mod catalog;
#[cfg(feature = "provider-clickhouse")]
pub mod clickhouse;
pub mod discard;
#[cfg(feature = "provider-kafka")]
pub mod kafka;
#[cfg(feature = "provider-logbroker")]
pub mod logbroker;
#[cfg(feature = "provider-postgres")]
pub mod postgres;
#[cfg(feature = "provider-s3")]
pub mod s3;
#[cfg(feature = "provider-ytsaurus")]
pub mod ytsaurus;
