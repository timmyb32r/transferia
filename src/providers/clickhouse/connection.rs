use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};

use super::ClickHouseSinkConfig;

pub(super) async fn build_pool(
    config: &ClickHouseSinkConfig,
) -> anyhow::Result<ConnectionPool<ArrowFormat>> {
    ConnectionPoolBuilder::<ArrowFormat>::new(config.connection_string.as_str())
        .configure_pool(|pool| pool.max_size(1))
        .configure_client(|builder| {
            let mut builder = builder
                .with_database(config.database.as_str())
                .with_username(config.username.as_str())
                .with_password(config.password.as_str())
                .with_tls(config.use_tls);
            if let Some(domain) = &config.tls_domain {
                builder = builder.with_domain(domain.as_str());
            }
            builder
        })
        .build()
        .await
        .map_err(|error| anyhow::anyhow!("Failed to build ClickHouse pool: {error}"))
}
