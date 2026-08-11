use clickhouse_arrow::{ArrowFormat, Client};

use super::connection::connect_client;
use super::ClickHouseSinkConfig;

pub(super) struct ClickHouseAdmin {
    client: Client<ArrowFormat>,
}

impl ClickHouseAdmin {
    pub(super) async fn connect(config: &ClickHouseSinkConfig) -> anyhow::Result<Self> {
        let client = connect_client(config)
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse admin connection failed: {error}"))?;
        client
            .execute("SELECT 1", None)
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse admin health check failed: {error}"))?;
        Ok(Self { client })
    }

    pub(super) async fn create_table(
        &self,
        name: &str,
        columns: &[(String, String)],
        sorting_key: &[String],
        recreate: bool,
    ) -> anyhow::Result<()> {
        if recreate {
            tracing::warn!(table = name, "dropping table before recreation");
            self.client
                .execute(&format!("DROP TABLE IF EXISTS `{name}`"), None)
                .await
                .map_err(|error| anyhow::anyhow!("Failed to drop table '{name}': {error}"))?;
        }
        let columns = columns
            .iter()
            .map(|(column, ty)| format!("`{column}` {ty}"))
            .collect::<Vec<_>>()
            .join(", ");
        let order = if sorting_key.is_empty() {
            "tuple()".to_string()
        } else {
            sorting_key
                .iter()
                .map(|column| format!("`{column}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{name}` ({columns}) ENGINE = MergeTree ORDER BY ({order})",
        );
        self.client
            .execute(&ddl, None)
            .await
            .map_err(|error| anyhow::anyhow!("Failed to create table '{name}': {error}"))?;
        Ok(())
    }
}
