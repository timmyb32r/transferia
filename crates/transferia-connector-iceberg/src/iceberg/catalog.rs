use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};

use super::config::{IcebergTableRef, OpenDalStorageConfig, RestCatalogAuth, RestCatalogConfig};
use super::storage::IcebergOpenDalStorageFactory;
use transferia_connector_support::outbound_http::{NetworkPolicy, OutboundHttpClient};

pub async fn build_catalog(
    config: &RestCatalogConfig,
    storage: &OpenDalStorageConfig,
) -> anyhow::Result<Arc<dyn Catalog>> {
    let mut properties = HashMap::from([(REST_CATALOG_PROP_URI.to_owned(), config.uri.clone())]);
    if let Some(warehouse) = &config.warehouse {
        properties.insert(REST_CATALOG_PROP_WAREHOUSE.to_owned(), warehouse.clone());
    }
    match &config.auth {
        RestCatalogAuth::None => {}
        RestCatalogAuth::Token { token } => {
            properties.insert("token".to_owned(), token.clone());
        }
        RestCatalogAuth::OAuth2 {
            client_id,
            client_secret,
            scope,
            token_url,
        } => {
            properties.insert(
                "credential".to_owned(),
                format!("{client_id}:{client_secret}"),
            );
            if let Some(scope) = scope {
                properties.insert("scope".to_owned(), scope.clone());
            }
            if let Some(token_url) = token_url {
                properties.insert("oauth2-server-uri".to_owned(), token_url.clone());
            }
        }
    }
    let storage_factory = Arc::new(IcebergOpenDalStorageFactory::new(storage.clone()));
    let client = OutboundHttpClient::new(
        Duration::from_millis(config.request_timeout_ms),
        [],
        NetworkPolicy::AllowPrivateNetworks,
    )?
    .transport();
    let catalog = RestCatalogBuilder::default()
        .with_client(client)
        .with_storage_factory(storage_factory)
        .load("transferia", properties)
        .await?;
    Ok(Arc::new(catalog))
}

pub fn table_ident(table: &IcebergTableRef) -> anyhow::Result<TableIdent> {
    TableIdent::from_strs(table.namespace.iter().chain(std::iter::once(&table.name)))
        .map_err(Into::into)
}
