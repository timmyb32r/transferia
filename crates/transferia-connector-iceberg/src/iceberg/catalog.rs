use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};

use super::config::{IcebergTableRef, OpenDalStorageConfig, RestCatalogAuth, RestCatalogConfig};
use super::storage::IcebergOpenDalStorageFactory;
use transferia_connector_support::external_request::observe_external_request;
use transferia_connector_support::outbound_http::{NetworkPolicy, OutboundHttpClient};

struct ObservedCatalog {
    inner: Arc<dyn Catalog>,
}

impl fmt::Debug for ObservedCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ObservedCatalog").finish()
    }
}

#[async_trait]
impl Catalog for ObservedCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        observe_external_request(
            "iceberg_rest_catalog",
            "list_namespaces",
            self.inner.list_namespaces(parent),
        )
        .await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        observe_external_request(
            "iceberg_rest_catalog",
            "create_namespace",
            self.inner.create_namespace(namespace, properties),
        )
        .await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        observe_external_request(
            "iceberg_rest_catalog",
            "get_namespace",
            self.inner.get_namespace(namespace),
        )
        .await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        observe_external_request(
            "iceberg_rest_catalog",
            "namespace_exists",
            self.inner.namespace_exists(namespace),
        )
        .await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        observe_external_request(
            "iceberg_rest_catalog",
            "update_namespace",
            self.inner.update_namespace(namespace, properties),
        )
        .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        observe_external_request(
            "iceberg_rest_catalog",
            "drop_namespace",
            self.inner.drop_namespace(namespace),
        )
        .await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        observe_external_request(
            "iceberg_rest_catalog",
            "list_tables",
            self.inner.list_tables(namespace),
        )
        .await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        observe_external_request(
            "iceberg_rest_catalog",
            "create_table",
            self.inner.create_table(namespace, creation),
        )
        .await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        observe_external_request(
            "iceberg_rest_catalog",
            "load_table",
            self.inner.load_table(table),
        )
        .await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        observe_external_request(
            "iceberg_rest_catalog",
            "drop_table",
            self.inner.drop_table(table),
        )
        .await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        observe_external_request(
            "iceberg_rest_catalog",
            "table_exists",
            self.inner.table_exists(table),
        )
        .await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        observe_external_request(
            "iceberg_rest_catalog",
            "rename_table",
            self.inner.rename_table(src, dest),
        )
        .await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        observe_external_request(
            "iceberg_rest_catalog",
            "register_table",
            self.inner.register_table(table, metadata_location),
        )
        .await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        observe_external_request(
            "iceberg_rest_catalog",
            "update_table",
            self.inner.update_table(commit),
        )
        .await
    }
}

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
    let catalog = observe_external_request(
        "iceberg_rest_catalog",
        "initialize",
        RestCatalogBuilder::default()
            .with_client(client)
            .with_storage_factory(storage_factory)
            .load("transferia", properties),
    )
    .await?;
    Ok(Arc::new(ObservedCatalog {
        inner: Arc::new(catalog),
    }))
}

pub fn table_ident(table: &IcebergTableRef) -> anyhow::Result<TableIdent> {
    TableIdent::from_strs(table.namespace.iter().chain(std::iter::once(&table.name)))
        .map_err(Into::into)
}
