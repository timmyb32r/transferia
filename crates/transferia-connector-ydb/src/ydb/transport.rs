use prost::Message;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;
use transferia_connector_support::external_request::observe_external_request;
use ydb_grpc::ydb_proto::coordination::v1::coordination_service_client::CoordinationServiceClient;
use ydb_grpc::ydb_proto::formats::ArrowBatchSettings;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;
use ydb_grpc::ydb_proto::table::{
    bulk_upsert_request, BulkUpsertRequest, BulkUpsertResult, CommitTransactionRequest,
    CommitTransactionResult, CreateSessionRequest, CreateSessionResult, CreateTableRequest,
    DeleteSessionRequest, DescribeTableRequest, DescribeTableResult, DropTableRequest,
    ExecuteDataQueryRequest, ExecuteQueryResult, ExecuteSchemeQueryRequest, Query,
    QueryCachePolicy, RollbackTransactionRequest, SerializableModeSettings, TransactionControl,
    TransactionSettings,
};
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;
use ydb_grpc::ydb_proto::{table, value, TypedValue};

use super::config::YdbConnectionConfig;

#[derive(Debug)]
struct YdbStatusError {
    operation: String,
    status: StatusCode,
    issues: String,
}

impl std::fmt::Display for YdbStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "YDB {} failed with {:?}: {}",
            self.operation, self.status, self.issues
        )
    }
}

impl std::error::Error for YdbStatusError {}

pub(super) fn is_retryable_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<YdbStatusError>().is_some_and(|error| {
        matches!(
            error.status,
            StatusCode::Unavailable
                | StatusCode::Overloaded
                | StatusCode::Aborted
                | StatusCode::Undetermined
                | StatusCode::SessionBusy
        )
    }) || error.downcast_ref::<tonic::Status>().is_some_and(|status| {
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::ResourceExhausted
                | tonic::Code::Aborted
                | tonic::Code::DeadlineExceeded
        )
    })
}

pub(super) fn is_not_found_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<YdbStatusError>()
        .is_some_and(|error| error.status == StatusCode::NotFound)
}

#[derive(Clone)]
pub(super) struct YdbClient {
    service: TableServiceClient<Channel>,
    topic_service: TopicServiceClient<Channel>,
    coordination_service: CoordinationServiceClient<Channel>,
    database: AsciiMetadataValue,
    token: Option<AsciiMetadataValue>,
    timeout: std::time::Duration,
}

impl YdbClient {
    pub async fn connect(config: &YdbConnectionConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let endpoint_url = config.tonic_endpoint()?;
        let mut endpoint = Endpoint::from_shared(endpoint_url.clone())?
            .connect_timeout(config.request_timeout())
            .timeout(config.request_timeout())
            .tcp_nodelay(true)
            .http2_keep_alive_interval(config.request_timeout())
            .keep_alive_timeout(config.request_timeout())
            .keep_alive_while_idle(true);
        if endpoint_url.starts_with("https://") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }
        let channel = endpoint.connect().await?;
        let token = config
            .auth
            .resolve()
            .await?
            .map(|token| {
                AsciiMetadataValue::try_from(token)
                    .map_err(|_| anyhow::anyhow!("YDB access token is not valid ASCII metadata"))
            })
            .transpose()?;
        let database = AsciiMetadataValue::try_from(config.database.clone())
            .map_err(|_| anyhow::anyhow!("YDB database is not valid ASCII metadata"))?;
        let max_rpc_message_bytes = config.max_rpc_message_bytes;
        Ok(Self {
            service: TableServiceClient::new(channel.clone())
                .max_decoding_message_size(max_rpc_message_bytes)
                .max_encoding_message_size(max_rpc_message_bytes),
            topic_service: TopicServiceClient::new(channel.clone())
                .max_decoding_message_size(max_rpc_message_bytes)
                .max_encoding_message_size(max_rpc_message_bytes),
            coordination_service: CoordinationServiceClient::new(channel)
                .max_decoding_message_size(max_rpc_message_bytes)
                .max_encoding_message_size(max_rpc_message_bytes),
            database,
            token,
            timeout: config.request_timeout(),
        })
    }

    pub fn request<T>(&self, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("x-ydb-database", self.database.clone());
        if let Some(token) = &self.token {
            request
                .metadata_mut()
                .insert("x-ydb-auth-ticket", token.clone());
        }
        request
    }

    pub fn topic_service(&self) -> TopicServiceClient<Channel> {
        self.topic_service.clone()
    }

    pub fn coordination_service(&self) -> CoordinationServiceClient<Channel> {
        self.coordination_service.clone()
    }

    pub async fn create_session(&mut self) -> anyhow::Result<String> {
        let request = self.request(CreateSessionRequest {
            operation_params: None,
        });
        let response = observe_external_request("ydb", "table.create_session", async {
            tokio::time::timeout(self.timeout, self.service.create_session(request))
                .await
                .map_err(|_| anyhow::anyhow!("YDB CreateSession timed out"))?
                .map_err(anyhow::Error::from)
        })
        .await?
        .into_inner();
        decode_operation::<CreateSessionResult>(response.operation, "CreateSession")
            .map(|result| result.session_id)
    }

    pub async fn describe_table(&mut self, path: String) -> anyhow::Result<DescribeTableResult> {
        let session_id = self.create_session().await?;
        let request = self.request(DescribeTableRequest {
            session_id: session_id.clone(),
            path,
            operation_params: None,
            include_shard_key_bounds: false,
            include_table_stats: false,
            include_partition_stats: false,
            include_set_val: false,
            include_shard_nodes_info: false,
        });
        let response = observe_external_request("ydb", "table.describe_table", async {
            tokio::time::timeout(self.timeout, self.service.describe_table(request))
                .await
                .map_err(|_| anyhow::anyhow!("YDB DescribeTable timed out"))?
                .map_err(anyhow::Error::from)
        })
        .await?
        .into_inner();
        let result = decode_operation(response.operation, "DescribeTable");
        let _ignored = self.delete_session(session_id).await;
        result
    }

    pub async fn create_table(&mut self, mut request: CreateTableRequest) -> anyhow::Result<()> {
        let session_id = self.create_session().await?;
        request.session_id = session_id.clone();
        let request = self.request(request);
        let response = tokio::time::timeout(self.timeout, self.service.create_table(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB CreateTable timed out"))??
            .into_inner();
        ensure_operation(response.operation, "CreateTable")?;
        let delete = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        });
        let _ignored = self.service.delete_session(delete).await;
        Ok(())
    }

    pub async fn drop_table(&mut self, path: String) -> anyhow::Result<()> {
        let session_id = self.create_session().await?;
        let request = self.request(DropTableRequest {
            session_id: session_id.clone(),
            path,
            operation_params: None,
        });
        let response = tokio::time::timeout(self.timeout, self.service.drop_table(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB DropTable timed out"))??
            .into_inner();
        ensure_operation(response.operation, "DropTable")?;
        let delete = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        });
        let _ignored = self.service.delete_session(delete).await;
        Ok(())
    }

    pub async fn delete_session(&mut self, session_id: String) -> anyhow::Result<()> {
        let request = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        });
        let response = observe_external_request("ydb", "table.delete_session", async {
            tokio::time::timeout(self.timeout, self.service.delete_session(request))
                .await
                .map_err(|_| anyhow::anyhow!("YDB DeleteSession timed out"))?
                .map_err(anyhow::Error::from)
        })
        .await?
        .into_inner();
        ensure_operation(response.operation, "DeleteSession")?;
        Ok(())
    }

    pub async fn execute_scheme_query(&mut self, yql_text: String) -> anyhow::Result<()> {
        let session_id = self.create_session().await?;
        let request = self.request(ExecuteSchemeQueryRequest {
            session_id: session_id.clone(),
            yql_text,
            operation_params: None,
        });
        let response =
            tokio::time::timeout(self.timeout, self.service.execute_scheme_query(request))
                .await
                .map_err(|_| anyhow::anyhow!("YDB ExecuteSchemeQuery timed out"))??
                .into_inner();
        ensure_operation(response.operation, "ExecuteSchemeQuery")?;
        let delete = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        });
        let _ignored = self.service.delete_session(delete).await;
        Ok(())
    }

    pub async fn execute_data_query(
        &mut self,
        yql_text: String,
        parameters: std::collections::HashMap<String, TypedValue>,
    ) -> anyhow::Result<()> {
        let session_id = self.create_session().await?;
        let request = self.request(ExecuteDataQueryRequest {
            session_id: session_id.clone(),
            tx_control: Some(TransactionControl {
                commit_tx: true,
                tx_selector: Some(table::transaction_control::TxSelector::BeginTx(
                    TransactionSettings {
                        tx_mode: Some(table::transaction_settings::TxMode::SerializableReadWrite(
                            SerializableModeSettings {},
                        )),
                    },
                )),
            }),
            query: Some(Query {
                query: Some(table::query::Query::YqlText(yql_text)),
            }),
            parameters,
            query_cache_policy: Some(QueryCachePolicy {
                keep_in_cache: true,
            }),
            operation_params: None,
            collect_stats: table::query_stats_collection::Mode::StatsCollectionNone.into(),
        });
        let response = tokio::time::timeout(self.timeout, self.service.execute_data_query(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB ExecuteDataQuery timed out"))??
            .into_inner();
        decode_operation::<ExecuteQueryResult>(response.operation, "ExecuteDataQuery")?;
        let delete = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        });
        let _ignored = self.service.delete_session(delete).await;
        Ok(())
    }

    pub async fn execute_checked_update(
        &mut self,
        yql_text: String,
        parameters: std::collections::HashMap<String, TypedValue>,
        expected_rows: u64,
    ) -> anyhow::Result<()> {
        let session_id = self.create_session().await?;
        let request = self.request(ExecuteDataQueryRequest {
            session_id: session_id.clone(),
            tx_control: Some(TransactionControl {
                commit_tx: false,
                tx_selector: Some(table::transaction_control::TxSelector::BeginTx(
                    TransactionSettings {
                        tx_mode: Some(table::transaction_settings::TxMode::SerializableReadWrite(
                            SerializableModeSettings {},
                        )),
                    },
                )),
            }),
            query: Some(Query {
                query: Some(table::query::Query::YqlText(yql_text)),
            }),
            parameters,
            query_cache_policy: Some(QueryCachePolicy {
                keep_in_cache: true,
            }),
            operation_params: None,
            collect_stats: table::query_stats_collection::Mode::StatsCollectionNone.into(),
        });
        let response = tokio::time::timeout(self.timeout, self.service.execute_data_query(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB ExecuteDataQuery timed out"))??
            .into_inner();
        let result =
            decode_operation::<ExecuteQueryResult>(response.operation, "ExecuteDataQuery")?;
        let transaction_id = result
            .tx_meta
            .as_ref()
            .map(|metadata| metadata.id.clone())
            .filter(|id| !id.is_empty())
            .ok_or_else(|| anyhow::anyhow!("YDB checked UPDATE returned no transaction id"))?;
        let matched_rows = result
            .result_sets
            .first()
            .and_then(|result| result.rows.first())
            .and_then(|row| row.items.first())
            .and_then(|value| value.value.as_ref())
            .and_then(|value| match value {
                value::Value::Uint64Value(value) => Some(*value),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("YDB checked UPDATE returned no Uint64 match count"))?;
        if matched_rows != expected_rows {
            let rollback = self.request(RollbackTransactionRequest {
                session_id: session_id.clone(),
                tx_id: transaction_id,
                operation_params: None,
            });
            let response =
                tokio::time::timeout(self.timeout, self.service.rollback_transaction(rollback))
                    .await
                    .map_err(|_| anyhow::anyhow!("YDB RollbackTransaction timed out"))??
                    .into_inner();
            ensure_operation(response.operation, "RollbackTransaction")?;
            let delete = self.request(DeleteSessionRequest {
                session_id,
                operation_params: None,
            });
            let _ignored = self.service.delete_session(delete).await;
            anyhow::bail!(
                "YDB UPDATE matched {matched_rows} rows, expected {expected_rows}; destination state is incomplete"
            );
        }
        let commit = self.request(CommitTransactionRequest {
            session_id: session_id.clone(),
            tx_id: transaction_id,
            operation_params: None,
            collect_stats: table::query_stats_collection::Mode::StatsCollectionNone.into(),
        });
        let response = tokio::time::timeout(self.timeout, self.service.commit_transaction(commit))
            .await
            .map_err(|_| anyhow::anyhow!("YDB CommitTransaction timed out"))??
            .into_inner();
        decode_operation::<CommitTransactionResult>(response.operation, "CommitTransaction")?;
        let delete = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        });
        let _ignored = self.service.delete_session(delete).await;
        Ok(())
    }

    pub async fn bulk_upsert(
        &mut self,
        table: String,
        schema: Vec<u8>,
        data: Vec<u8>,
    ) -> anyhow::Result<()> {
        let request = self.request(BulkUpsertRequest {
            table,
            rows: None,
            operation_params: None,
            data,
            data_format: Some(bulk_upsert_request::DataFormat::ArrowBatchSettings(
                ArrowBatchSettings { schema },
            )),
        });
        let response = tokio::time::timeout(self.timeout, self.service.bulk_upsert(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB BulkUpsert timed out"))??
            .into_inner();
        decode_operation::<BulkUpsertResult>(response.operation, "BulkUpsert")?;
        Ok(())
    }

    pub const fn service(&mut self) -> &mut TableServiceClient<Channel> {
        &mut self.service
    }

    #[must_use]
    pub const fn timeout(&self) -> std::time::Duration {
        self.timeout
    }
}

fn decode_operation<T: Message + Default>(
    operation: Option<ydb_grpc::ydb_proto::operations::Operation>,
    name: &str,
) -> anyhow::Result<T> {
    let operation = ensure_operation(operation, name)?;
    let result = operation
        .result
        .ok_or_else(|| anyhow::anyhow!("YDB {name} returned no result"))?;
    Ok(T::decode(result.value.as_slice())?)
}

fn ensure_operation(
    operation: Option<ydb_grpc::ydb_proto::operations::Operation>,
    name: &str,
) -> anyhow::Result<ydb_grpc::ydb_proto::operations::Operation> {
    let operation = operation.ok_or_else(|| anyhow::anyhow!("YDB {name} returned no operation"))?;
    anyhow::ensure!(
        operation.ready,
        "YDB {name} returned an asynchronous operation"
    );
    let status = StatusCode::try_from(operation.status).unwrap_or(StatusCode::Unspecified);
    if status != StatusCode::Success {
        return Err(YdbStatusError {
            operation: name.to_owned(),
            status,
            issues: serde_json::to_string(&operation.issues)?,
        }
        .into());
    }
    Ok(operation)
}
