use prost::Message;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;
use ydb_grpc::ydb_proto::formats::ArrowBatchSettings;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;
use ydb_grpc::ydb_proto::table::{
    bulk_upsert_request, BulkUpsertRequest, BulkUpsertResult, CreateSessionRequest,
    CreateSessionResult, DeleteSessionRequest, DescribeTableRequest, DescribeTableResult,
    ExecuteSchemeQueryRequest,
};

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

#[derive(Clone)]
pub(super) struct YdbClient {
    service: TableServiceClient<Channel>,
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
        Ok(Self {
            service: TableServiceClient::new(channel)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024),
            database,
            token,
            timeout: config.request_timeout(),
        })
    }

    pub fn request<T>(&self, body: T) -> anyhow::Result<Request<T>> {
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("x-ydb-database", self.database.clone());
        if let Some(token) = &self.token {
            request
                .metadata_mut()
                .insert("x-ydb-auth-ticket", token.clone());
        }
        Ok(request)
    }

    pub async fn create_session(&mut self) -> anyhow::Result<String> {
        let request = self.request(CreateSessionRequest {
            operation_params: None,
        })?;
        let response = tokio::time::timeout(self.timeout, self.service.create_session(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB CreateSession timed out"))??
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
        })?;
        let response = tokio::time::timeout(self.timeout, self.service.describe_table(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB DescribeTable timed out"))??
            .into_inner();
        let result = decode_operation(response.operation, "DescribeTable");
        let delete = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        })?;
        let _ignored = self.service.delete_session(delete).await;
        result
    }

    pub async fn delete_session(&mut self, session_id: String) -> anyhow::Result<()> {
        let request = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        })?;
        let response = tokio::time::timeout(self.timeout, self.service.delete_session(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB DeleteSession timed out"))??
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
        })?;
        let response =
            tokio::time::timeout(self.timeout, self.service.execute_scheme_query(request))
                .await
                .map_err(|_| anyhow::anyhow!("YDB ExecuteSchemeQuery timed out"))??
                .into_inner();
        ensure_operation(response.operation, "ExecuteSchemeQuery")?;
        let delete = self.request(DeleteSessionRequest {
            session_id,
            operation_params: None,
        })?;
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
        })?;
        let response = tokio::time::timeout(self.timeout, self.service.bulk_upsert(request))
            .await
            .map_err(|_| anyhow::anyhow!("YDB BulkUpsert timed out"))??
            .into_inner();
        decode_operation::<BulkUpsertResult>(response.operation, "BulkUpsert")?;
        Ok(())
    }

    pub fn service(&mut self) -> &mut TableServiceClient<Channel> {
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
