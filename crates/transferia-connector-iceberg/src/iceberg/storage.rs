use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::services::{Webhdfs, S3};
use opendal::{Operator, Writer};
use serde::{Deserialize, Serialize};
use url::Url;

use super::config::{HdfsStorageConfig, OpenDalStorageConfig, S3StorageConfig};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IcebergOpenDalStorageFactory {
    config: OpenDalStorageConfig,
}

impl IcebergOpenDalStorageFactory {
    pub const fn new(config: OpenDalStorageConfig) -> Self {
        Self { config }
    }
}

#[typetag::serde]
impl StorageFactory for IcebergOpenDalStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(IcebergOpenDalStorage {
            config: self.config.clone(),
        }))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct IcebergOpenDalStorage {
    config: OpenDalStorageConfig,
}

impl IcebergOpenDalStorage {
    fn operator_and_path(&self, location: &str) -> Result<(Operator, String)> {
        match &self.config {
            OpenDalStorageConfig::S3(config) => s3_operator(config, location),
            OpenDalStorageConfig::Hdfs(config) => hdfs_operator(config, location),
        }
    }
}

fn s3_operator(config: &S3StorageConfig, location: &str) -> Result<(Operator, String)> {
    let url = Url::parse(location).map_err(iceberg_invalid)?;
    if !matches!(url.scheme(), "s3" | "s3a" | "s3n") {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("expected an S3 Iceberg location, got '{location}'"),
        ));
    }
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("S3 location '{location}' has no bucket"),
        )
    })?;
    if bucket != config.bucket {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "S3 location bucket '{bucket}' differs from configured bucket '{}'",
                config.bucket
            ),
        ));
    }
    let mut builder = S3::default().bucket(&config.bucket);
    if let Some(region) = &config.region {
        builder = builder.region(region);
    }
    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint(endpoint);
    }
    if let Some(access_key_id) = &config.access_key_id {
        builder = builder.access_key_id(access_key_id);
    }
    if let Some(secret_access_key) = &config.secret_access_key {
        builder = builder.secret_access_key(secret_access_key);
    }
    if let Some(session_token) = &config.session_token {
        builder = builder.session_token(session_token);
    }
    if !config.path_style_access {
        builder = builder.enable_virtual_host_style();
    }
    if config.allow_anonymous {
        builder = builder.allow_anonymous();
    }
    let operator = Operator::new(builder).map_err(opendal_error)?.finish();
    Ok((operator, url.path().trim_start_matches('/').to_owned()))
}

fn hdfs_operator(config: &HdfsStorageConfig, location: &str) -> Result<(Operator, String)> {
    let url = Url::parse(location).map_err(iceberg_invalid)?;
    if url.scheme() != "hdfs" {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("expected an hdfs:// Iceberg location, got '{location}'"),
        ));
    }
    let authority = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("HDFS location '{location}' has no authority"),
        )
    })?;
    if authority != config.authority {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "HDFS location authority '{authority}' differs from configured authority '{}'",
                config.authority
            ),
        ));
    }
    let configured_root = config.root.trim_end_matches('/');
    let absolute_path = url.path();
    let relative = if configured_root.is_empty() {
        absolute_path.trim_start_matches('/')
    } else {
        absolute_path
            .strip_prefix(configured_root)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "HDFS location path '{absolute_path}' is outside configured root '{}'",
                        config.root
                    ),
                )
            })?
            .trim_start_matches('/')
    };
    let mut builder = Webhdfs::default()
        .endpoint(&config.endpoint)
        .root(&config.root);
    if let Some(user) = &config.user {
        builder = builder.user_name(user);
    }
    let operator = Operator::new(builder).map_err(opendal_error)?.finish();
    Ok((operator, relative.to_owned()))
}

#[typetag::serde]
#[async_trait]
impl Storage for IcebergOpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (operator, relative) = self.operator_and_path(path)?;
        operator.exists(&relative).await.map_err(opendal_error)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (operator, relative) = self.operator_and_path(path)?;
        let metadata = operator.stat(&relative).await.map_err(opendal_error)?;
        Ok(FileMetadata {
            size: metadata.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (operator, relative) = self.operator_and_path(path)?;
        Ok(operator
            .read(&relative)
            .await
            .map_err(opendal_error)?
            .to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (operator, relative) = self.operator_and_path(path)?;
        Ok(Box::new(OpenDalReader(
            operator.reader(&relative).await.map_err(opendal_error)?,
        )))
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        let (operator, relative) = self.operator_and_path(path)?;
        operator
            .write(&relative, bytes)
            .await
            .map_err(opendal_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (operator, relative) = self.operator_and_path(path)?;
        Ok(Box::new(OpenDalWriter(
            operator.writer(&relative).await.map_err(opendal_error)?,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (operator, relative) = self.operator_and_path(path)?;
        operator.delete(&relative).await.map_err(opendal_error)?;
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (operator, relative) = self.operator_and_path(path)?;
        operator
            .remove_all(&relative)
            .await
            .map_err(opendal_error)?;
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.operator_and_path(path)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.operator_and_path(path)?;
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
}

struct OpenDalReader(opendal::Reader);

#[async_trait]
impl FileRead for OpenDalReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(self.0.read(range).await.map_err(opendal_error)?.to_bytes())
    }
}

struct OpenDalWriter(Writer);

#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        self.0.write(bytes).await.map_err(opendal_error)?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.0.close().await.map_err(opendal_error)?;
        Ok(())
    }
}

fn opendal_error(error: opendal::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "OpenDAL storage operation failed").with_source(error)
}

fn iceberg_invalid(error: url::ParseError) -> Error {
    Error::new(ErrorKind::DataInvalid, "invalid Iceberg storage location").with_source(error)
}
