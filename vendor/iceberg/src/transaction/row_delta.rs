// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information.
// Licensed under the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, FormatVersion, ManifestEntry, ManifestFile, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Atomically adds equality-delete files and replacement data files.
pub struct RowDeltaAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    idempotency_properties: Option<HashMap<String, String>>,
    files: Vec<DataFile>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            idempotency_properties: None,
            files: Vec::new(),
        }
    }

    /// Set whether existing live file paths are checked before commit.
    pub fn with_check_duplicate(mut self, value: bool) -> Self {
        self.check_duplicate = value;
        self
    }

    /// Add replacement data files.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files.extend(files);
        self
    }

    /// Add equality-delete files.
    pub fn add_delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files.extend(files);
        self
    }

    /// Set the stable UUID used for generated snapshot metadata files.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for generated manifests.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    /// Treat a snapshot carrying every supplied property as this exact commit.
    pub fn set_idempotency_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.idempotency_properties = Some(properties);
        self
    }

    fn is_already_committed(&self, table: &Table) -> bool {
        let Some(expected) = self
            .idempotency_properties
            .as_ref()
            .filter(|expected| !expected.is_empty())
        else {
            return false;
        };
        let mut snapshot = table.metadata().current_snapshot();
        while let Some(current) = snapshot {
            if expected.iter().all(|(key, value)| {
                current.summary().additional_properties.get(key) == Some(value)
            }) {
                return true;
            }
            snapshot = current
                .parent_snapshot_id()
                .and_then(|parent| table.metadata().snapshot_by_id(parent));
        }
        false
    }

    fn validate(&self, table: &Table) -> Result<()> {
        if table.metadata().format_version() == FormatVersion::V1 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Row delta requires Iceberg format version 2 or newer",
            ));
        }
        if self.files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Row delta requires at least one data or equality-delete file",
            ));
        }
        let identifier_ids = table
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect::<HashSet<_>>();
        if identifier_ids.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Row delta equality deletes require identifier field ids",
            ));
        }
        for file in &self.files {
            if table.metadata().default_partition_spec_id() != file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Row delta file partition spec id does not match the table default",
                ));
            }
            SnapshotProducer::validate_partition_value(
                file.partition(),
                table.metadata().default_partition_type(),
            )?;
            match file.content_type() {
                DataContentType::Data => {}
                DataContentType::EqualityDeletes => {
                    let equality_ids = file
                        .equality_ids()
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "Equality-delete file is missing equality field ids",
                            )
                        })?
                        .into_iter()
                        .collect::<HashSet<_>>();
                    if equality_ids != identifier_ids {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Equality-delete field ids do not match table identifier field ids",
                        ));
                    }
                }
                DataContentType::PositionDeletes => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Position-delete files are not supported by row delta",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.is_already_committed(table) {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }
        self.validate(table)?;
        let producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.files.clone(),
        );
        if self.check_duplicate {
            producer.validate_duplicate_files().await?;
        }
        producer
            .commit(RowDeltaOperation, DefaultManifestProcess)
            .await
    }
}

struct RowDeltaOperation;

impl SnapshotProduceOperation for RowDeltaOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(Vec::new())
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(Vec::new());
        };
        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;
        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}
