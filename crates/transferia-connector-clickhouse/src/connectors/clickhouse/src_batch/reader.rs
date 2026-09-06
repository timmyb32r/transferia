use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    make_array, ArrayData, ArrayRef, BinaryArray, BooleanBuilder, Decimal128Array, Decimal256Array,
    DictionaryArray, Int32Builder, Int64Array, PrimitiveArray, StringArray, UInt8Array, UInt64Array,
};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Int8Type, Int16Type, Int32Type, Schema, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::Type;
use futures_util::future::BoxFuture;
use futures_util::Stream;
use futures_util::StreamExt as _;
use std::pin::Pin;

use super::connector::DiscoveredTable;
use super::types::{is_string_conversion, source_declaration, validate_wire_declaration, wire_declaration};
use crate::metrics::SourceCounters;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::SchemaColumn;
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_core::failure::DataPlaneFailure;
use transferia_core::source::{CommitMarker, Source};

pub(super) struct ClickHouseSource {
    table: DiscoveredTable,
    column_plans: Vec<SnapshotColumnPlan>,
    partition_id: i64,
    stream: SnapshotStream,
    pending: Option<(RecordBatch, usize)>,
    batch_rows: usize,
    request_timeout: Duration,
    offset: i64,
    finished: bool,
    counters: Arc<SourceCounters>,
}

pub(super) type SnapshotStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<RecordBatch>> + Send + 'static>>;

impl ClickHouseSource {
    pub(super) fn new(
        table: DiscoveredTable,
        partition_id: i64,
        stream: SnapshotStream,
        batch_rows: usize,
        request_timeout: Duration,
        counters: Arc<SourceCounters>,
    ) -> anyhow::Result<Self> {
        let column_plans = snapshot_column_plans(&table)?;
        Ok(Self {
            table,
            column_plans,
            partition_id,
            stream,
            pending: None,
            batch_rows,
            request_timeout,
            offset: 0,
            finished: false,
            counters,
        })
    }

    fn output(&mut self, batch: &RecordBatch) -> anyhow::Result<SourceBatch> {
        let batch = normalize_snapshot_schema_with_plans(batch, &self.table, &self.column_plans)?;
        validate_snapshot_schema(&batch, &self.table)?;
        let rows = batch.num_rows();
        let rows_i64 = i64::try_from(rows)?;
        let base = batch.num_columns();
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        let mut arrays = batch.columns().to_vec();
        let system_columns = if self.table.physical_system_columns.is_empty() {
            fields.extend([
                Arc::new(Field::new(
                    SystemColumnKind::Topic.default_name(),
                    DataType::Utf8,
                    false,
                )),
                Arc::new(Field::new(
                    SystemColumnKind::Partition.default_name(),
                    DataType::Int64,
                    false,
                )),
                Arc::new(Field::new(
                    SystemColumnKind::Offset.default_name(),
                    DataType::Int64,
                    false,
                )),
                Arc::new(Field::new(
                    SystemColumnKind::MessageIndex.default_name(),
                    DataType::UInt64,
                    false,
                )),
            ]);
            arrays.extend([
                Arc::new(StringArray::from(vec![
                    format!(
                        "{}.{}",
                        self.table.config.database, self.table.config.name
                    );
                    rows
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![self.partition_id; rows])) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(
                    self.offset
                        ..self
                            .offset
                            .checked_add(rows_i64)
                            .ok_or_else(|| anyhow::anyhow!("ClickHouse source offset overflow"))?,
                )) as ArrayRef,
                Arc::new(UInt64Array::from(vec![0_u64; rows])) as ArrayRef,
            ]);
            SystemColumns::new(vec![
                SystemColumn {
                    kind: SystemColumnKind::Topic,
                    name: Arc::from(SystemColumnKind::Topic.default_name()),
                    index: base,
                },
                SystemColumn {
                    kind: SystemColumnKind::Partition,
                    name: Arc::from(SystemColumnKind::Partition.default_name()),
                    index: base + 1,
                },
                SystemColumn {
                    kind: SystemColumnKind::Offset,
                    name: Arc::from(SystemColumnKind::Offset.default_name()),
                    index: base + 2,
                },
                SystemColumn {
                    kind: SystemColumnKind::MessageIndex,
                    name: Arc::from(SystemColumnKind::MessageIndex.default_name()),
                    index: base + 3,
                },
            ])
        } else {
            self.table.physical_system_columns.clone()
        };
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        self.offset = self
            .offset
            .checked_add(rows_i64)
            .ok_or_else(|| anyhow::anyhow!("ClickHouse source offset overflow"))?;
        self.counters.add_records(rows as u64);
        Ok(SourceBatch::Typed {
            tables: vec![TableData::new(
                Arc::from(self.table.config.name.as_str()),
                false,
                batch,
                system_columns,
            )],
            source_rows: rows as u64,
            commit_marker: Some(CommitMarker::new(self.offset)),
            memory: Vec::new(),
        })
    }
}

impl Source for ClickHouseSource {
    fn read_batch(
        &mut self,
    ) -> BoxFuture<'_, transferia_core::failure::DataPlaneResult<SourceBatch>> {
        Box::pin(async move {
            loop {
                if let Some((batch, offset)) = self.pending.take() {
                    let take = self.batch_rows.min(batch.num_rows() - offset);
                    let output = batch.slice(offset, take);
                    if offset + take < batch.num_rows() {
                        self.pending = Some((batch, offset + take));
                    }
                    return self.output(&output).map_err(DataPlaneFailure::fatal);
                }
                if self.finished {
                    tracing::info!(
                        table = %format!("{}.{}", self.table.config.database, self.table.config.name),
                        emitted_rows = self.offset,
                        "ClickHouse snapshot source completed"
                    );
                    return Ok(SourceBatch::Finished);
                }
                let next = tokio::time::timeout(self.request_timeout, self.stream.next())
                    .await
                    .map_err(|_| {
                        DataPlaneFailure::retryable(anyhow::anyhow!(
                            "ClickHouse snapshot response timed out after {} ms",
                            self.request_timeout.as_millis()
                        ))
                    })?;
                match next {
                    Some(Ok(batch)) if batch.num_rows() > 0 => {
                        // This is the payload after transport decompression and
                        // ClickHouse-to-Arrow decoding, before Transferia adds synthetic
                        // system columns. Transports that expose compressed response
                        // sizes account for network-raw at their own boundary.
                        self.counters.add_network_decoded_bytes(
                            u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
                        );
                        self.pending = Some((batch, 0));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        return Err(DataPlaneFailure::retryable(anyhow::anyhow!(
                            "ClickHouse snapshot response failed: {error}"
                        )))
                    }
                    None => self.finished = true,
                }
            }
        })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn validate_snapshot_schema(batch: &RecordBatch, table: &DiscoveredTable) -> anyhow::Result<()> {
    anyhow::ensure!(
        batch.num_columns() == table.schema.columns.len(),
        "ClickHouse snapshot query for '{}.{}' returned {} columns, discovery declared {}",
        table.config.database,
        table.config.name,
        batch.num_columns(),
        table.schema.columns.len()
    );
    for (actual, expected) in batch.schema().fields().iter().zip(&table.schema.columns) {
        anyhow::ensure!(actual.name() == &expected.name && actual.data_type() == &expected.data_type && actual.is_nullable() == expected.nullable, "ClickHouse snapshot schema drifted at '{}.{}': discovered '{} {:?} nullable={}', query returned '{} {:?} nullable={}'", table.config.database, table.config.name, expected.name, expected.data_type, expected.nullable, actual.name(), actual.data_type(), actual.is_nullable());
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn normalize_snapshot_schema(
    batch: &RecordBatch,
    table: &DiscoveredTable,
) -> anyhow::Result<RecordBatch> {
    normalize_snapshot_schema_with_plans(batch, table, &snapshot_column_plans(table)?)
}

fn normalize_snapshot_schema_with_plans(
    batch: &RecordBatch,
    table: &DiscoveredTable,
    column_plans: &[SnapshotColumnPlan],
) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(
        batch.num_columns() == table.schema.columns.len(),
        "ClickHouse snapshot query for '{}.{}' returned {} columns, discovery declared {}",
        table.config.database,
        table.config.name,
        batch.num_columns(),
        table.schema.columns.len()
    );
    let input_schema = batch.schema();
    let mut fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();
    let mut arrays = batch.columns().to_vec();
    for (index, column) in table.schema.columns.iter().enumerate() {
        let plan = &column_plans[index];
        let actual = &input_schema.fields()[index];
        let allow_native_tuple_names = validate_wire_declaration(actual, plan.wire_declaration.as_deref()).map_err(|error| {
            anyhow::anyhow!(
                "ClickHouse source table '{}.{}' column '{}': {error:#}",
                table.config.database, table.config.name, column.name,
            )
        })?;
        anyhow::ensure!(
            actual.name() == &column.name && actual.is_nullable() == column.nullable,
            "ClickHouse snapshot schema drifted at '{}.{}' column {}: discovered '{} nullable={}', query returned '{} nullable={}'",
            table.config.database,
            table.config.name,
            index,
            column.name,
            column.nullable,
            actual.name(),
            actual.is_nullable(),
        );
        let expected = plan.field.data_type();
        if arrays[index].data_type() == &DataType::Binary
            && expected == &DataType::Utf8
            && plan.string_conversion
        {
            arrays[index] = decode_snapshot_string(&arrays[index], &format!(
                "{}.{}.{}", table.config.database, table.config.name, column.name,
            ))?;
        }
        if !allow_native_tuple_names {
            arrays[index] = plan.enums.decode(&arrays[index], &column.name).map_err(|error| {
                anyhow::anyhow!("ClickHouse source table '{}.{}' column '{}': {error:#}",
                    table.config.database, table.config.name, column.name)
            })?;
        }
        arrays[index] = normalize_snapshot_array(
            &arrays[index], expected, &column.name, allow_native_tuple_names,
        ).map_err(|error| anyhow::anyhow!(
            "ClickHouse snapshot schema drifted at '{}.{}' column '{}': {error:#}",
            table.config.database, table.config.name, column.name,
        ))?;
        fields[index] = Arc::clone(&plan.field);
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

struct SnapshotColumnPlan {
    field: Arc<Field>,
    string_conversion: bool,
    wire_declaration: Option<String>,
    enums: EnumTransport,
}

fn snapshot_column_plans(table: &DiscoveredTable) -> anyhow::Result<Vec<SnapshotColumnPlan>> {
    table.schema.columns.iter().enumerate().map(|(index, column)| {
        let system = table.physical_system_columns.iter().find(|system| system.index == index);
        let data_type = system.map_or_else(|| column.data_type.clone(), |system| system.kind.data_type());
        Ok(SnapshotColumnPlan {
            field: Arc::new(Field::new(&column.name, data_type, column.nullable)
                .with_metadata(column.arrow_metadata())),
            string_conversion: is_string_conversion(column)
                || system.is_some_and(|system| system.kind == SystemColumnKind::Topic),
            wire_declaration: wire_declaration(column),
            enums: EnumTransport::for_column(column).map_err(|error| anyhow::anyhow!(
                "ClickHouse source table '{}.{}' column '{}': {error:#}",
                table.config.database, table.config.name, column.name,
            ))?,
        })
    }).collect()
}

pub(super) enum EnumTransport {
    Identity,
    Enum(EnumLookup),
    Children(Vec<Self>),
}

pub(super) struct EnumLookup {
    code_type: DataType,
    minimum: i32,
    indexes: Box<[i32]>,
    labels: ArrayRef,
}

impl EnumTransport {
    pub(super) fn for_column(column: &SchemaColumn) -> anyhow::Result<Self> {
        if is_string_conversion(column) {
            return Ok(Self::Identity);
        }
        source_declaration(column).map(|declaration| {
            Self::new(&declaration.parse::<Type>()?)
        }).transpose().map(|plan| plan.unwrap_or(Self::Identity))
    }

    pub(super) fn new(declared: &Type) -> anyhow::Result<Self> {
        let children = match declared {
            Type::Nullable(inner) => return Self::new(inner),
            Type::Enum8(labels) => return Ok(Self::Enum(EnumLookup::new(labels, DataType::Int8)?)),
            Type::Enum16(labels) => return Ok(Self::Enum(EnumLookup::new(labels, DataType::Int16)?)),
            Type::Array(inner) | Type::LowCardinality(inner) => vec![Self::new(inner)?],
            Type::Tuple(members) => members.iter().map(Self::new).collect::<anyhow::Result<Vec<_>>>()?,
            Type::Map(key, value) => vec![Self::Children(vec![Self::new(key)?, Self::new(value)?])],
            _ => return Ok(Self::Identity),
        };
        if children.iter().all(Self::is_identity) {
            Ok(Self::Identity)
        } else {
            Ok(Self::Children(children))
        }
    }

    fn is_identity(&self) -> bool {
        match self {
            Self::Identity => true,
            Self::Children(children) => children.iter().all(Self::is_identity),
            Self::Enum(_) => false,
        }
    }

    pub(super) fn parquet_type(&self, canonical: &DataType) -> anyhow::Result<DataType> {
        match self {
            Self::Identity => Ok(canonical.clone()),
            Self::Enum(mapping) => Ok(mapping.code_type.clone()),
            Self::Children(plans) => {
                let children = nested_types(canonical)?;
                anyhow::ensure!(children.len() == plans.len(), "invalid ClickHouse enum container schema");
                let children = plans.iter().zip(children).map(|(plan, data_type)| {
                    plan.parquet_type(data_type)
                }).collect::<anyhow::Result<Vec<_>>>()?;
                with_nested_types(canonical, children)
            }
        }
    }

    pub(super) fn decode(&self, array: &ArrayRef, path: &str) -> anyhow::Result<ArrayRef> {
        match self {
            Self::Identity => Ok(Arc::clone(array)),
            Self::Enum(mapping) => mapping.decode(array, path),
            Self::Children(plans) => {
                let data = array.to_data();
                anyhow::ensure!(data.child_data().len() == plans.len(),
                    "ClickHouse enum column '{path}' has invalid nested Arrow storage");
                let children = plans.iter().zip(data.child_data()).enumerate().map(|(index, (plan, child))| {
                    plan.decode(&make_array(child.clone()), &format!("{path}[{index}]"))
                        .map(|array| array.to_data())
                }).collect::<anyhow::Result<Vec<_>>>()?;
                let data_type = with_nested_types(array.data_type(),
                    children.iter().map(|child| child.data_type().clone()).collect())?;
                Ok(make_array(data.into_builder().data_type(data_type).child_data(children).build()?))
            }
        }
    }
}

impl EnumLookup {
    fn new<T: Copy + Into<i32>>(labels: &[(String, T)], code_type: DataType) -> anyhow::Result<Self> {
        let minimum = labels.iter().map(|(_, code)| (*code).into()).min()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse enum declaration is empty"))?;
        let maximum = labels.iter().map(|(_, code)| (*code).into()).max().unwrap_or(minimum);
        let mut indexes = vec![-1_i32; usize::try_from(maximum - minimum + 1)?];
        let mut names = std::collections::BTreeSet::new();
        for (index, (label, code)) in labels.iter().enumerate() {
            let slot = &mut indexes[usize::try_from((*code).into() - minimum)?];
            anyhow::ensure!(*slot == -1 && names.insert(label), "ClickHouse enum repeats a code or label");
            *slot = i32::try_from(index)?;
        }
        Ok(Self {
            code_type, minimum, indexes: indexes.into_boxed_slice(),
            labels: Arc::new(StringArray::from_iter_values(labels.iter().map(|(label, _)| label))),
        })
    }

    fn decode(&self, array: &ArrayRef, path: &str) -> anyhow::Result<ArrayRef> {
        let canonical = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        if array.data_type() == &canonical {
            return Ok(Arc::clone(array));
        }
        anyhow::ensure!(array.data_type() == &self.code_type,
            "ClickHouse enum column '{path}' schema drifted: expected {:?} codes, query returned {:?}",
            self.code_type, array.data_type());
        match self.code_type {
            DataType::Int8 => self.decode_codes::<Int8Type>(array, path),
            DataType::Int16 => self.decode_codes::<Int16Type>(array, path),
            _ => anyhow::bail!("ClickHouse enum column '{path}' has invalid code storage"),
        }
    }

    fn decode_codes<T>(&self, array: &ArrayRef, path: &str) -> anyhow::Result<ArrayRef>
    where T: ArrowPrimitiveType, T::Native: Into<i32> {
        let values = array.as_any().downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse enum column '{path}' has invalid Arrow storage"))?;
        let mut keys = Int32Builder::with_capacity(array.len());
        for (row, code) in values.iter().enumerate() {
            let Some(code) = code else { keys.append_null(); continue };
            let code = code.into();
            let index = usize::try_from(code - self.minimum).ok()
                .and_then(|index| self.indexes.get(index)).copied().filter(|index| *index >= 0)
                .ok_or_else(|| anyhow::anyhow!("ClickHouse enum column '{path}' row {row} has undeclared code {code}"))?;
            keys.append_value(index);
        }
        Ok(Arc::new(DictionaryArray::<Int32Type>::try_new(keys.finish(), Arc::clone(&self.labels))?))
    }
}

fn nested_types(data_type: &DataType) -> anyhow::Result<Vec<&DataType>> {
    Ok(match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => vec![field.data_type()],
        DataType::Struct(fields) => fields.iter().map(|field| field.data_type()).collect(),
        DataType::Dictionary(_, value) => vec![value],
        _ => anyhow::bail!("ClickHouse enum container schema drifted: got {data_type:?}"),
    })
}

fn with_nested_types(data_type: &DataType, children: Vec<DataType>) -> anyhow::Result<DataType> {
    let first = || children.first().cloned().ok_or_else(|| anyhow::anyhow!("missing ClickHouse enum container child"));
    Ok(match data_type {
        DataType::List(field) => DataType::List(Arc::new(field.as_ref().clone().with_data_type(first()?))),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(field.as_ref().clone().with_data_type(first()?))),
        DataType::FixedSizeList(field, length) => DataType::FixedSizeList(Arc::new(field.as_ref().clone().with_data_type(first()?)), *length),
        DataType::Map(field, sorted) => DataType::Map(Arc::new(field.as_ref().clone().with_data_type(first()?)), *sorted),
        DataType::Struct(fields) => {
            anyhow::ensure!(fields.len() == children.len(), "ClickHouse enum tuple member count drifted");
            DataType::Struct(fields.iter().zip(children).map(|(field, data_type)| {
                Arc::new(field.as_ref().clone().with_data_type(data_type))
            }).collect())
        }
        DataType::Dictionary(key, _) => DataType::Dictionary(key.clone(), Box::new(first()?)),
        _ => anyhow::bail!("ClickHouse enum container schema drifted: got {data_type:?}"),
    })
}

fn decode_snapshot_string(array: &ArrayRef, path: &str) -> anyhow::Result<ArrayRef> {
    let binary = array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
        anyhow::anyhow!("ClickHouse column '{path}' has invalid binary Arrow storage")
    })?;
    // The default Arrow cast substitutes NULL for invalid UTF-8. String values
    // chosen explicitly by the user must instead decode exactly or fail.
    StringArray::try_from_binary(binary.clone())
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|error| anyhow::anyhow!(
            "ClickHouse column '{path}' cannot be decoded as UTF-8 without losing data: {error}",
        ))
}

fn normalize_snapshot_array(
    array: &ArrayRef,
    expected: &DataType,
    path: &str,
    allow_native_tuple_names: bool,
) -> anyhow::Result<ArrayRef> {
    let actual = array.data_type();
    match (actual, expected) {
        (DataType::Decimal128(actual_precision, actual_scale), DataType::Decimal128(precision, scale))
            if actual_scale == scale && (actual_precision == precision || allow_native_tuple_names) => {
                let values = array.as_any().downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| anyhow::anyhow!("ClickHouse decimal column '{path}' has invalid Arrow storage"))?;
                values.validate_decimal_precision(*precision).map_err(|error| anyhow::anyhow!(
                    "ClickHouse decimal column '{path}' violates declared precision {precision}: {error}"))?;
                if actual == expected {
                    return Ok(Arc::clone(array));
                }
                return Ok(Arc::new(values.clone().with_precision_and_scale(*precision, *scale)?));
            }
        (DataType::Decimal256(actual_precision, actual_scale), DataType::Decimal256(precision, scale))
            if actual_scale == scale && (actual_precision == precision || allow_native_tuple_names) => {
                let values = array.as_any().downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| anyhow::anyhow!("ClickHouse decimal column '{path}' has invalid Arrow storage"))?;
                values.validate_decimal_precision(*precision).map_err(|error| anyhow::anyhow!(
                    "ClickHouse decimal column '{path}' violates declared precision {precision}: {error}"))?;
                if actual == expected {
                    return Ok(Arc::clone(array));
                }
                return Ok(Arc::new(values.clone().with_precision_and_scale(*precision, *scale)?));
            }
        (DataType::UInt8, DataType::Boolean) if allow_native_tuple_names => {
            let values = array.as_any().downcast_ref::<UInt8Array>()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse Boolean column '{path}' has invalid Arrow storage"))?;
            let mut output = BooleanBuilder::with_capacity(array.len());
            for (row, value) in values.iter().enumerate() {
                match value {
                    None => output.append_null(),
                    Some(0) => output.append_value(false),
                    Some(1) => output.append_value(true),
                    Some(value) => anyhow::bail!("ClickHouse Boolean column '{path}' row {row} must be 0 or 1, got {value}"),
                }
            }
            return Ok(Arc::new(output.finish()));
        }
        _ => {}
    }
    if actual == expected && !contains_decimal(expected) {
        return Ok(Arc::clone(array));
    }
    if is_transport_timestamp_representation(actual, expected) {
        ensure_lossless_timestamp_cast(array, expected, path)?;
        return cast_with_options(
            array, expected, &CastOptions { safe: false, ..CastOptions::default() },
        ).map_err(|error| anyhow::anyhow!(
            "ClickHouse timestamp column '{path}' cannot be decoded as {expected:?}: {error}",
        ));
    }
    let data = array.to_data();
    let first_child = || data.child_data().first().ok_or_else(|| {
        anyhow::anyhow!("ClickHouse column '{path}' has invalid Arrow child storage")
    });
    let children = match (actual, expected) {
        (DataType::List(actual), DataType::List(expected))
        | (DataType::LargeList(actual), DataType::LargeList(expected)) => {
            vec![normalize_snapshot_child(
                first_child()?, actual, expected, path, true, allow_native_tuple_names,
            )?]
        }
        (DataType::FixedSizeList(actual, actual_len), DataType::FixedSizeList(expected, expected_len))
            if actual_len == expected_len => {
                vec![normalize_snapshot_child(
                    first_child()?, actual, expected, path, true, allow_native_tuple_names,
                )?]
            }
        (DataType::Map(actual, actual_sorted), DataType::Map(expected, expected_sorted))
            if actual_sorted == expected_sorted => {
                vec![normalize_snapshot_child(
                    first_child()?, actual, expected, path, true, allow_native_tuple_names,
                )?]
            }
        (DataType::Struct(actual), DataType::Struct(expected)) if actual.len() == expected.len() => {
            anyhow::ensure!(
                data.child_data().len() == actual.len(),
                "ClickHouse column '{path}' has invalid Arrow struct storage",
            );
            actual.iter().zip(expected).zip(data.child_data()).enumerate()
                .map(|(index, ((actual, expected), child))| {
                    let positional_name = allow_native_tuple_names
                        && actual.name() == &format!("field_{index}");
                    normalize_snapshot_child(
                        child, actual, expected, path, positional_name, allow_native_tuple_names,
                    )
                }).collect::<anyhow::Result<Vec<_>>>()?
        }
        (DataType::Dictionary(actual_key, _), DataType::Dictionary(expected_key, expected_value))
            if actual_key == expected_key => vec![
                normalize_snapshot_array(
                    &make_array(first_child()?.clone()), expected_value, path, allow_native_tuple_names,
                )?.to_data(),
            ],
        _ => anyhow::bail!(
            "ClickHouse column '{path}' schema drifted: discovered {expected:?}, query returned {actual:?}",
        ),
    };
    if actual == expected {
        // Recursive validation does not require rebuilding unchanged arrays.
        return Ok(Arc::clone(array));
    }
    Ok(make_array(data.into_builder()
        .data_type(expected.clone()).child_data(children).build()?))
}

fn contains_decimal(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_decimal(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_decimal(field.data_type())),
        DataType::Dictionary(_, value) => contains_decimal(value),
        _ => false,
    }
}

fn normalize_snapshot_child(
    child: &ArrayData,
    actual: &Field,
    expected: &Field,
    path: &str,
    transport_name: bool,
    allow_native_tuple_names: bool,
) -> anyhow::Result<ArrayData> {
    anyhow::ensure!(
        // List items and map-entry groups have transport-authored names, not
        // ClickHouse identifiers. Struct members require an exact name unless
        // the caller has validated the full native tuple declaration.
        (actual.name() == expected.name() || transport_name)
            && actual.is_nullable() == expected.is_nullable(),
        "ClickHouse column '{path}' nested schema drifted: discovered '{} nullable={}', query returned '{} nullable={}'",
        expected.name(), expected.is_nullable(), actual.name(), actual.is_nullable(),
    );
    normalize_snapshot_array(
        &make_array(child.clone()), expected.data_type(),
        &format!("{path}.{}", expected.name()), allow_native_tuple_names,
    ).map(|array| array.to_data())
}

fn is_transport_timestamp_representation(actual: &DataType, expected: &DataType) -> bool {
    let DataType::Timestamp(expected_unit, _) = expected else {
        return false;
    };
    let parquet_unit = match expected_unit {
        TimeUnit::Second => TimeUnit::Millisecond,
        unit => *unit,
    };
    matches!(actual, DataType::Timestamp(unit, Some(timezone))
        if timezone.as_ref() == "UTC" && (*unit == *expected_unit || *unit == parquet_unit))
}

fn ensure_lossless_timestamp_cast(
    array: &ArrayRef,
    expected: &DataType,
    column: &str,
) -> anyhow::Result<()> {
    let DataType::Timestamp(actual_unit, _) = array.data_type() else {
        anyhow::bail!("ClickHouse column '{column}' is not a timestamp array")
    };
    let DataType::Timestamp(expected_unit, _) = expected else {
        anyhow::bail!("ClickHouse discovered column '{column}' is not a timestamp")
    };
    if actual_unit == expected_unit {
        return Ok(());
    }
    let actual_scale = timestamp_units_per_second(*actual_unit);
    let expected_scale = timestamp_units_per_second(*expected_unit);
    match actual_unit {
        TimeUnit::Second => check_timestamp_values::<TimestampSecondType>(
            array,
            actual_scale,
            expected_scale,
            column,
        ),
        TimeUnit::Millisecond => check_timestamp_values::<TimestampMillisecondType>(
            array,
            actual_scale,
            expected_scale,
            column,
        ),
        TimeUnit::Microsecond => check_timestamp_values::<TimestampMicrosecondType>(
            array,
            actual_scale,
            expected_scale,
            column,
        ),
        TimeUnit::Nanosecond => check_timestamp_values::<TimestampNanosecondType>(
            array,
            actual_scale,
            expected_scale,
            column,
        ),
    }
}

fn check_timestamp_values<T>(
    array: &ArrayRef,
    actual_scale: i128,
    expected_scale: i128,
    column: &str,
) -> anyhow::Result<()>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    let values = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| {
            anyhow::anyhow!("ClickHouse timestamp column '{column}' has invalid Arrow storage")
        })?;
    for value in values.iter().flatten() {
        let scaled = i128::from(value) * expected_scale;
        anyhow::ensure!(
            scaled % actual_scale == 0,
            "ClickHouse timestamp column '{column}' contains value {value} that cannot be represented losslessly in the discovered timestamp unit"
        );
        i64::try_from(scaled / actual_scale).map_err(|_| {
            anyhow::anyhow!(
                "ClickHouse timestamp column '{column}' contains value {value} that overflows the discovered timestamp unit"
            )
        })?;
    }
    Ok(())
}

const fn timestamp_units_per_second(unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

#[cfg(test)]
#[path = "tests/transport_types.rs"]
mod transport_types;
