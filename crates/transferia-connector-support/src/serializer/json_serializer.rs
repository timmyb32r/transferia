use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use serde::Deserialize;
use transferia_core::data::schema::{
    SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME,
};

const ARROW_JSON_EXTENSION_NAME: &str = "arrow.json";
const MYSQL_BINARY_EXTENSION_NAME: &str = "transferia.mysql.binary";
const MYSQL_DATE_EXTENSION_NAME: &str = "transferia.mysql.date";
const MYSQL_DATETIME_EXTENSION_NAME: &str = "transferia.mysql.datetime";
const MYSQL_DECIMAL_EXTENSION_NAME: &str = "transferia.mysql.decimal";
const MYSQL_ENUM_EXTENSION_NAME: &str = "transferia.mysql.enum";
const MYSQL_FLOAT_EXTENSION_NAME: &str = "transferia.mysql.float";
const MYSQL_SET_EXTENSION_NAME: &str = "transferia.mysql.set";
const MYSQL_SIGNED_INTEGER_EXTENSION_NAME: &str = "transferia.mysql.signed_integer";
const MYSQL_TEXT_BYTES_EXTENSION_NAME: &str = "transferia.mysql.text_bytes";
const MYSQL_TEXT_EXTENSION_NAME: &str = "transferia.mysql.text";
const MYSQL_TIME_EXTENSION_NAME: &str = "transferia.mysql.time";
const MYSQL_TIMESTAMP_EXTENSION_NAME: &str = "transferia.mysql.timestamp";
const MYSQL_UNSIGNED_INTEGER_EXTENSION_NAME: &str = "transferia.mysql.unsigned_integer";
const MYSQL_YEAR_EXTENSION_NAME: &str = "transferia.mysql.year";

/// JSON Lines (NDJSON) serializer: one JSON object per row.
///
/// Output format: `{"column_name": "column_value", ...}\n`
/// This is an independent Arrow-to-NDJSON projection; standard NDJSON readers
/// can consume the output without knowing the source parser configuration.
///
/// Null values are always emitted explicitly as `"col": null`, matching the
/// Confluent S3 JSON format.
///
/// **Optimization**: Column types are pre-classified into internal writer
/// variants when the encoder is constructed. This eliminates per-value
/// `downcast_ref` overhead — the type check happens once per column,
/// not once per value.
/// A pre-classified, optionally projected view of one Arrow batch.
pub struct JsonBatchEncoder {
    columns: Vec<JsonColumnWriter>,

    non_finite_floats: NonFiniteFloatEncoding,
}

#[derive(Clone, Copy)]
enum NonFiniteFloatEncoding {
    Null,
    ProtobufJsonString,
}

pub(crate) struct JsonColumnProjection {
    pub(crate) output_name: String,

    pub(crate) source_index: Option<usize>,
}

struct JsonColumnWriter {
    source_index: Option<usize>,
    output_name: String,
    writer: ColumnWriter,
}

impl JsonBatchEncoder {
    pub fn new(
        batch: &RecordBatch,
        mut include_column: impl FnMut(usize) -> bool,
    ) -> anyhow::Result<Self> {
        let projection = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| include_column(*index))
            .map(|(index, field)| JsonColumnProjection {
                output_name: field.name().to_owned(),
                source_index: Some(index),
            })
            .collect::<Vec<_>>();
        Self::projected(batch, projection)
    }

    pub(crate) fn projected(
        batch: &RecordBatch,
        projection: impl IntoIterator<Item = JsonColumnProjection>,
    ) -> anyhow::Result<Self> {
        Self::projected_with_float_encoding(
            batch,
            projection,
            NonFiniteFloatEncoding::Null,
            false,
        )
    }

    pub(crate) fn projected_debezium(
        batch: &RecordBatch,
        projection: impl IntoIterator<Item = JsonColumnProjection>,
    ) -> anyhow::Result<Self> {
        Self::projected_with_float_encoding(
            batch,
            projection,
            NonFiniteFloatEncoding::ProtobufJsonString,
            false,
        )
    }

    pub(crate) fn projected_debezium_mysql(
        batch: &RecordBatch,
        projection: impl IntoIterator<Item = JsonColumnProjection>,
    ) -> anyhow::Result<Self> {
        Self::projected_with_float_encoding(
            batch,
            projection,
            NonFiniteFloatEncoding::ProtobufJsonString,
            true,
        )
    }

    fn projected_with_float_encoding(
        batch: &RecordBatch,
        projection: impl IntoIterator<Item = JsonColumnProjection>,
        non_finite_floats: NonFiniteFloatEncoding,
        mysql_debezium_extensions: bool,
    ) -> anyhow::Result<Self> {
        let schema = batch.schema();
        let columns = projection
            .into_iter()
            .map(|projection| {
                let writer = if let Some(index) = projection.source_index {
                    let field = schema.field(index);
                    if mysql_debezium_extensions {
                        ColumnWriter::classify_mysql_debezium(
                            field,
                            batch.column(index).as_ref(),
                        )?
                    } else {
                        ColumnWriter::classify(batch.column(index).as_ref()).ok_or_else(|| {
                            anyhow::anyhow!(
                                "JSON serializer: unsupported type {:?} for column '{}'",
                                field.data_type(),
                                field.name(),
                            )
                        })?
                    }
                } else {
                    ColumnWriter::Null
                };
                Ok(JsonColumnWriter {
                    source_index: projection.source_index,
                    output_name: projection.output_name,
                    writer,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            columns,
            non_finite_floats,
        })
    }

    /// Append exactly one compact JSON object followed by a newline.
    pub fn write_row(&self, row: usize, output: &mut Vec<u8>) -> anyhow::Result<()> {
        self.write_object(row, output)?;
        output.push(b'\n');
        Ok(())
    }

    pub(crate) fn write_object(&self, row: usize, output: &mut Vec<u8>) -> anyhow::Result<()> {
        self.write_object_with(row, output, |_, _, _| false)
    }

    pub(crate) fn write_object_with(
        &self,
        row: usize,
        output: &mut Vec<u8>,
        mut override_value: impl FnMut(Option<usize>, &str, &mut Vec<u8>) -> bool,
    ) -> anyhow::Result<()> {
        output.push(b'{');
        for (index, column) in self.columns.iter().enumerate() {
            if index != 0 {
                output.push(b',');
            }
            write_json_string(output, &column.output_name);
            output.push(b':');
            if override_value(column.source_index, &column.output_name, output) {
                continue;
            }
            if column.writer.is_null_at(row) {
                output.extend_from_slice(b"null");
            } else {
                column
                    .writer
                    .write_value(output, row, self.non_finite_floats)?;
            }
        }
        output.push(b'}');
        Ok(())
    }

    pub(crate) fn row_equals(&self, other: &Self, row: usize) -> bool {
        self.columns.len() == other.columns.len()
            && self
                .columns
                .iter()
                .zip(&other.columns)
                .all(|(left, right)| {
                    left.output_name == right.output_name
                        && left.writer.value_equals(&right.writer, row)
                })
    }
}

/// Pre-classified column writer. Arrow array clones retain shared immutable
/// buffers, so the encoder is cheap to own and can outlive the `RecordBatch`
/// wrapper without copying column data.
///
/// Date and Timestamp types map to their integer representation:
///   Date32 → Int32, Date64 → Int64, Timestamp(*) → Int64.
#[derive(Deserialize)]
struct MySqlColumnExtensionMetadata {
    version: u8,
    data_type: String,
    column_type: String,
    unsigned: bool,
    numeric_precision: Option<u64>,
    numeric_scale: Option<u64>,
    datetime_precision: Option<u64>,
    character_set: Option<String>,
    enum_set_values: Option<Vec<String>>,
}

enum MySqlDebeziumEncoding {
    Generic,
    Bit1,
    Cp1252Text,
    PreciseUnsigned64,
    Decimal {
        precision: u32,
        scale: u32,
        unsigned: bool,
    },
    Date,
    DateTime { precision: u32 },
    Timestamp { precision: u32 },
    Time { precision: u32 },
    Year,
    Enum(Arc<[String]>),
    Set(Arc<[String]>),
}

pub(super) fn validate_mysql_debezium_column(column: &SchemaColumn) -> anyhow::Result<()> {
    mysql_debezium_encoding(
        &column.name,
        &column.data_type,
        column.arrow_extension_name,
        column.arrow_extension_metadata.as_deref(),
    )?;
    Ok(())
}

fn mysql_debezium_encoding(
    column_name: &str,
    arrow_type: &DataType,
    extension_name: Option<&str>,
    extension_metadata: Option<&str>,
) -> anyhow::Result<MySqlDebeziumEncoding> {
    let extension_name = extension_name.ok_or_else(|| {
        anyhow::anyhow!(
            "MySQL Debezium column '{column_name}' is missing its physical Arrow extension name"
        )
    })?;
    let extension_metadata = extension_metadata.ok_or_else(|| {
        anyhow::anyhow!(
            "MySQL Debezium column '{column_name}' is missing its physical Arrow extension metadata"
        )
    })?;
    let metadata: MySqlColumnExtensionMetadata = serde_json::from_str(extension_metadata)
        .map_err(|error| {
            anyhow::anyhow!(
                "MySQL Debezium column '{column_name}' has invalid physical extension metadata: {error}"
            )
        })?;
    anyhow::ensure!(
        metadata.version == 1,
        "MySQL Debezium column '{column_name}' requires physical extension metadata version 1, got {}",
        metadata.version
    );
    anyhow::ensure!(
        metadata.data_type == metadata.data_type.to_ascii_lowercase(),
        "MySQL Debezium column '{column_name}' has non-canonical physical data_type '{}'",
        metadata.data_type
    );
    let column_type = metadata.column_type.to_ascii_lowercase();
    anyhow::ensure!(
        !column_type.is_empty()
            && column_type.starts_with(&metadata.data_type)
            && column_type
                .as_bytes()
                .get(metadata.data_type.len())
                .is_none_or(|byte| matches!(*byte, b'(' | b' ')),
        "MySQL Debezium column '{column_name}' physical column_type '{}' disagrees with data_type '{}'",
        metadata.column_type,
        metadata.data_type
    );

    let (expected_extension, expected_arrow_type, encoding) = match metadata.data_type.as_str() {
        "tinyint" => (
            if metadata.unsigned {
                MYSQL_UNSIGNED_INTEGER_EXTENSION_NAME
            } else {
                MYSQL_SIGNED_INTEGER_EXTENSION_NAME
            },
            if metadata.unsigned {
                DataType::UInt8
            } else {
                DataType::Int8
            },
            MySqlDebeziumEncoding::Generic,
        ),
        "smallint" => (
            if metadata.unsigned {
                MYSQL_UNSIGNED_INTEGER_EXTENSION_NAME
            } else {
                MYSQL_SIGNED_INTEGER_EXTENSION_NAME
            },
            if metadata.unsigned {
                DataType::UInt16
            } else {
                DataType::Int16
            },
            MySqlDebeziumEncoding::Generic,
        ),
        "mediumint" | "int" | "integer" => (
            if metadata.unsigned {
                MYSQL_UNSIGNED_INTEGER_EXTENSION_NAME
            } else {
                MYSQL_SIGNED_INTEGER_EXTENSION_NAME
            },
            if metadata.unsigned {
                DataType::UInt32
            } else {
                DataType::Int32
            },
            MySqlDebeziumEncoding::Generic,
        ),
        "bigint" => (
            if metadata.unsigned {
                MYSQL_UNSIGNED_INTEGER_EXTENSION_NAME
            } else {
                MYSQL_SIGNED_INTEGER_EXTENSION_NAME
            },
            if metadata.unsigned {
                DataType::UInt64
            } else {
                DataType::Int64
            },
            if metadata.unsigned {
                MySqlDebeziumEncoding::PreciseUnsigned64
            } else {
                MySqlDebeziumEncoding::Generic
            },
        ),
        "float" => (
            MYSQL_FLOAT_EXTENSION_NAME,
            DataType::Float32,
            MySqlDebeziumEncoding::Generic,
        ),
        "double" | "real" => (
            MYSQL_FLOAT_EXTENSION_NAME,
            DataType::Float64,
            MySqlDebeziumEncoding::Generic,
        ),
        "bit" => {
            let precision = metadata.numeric_precision.ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL Debezium BIT column '{column_name}' omits numeric_precision"
                )
            })?;
            anyhow::ensure!(
                (1..=64).contains(&precision),
                "MySQL Debezium BIT column '{column_name}' has invalid precision {precision}"
            );
            (
                MYSQL_BINARY_EXTENSION_NAME,
                DataType::Binary,
                if precision == 1 {
                    MySqlDebeziumEncoding::Bit1
                } else {
                    MySqlDebeziumEncoding::Generic
                },
            )
        }
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => (
            MYSQL_BINARY_EXTENSION_NAME,
            DataType::Binary,
            MySqlDebeziumEncoding::Generic,
        ),
        "vector" => anyhow::bail!(
            "MySQL Debezium column '{column_name}' uses VECTOR, whose exact FloatVector representation is not supported"
        ),
        "geometry" | "point" | "linestring" | "polygon" | "multipoint"
        | "multilinestring" | "multipolygon" | "geometrycollection" => anyhow::bail!(
            "MySQL Debezium column '{column_name}' uses spatial type '{}', whose exact Debezium struct representation is not supported",
            metadata.data_type
        ),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
        | "inet4" | "inet6" | "uuid" => {
            let character_set = metadata.character_set.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL Debezium text column '{column_name}' omits its character_set"
                )
            })?;
            if matches!(character_set, "ascii" | "utf8mb3" | "utf8mb4") {
                (
                    MYSQL_TEXT_EXTENSION_NAME,
                    DataType::Utf8,
                    MySqlDebeziumEncoding::Generic,
                )
            } else {
                anyhow::ensure!(
                    character_set == "latin1",
                    "MySQL Debezium text column '{column_name}' has unsupported character set '{character_set}'"
                );
                (
                    MYSQL_TEXT_BYTES_EXTENSION_NAME,
                    DataType::Binary,
                    MySqlDebeziumEncoding::Cp1252Text,
                )
            }
        }
        "json" => (
            ARROW_JSON_EXTENSION_NAME,
            DataType::Utf8,
            MySqlDebeziumEncoding::Generic,
        ),
        "decimal" | "numeric" => {
            let precision = metadata.numeric_precision.ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL Debezium decimal column '{column_name}' omits numeric_precision"
                )
            })?;
            let scale = metadata.numeric_scale.ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL Debezium decimal column '{column_name}' omits numeric_scale"
                )
            })?;
            anyhow::ensure!(
                (1..=65).contains(&precision) && scale <= precision,
                "MySQL Debezium decimal column '{column_name}' has invalid precision/scale {precision}/{scale}"
            );
            (
                MYSQL_DECIMAL_EXTENSION_NAME,
                DataType::Utf8,
                MySqlDebeziumEncoding::Decimal {
                    precision: u32::try_from(precision)?,
                    scale: u32::try_from(scale).map_err(|_| {
                        anyhow::anyhow!(
                            "MySQL Debezium decimal column '{column_name}' scale {scale} is too large"
                        )
                    })?,
                    unsigned: metadata.unsigned,
                },
            )
        }
        "date" => (
            MYSQL_DATE_EXTENSION_NAME,
            DataType::Utf8,
            MySqlDebeziumEncoding::Date,
        ),
        "datetime" | "timestamp" | "time" => {
            let precision = metadata.datetime_precision.ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL Debezium temporal column '{column_name}' omits datetime_precision"
                )
            })?;
            anyhow::ensure!(
                precision <= 6,
                "MySQL Debezium temporal column '{column_name}' has unsupported precision {precision}"
            );
            let precision = u32::try_from(precision)?;
            if metadata.data_type == "datetime" {
                (
                    MYSQL_DATETIME_EXTENSION_NAME,
                    DataType::Utf8,
                    MySqlDebeziumEncoding::DateTime { precision },
                )
            } else if metadata.data_type == "timestamp" {
                (
                    MYSQL_TIMESTAMP_EXTENSION_NAME,
                    DataType::Utf8,
                    MySqlDebeziumEncoding::Timestamp { precision },
                )
            } else {
                (
                    MYSQL_TIME_EXTENSION_NAME,
                    DataType::Utf8,
                    MySqlDebeziumEncoding::Time { precision },
                )
            }
        }
        "year" => (
            MYSQL_YEAR_EXTENSION_NAME,
            DataType::Utf8,
            MySqlDebeziumEncoding::Year,
        ),
        "enum" => {
            let members = metadata.enum_set_values.ok_or_else(|| {
                anyhow::anyhow!("MySQL Debezium ENUM column '{column_name}' omits its members")
            })?;
            anyhow::ensure!(
                members.len() <= usize::from(u16::MAX),
                "MySQL Debezium ENUM column '{column_name}' has too many members"
            );
            validate_member_projection(column_name, "ENUM", &members, false)?;
            (
                MYSQL_ENUM_EXTENSION_NAME,
                DataType::UInt16,
                MySqlDebeziumEncoding::Enum(Arc::from(members)),
            )
        }
        "set" => {
            let members = metadata.enum_set_values.ok_or_else(|| {
                anyhow::anyhow!("MySQL Debezium SET column '{column_name}' omits its members")
            })?;
            anyhow::ensure!(
                members.len() <= 64,
                "MySQL Debezium SET column '{column_name}' has more than 64 members"
            );
            validate_member_projection(column_name, "SET", &members, true)?;
            (
                MYSQL_SET_EXTENSION_NAME,
                DataType::UInt64,
                MySqlDebeziumEncoding::Set(Arc::from(members)),
            )
        }
        unsupported => anyhow::bail!(
            "MySQL Debezium column '{column_name}' has unsupported physical data_type '{unsupported}'"
        ),
    };
    anyhow::ensure!(
        extension_name == expected_extension,
        "MySQL Debezium column '{column_name}' physical type '{}' requires Arrow extension '{expected_extension}', got '{extension_name}'",
        metadata.data_type
    );
    anyhow::ensure!(
        arrow_type == &expected_arrow_type,
        "MySQL Debezium column '{column_name}' physical type '{}' requires Arrow {expected_arrow_type:?}, got {arrow_type:?}",
        metadata.data_type
    );
    Ok(encoding)
}

fn validate_member_projection(
    column_name: &str,
    family: &str,
    members: &[String],
    reject_comma: bool,
) -> anyhow::Result<()> {
    let mut unique = std::collections::HashSet::with_capacity(members.len());
    for member in members {
        anyhow::ensure!(
            !member.is_empty(),
            "MySQL Debezium {family} column '{column_name}' has an empty member, which is not losslessly distinguishable in member-text output"
        );
        anyhow::ensure!(
            !reject_comma || !member.contains(','),
            "MySQL Debezium {family} column '{column_name}' has comma-containing member '{member}', which is not losslessly distinguishable in member-text output"
        );
        anyhow::ensure!(
            unique.insert(member),
            "MySQL Debezium {family} column '{column_name}' has duplicate member '{member}'"
        );
    }
    Ok(())
}

enum ColumnWriter {
    Null,
    Utf8(arrow::array::StringArray),
    LargeUtf8(arrow::array::LargeStringArray),
    Binary(arrow::array::BinaryArray),
    LargeBinary(arrow::array::LargeBinaryArray),
    FixedSizeBinary(arrow::array::FixedSizeBinaryArray),
    Int8(arrow::array::Int8Array),
    Int16(arrow::array::Int16Array),
    Int32(arrow::array::Int32Array),
    Int64(arrow::array::Int64Array),
    UInt8(arrow::array::UInt8Array),
    UInt16(arrow::array::UInt16Array),
    UInt32(arrow::array::UInt32Array),
    UInt64(arrow::array::UInt64Array),
    Float32(arrow::array::Float32Array),
    Float64(arrow::array::Float64Array),
    Boolean(arrow::array::BooleanArray),
    Date32(arrow::array::Date32Array),
    Date64(arrow::array::Date64Array),
    TimestampSecond(arrow::array::TimestampSecondArray),
    TimestampMillisecond(arrow::array::TimestampMillisecondArray),
    TimestampMicrosecond(arrow::array::TimestampMicrosecondArray),
    TimestampNanosecond(arrow::array::TimestampNanosecondArray),
    MySqlBit1(arrow::array::BinaryArray),
    MySqlCp1252Text(arrow::array::BinaryArray),
    MySqlPreciseUnsigned64(arrow::array::UInt64Array),
    MySqlDecimal {
        values: arrow::array::StringArray,
        precision: u32,
        scale: u32,
        unsigned: bool,
    },
    MySqlDate(arrow::array::StringArray),
    MySqlDateTime {
        values: arrow::array::StringArray,
        precision: u32,
    },
    MySqlTimestamp {
        values: arrow::array::StringArray,
        precision: u32,
    },
    MySqlTime {
        values: arrow::array::StringArray,
        precision: u32,
    },
    MySqlYear(arrow::array::StringArray),
    MySqlEnum {
        values: arrow::array::UInt16Array,
        members: Arc<[String]>,
    },
    MySqlSet {
        values: arrow::array::UInt64Array,
        members: Arc<[String]>,
    },
}

fn mysql_string_array(
    field: &arrow::datatypes::Field,
    array: &dyn Array,
) -> anyhow::Result<arrow::array::StringArray> {
    array
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "MySQL Debezium column '{}' has the wrong runtime Arrow array",
                field.name()
            )
        })
}

impl ColumnWriter {
    fn classify_mysql_debezium(
        field: &arrow::datatypes::Field,
        array: &dyn Array,
    ) -> anyhow::Result<Self> {
        let encoding = mysql_debezium_encoding(
            field.name(),
            field.data_type(),
            field
                .metadata()
                .get(META_ARROW_EXTENSION_NAME)
                .map(String::as_str),
            field
                .metadata()
                .get(META_ARROW_EXTENSION_METADATA)
                .map(String::as_str),
        )?;
        match encoding {
            MySqlDebeziumEncoding::Generic => Self::classify(array).ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL Debezium serializer: unsupported Arrow type {:?} for column '{}'",
                    field.data_type(),
                    field.name(),
                )
            }),
            MySqlDebeziumEncoding::Bit1 => Ok(Self::MySqlBit1(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL Debezium BIT(1) column '{}' has the wrong runtime Arrow array",
                            field.name()
                        )
                    })?,
            )),
            MySqlDebeziumEncoding::Cp1252Text => Ok(Self::MySqlCp1252Text(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL Debezium latin1 column '{}' has the wrong runtime Arrow array",
                            field.name()
                        )
                    })?,
            )),
            MySqlDebeziumEncoding::PreciseUnsigned64 => Ok(Self::MySqlPreciseUnsigned64(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL Debezium BIGINT UNSIGNED column '{}' has the wrong runtime Arrow array",
                            field.name()
                        )
                    })?,
            )),
            MySqlDebeziumEncoding::Decimal {
                precision,
                scale,
                unsigned,
            } => Ok(Self::MySqlDecimal {
                values: mysql_string_array(field, array)?,
                precision,
                scale,
                unsigned,
            }),
            MySqlDebeziumEncoding::Date => {
                Ok(Self::MySqlDate(mysql_string_array(field, array)?))
            }
            MySqlDebeziumEncoding::DateTime { precision } => Ok(Self::MySqlDateTime {
                values: mysql_string_array(field, array)?,
                precision,
            }),
            MySqlDebeziumEncoding::Timestamp { precision } => Ok(Self::MySqlTimestamp {
                values: mysql_string_array(field, array)?,
                precision,
            }),
            MySqlDebeziumEncoding::Time { precision } => Ok(Self::MySqlTime {
                values: mysql_string_array(field, array)?,
                precision,
            }),
            MySqlDebeziumEncoding::Year => {
                Ok(Self::MySqlYear(mysql_string_array(field, array)?))
            }
            MySqlDebeziumEncoding::Enum(members) => Ok(Self::MySqlEnum {
                values: array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt16Array>()
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL Debezium ENUM column '{}' has the wrong runtime Arrow array",
                            field.name()
                        )
                    })?,
                members,
            }),
            MySqlDebeziumEncoding::Set(members) => Ok(Self::MySqlSet {
                values: array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL Debezium SET column '{}' has the wrong runtime Arrow array",
                            field.name()
                        )
                    })?,
                members,
            }),
        }
    }

    /// Classify an Arrow array into the appropriate writer variant.
    /// Returns `None` for unsupported types.
    fn classify(array: &dyn Array) -> Option<Self> {
        use arrow::array::{
            BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array,
            Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
            StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
        };

        let dt = array.data_type();
        let writer = match *dt {
            DataType::Utf8 => {
                let a = array.as_any().downcast_ref::<StringArray>()?;
                Self::Utf8(a.clone())
            }
            DataType::LargeUtf8 => {
                let a = array.as_any().downcast_ref::<LargeStringArray>()?;
                Self::LargeUtf8(a.clone())
            }
            DataType::Binary => {
                let a = array.as_any().downcast_ref::<BinaryArray>()?;
                Self::Binary(a.clone())
            }
            DataType::LargeBinary => {
                let a = array.as_any().downcast_ref::<LargeBinaryArray>()?;
                Self::LargeBinary(a.clone())
            }
            DataType::FixedSizeBinary(_) => {
                let a = array.as_any().downcast_ref::<FixedSizeBinaryArray>()?;
                Self::FixedSizeBinary(a.clone())
            }
            DataType::Int8 => {
                let a = array.as_any().downcast_ref::<Int8Array>()?;
                Self::Int8(a.clone())
            }
            DataType::Int16 => {
                let a = array.as_any().downcast_ref::<Int16Array>()?;
                Self::Int16(a.clone())
            }
            DataType::Int32 => {
                let a = array.as_any().downcast_ref::<Int32Array>()?;
                Self::Int32(a.clone())
            }
            DataType::Int64 => {
                let a = array.as_any().downcast_ref::<Int64Array>()?;
                Self::Int64(a.clone())
            }
            DataType::Date32 => Self::Date32(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::Date32Array>()?
                    .clone(),
            ),
            DataType::Date64 => Self::Date64(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::Date64Array>()?
                    .clone(),
            ),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => Self::TimestampSecond(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()?
                    .clone(),
            ),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                Self::TimestampMillisecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMillisecondArray>()?
                        .clone(),
                )
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                Self::TimestampMicrosecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()?
                        .clone(),
                )
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                Self::TimestampNanosecond(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampNanosecondArray>()?
                        .clone(),
                )
            }
            DataType::UInt8 => {
                let a = array.as_any().downcast_ref::<UInt8Array>()?;
                Self::UInt8(a.clone())
            }
            DataType::UInt16 => {
                let a = array.as_any().downcast_ref::<UInt16Array>()?;
                Self::UInt16(a.clone())
            }
            DataType::UInt32 => {
                let a = array.as_any().downcast_ref::<UInt32Array>()?;
                Self::UInt32(a.clone())
            }
            DataType::UInt64 => {
                let a = array.as_any().downcast_ref::<UInt64Array>()?;
                Self::UInt64(a.clone())
            }
            DataType::Float32 => {
                let a = array.as_any().downcast_ref::<Float32Array>()?;
                Self::Float32(a.clone())
            }
            DataType::Float64 => {
                let a = array.as_any().downcast_ref::<Float64Array>()?;
                Self::Float64(a.clone())
            }
            DataType::Boolean => {
                let a = array.as_any().downcast_ref::<BooleanArray>()?;
                Self::Boolean(a.clone())
            }
            DataType::Null
            | DataType::Float16
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::BinaryView
            | DataType::Utf8View
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::FixedSizeList(..)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_)
            | DataType::Union(..)
            | DataType::Dictionary(..)
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Map(..)
            | DataType::RunEndEncoded(..) => return None,
        };
        Some(writer)
    }

    /// Write the value at the given row index into the buffer.
    /// No dynamic dispatch: the variant is pre-determined.
    #[inline]
    fn write_value(
        &self,
        buf: &mut Vec<u8>,
        row: usize,
        non_finite_floats: NonFiniteFloatEncoding,
    ) -> anyhow::Result<()> {
        match self {
            Self::Null => buf.extend_from_slice(b"null"),
            Self::Utf8(a) => write_json_string(buf, a.value(row)),
            Self::LargeUtf8(a) => write_json_string(buf, a.value(row)),
            Self::Binary(a) => write_base64(buf, a.value(row))?,
            Self::LargeBinary(a) => write_base64(buf, a.value(row))?,
            Self::FixedSizeBinary(a) => write_base64(buf, a.value(row))?,
            Self::Int8(a) => write_int(buf, a.value(row)),
            Self::Int16(a) => write_int(buf, a.value(row)),
            Self::Int32(a) => write_int(buf, a.value(row)),
            Self::Int64(a) => write_int(buf, a.value(row)),
            Self::UInt8(a) => write_uint(buf, a.value(row)),
            Self::UInt16(a) => write_uint(buf, a.value(row)),
            Self::UInt32(a) => write_uint(buf, a.value(row)),
            Self::UInt64(a) => write_uint(buf, a.value(row)),
            Self::Float32(a) => {
                write_float(buf, a.value(row), non_finite_floats);
            }
            Self::Float64(a) => {
                write_float(buf, a.value(row), non_finite_floats);
            }
            Self::Boolean(a) => {
                buf.extend_from_slice(if a.value(row) { b"true" } else { b"false" });
            }
            Self::Date32(a) => write_int(buf, a.value(row)),
            Self::Date64(a) => write_int(buf, a.value(row)),
            Self::TimestampSecond(a) => write_int(buf, a.value(row)),
            Self::TimestampMillisecond(a) => write_int(buf, a.value(row)),
            Self::TimestampMicrosecond(a) => write_int(buf, a.value(row)),
            Self::TimestampNanosecond(a) => write_int(buf, a.value(row)),
            Self::MySqlBit1(a) => {
                let value = a.value(row);
                anyhow::ensure!(
                    value.len() == 1 && value[0] <= 1,
                    "MySQL Debezium BIT(1) value must be exactly one byte containing 0 or 1"
                );
                buf.extend_from_slice(if value[0] == 1 { b"true" } else { b"false" });
            }
            Self::MySqlCp1252Text(a) => write_cp1252_json_string(buf, a.value(row))?,
            Self::MySqlPreciseUnsigned64(a) => {
                let value = bigdecimal::num_bigint::BigInt::from(a.value(row));
                write_base64(buf, &value.to_signed_bytes_be())?;
            }
            Self::MySqlDecimal {
                values,
                precision,
                scale,
                unsigned,
            } => {
                write_mysql_decimal(
                    buf,
                    values.value(row),
                    *precision,
                    *scale,
                    *unsigned,
                )?;
            }
            Self::MySqlDate(a) => write_int(buf, mysql_date_days(a.value(row))?),
            Self::MySqlDateTime { values, precision } => {
                write_int(
                    buf,
                    mysql_datetime_timestamp(values.value(row), *precision)?,
                );
            }
            Self::MySqlTimestamp { values, precision } => {
                write_mysql_zoned_timestamp(buf, values.value(row), *precision)?;
            }
            Self::MySqlTime { values, precision } => {
                write_int(buf, mysql_time_timestamp(values.value(row), *precision)?);
            }
            Self::MySqlYear(a) => write_int(buf, mysql_year(a.value(row))?),
            Self::MySqlEnum { values, members } => {
                let ordinal = usize::from(values.value(row));
                let value = if ordinal == 0 {
                    ""
                } else {
                    members.get(ordinal - 1).ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL Debezium ENUM ordinal {ordinal} exceeds {} declared members",
                            members.len()
                        )
                    })?
                };
                write_json_string(buf, value);
            }
            Self::MySqlSet { values, members } => {
                let bits = values.value(row);
                let known_bits = if members.len() == 64 {
                    u64::MAX
                } else {
                    (1_u64 << members.len()) - 1
                };
                anyhow::ensure!(
                    bits & !known_bits == 0,
                    "MySQL Debezium SET bits {bits:#x} exceed {} declared members",
                    members.len()
                );
                buf.push(b'"');
                let mut first = true;
                for (index, member) in members.iter().enumerate() {
                    if bits & (1_u64 << index) == 0 {
                        continue;
                    }
                    if !first {
                        buf.push(b',');
                    }
                    write_json_string_contents(buf, member);
                    first = false;
                }
                buf.push(b'"');
            }
        }
        Ok(())
    }

    /// Check if the value is null at the given row.
    #[inline]
    fn is_null_at(&self, row: usize) -> bool {
        match self {
            Self::Null => true,
            Self::Utf8(a) => a.is_null(row),
            Self::LargeUtf8(a) => a.is_null(row),
            Self::Binary(a) => a.is_null(row),
            Self::LargeBinary(a) => a.is_null(row),
            Self::FixedSizeBinary(a) => a.is_null(row),
            Self::Int8(a) => a.is_null(row),
            Self::Int16(a) => a.is_null(row),
            Self::Int32(a) => a.is_null(row),
            Self::Int64(a) => a.is_null(row),
            Self::UInt8(a) => a.is_null(row),
            Self::UInt16(a) => a.is_null(row),
            Self::UInt32(a) => a.is_null(row),
            Self::UInt64(a) => a.is_null(row),
            Self::Float32(a) => a.is_null(row),
            Self::Float64(a) => a.is_null(row),
            Self::Boolean(a) => a.is_null(row),
            Self::Date32(a) => a.is_null(row),
            Self::Date64(a) => a.is_null(row),
            Self::TimestampSecond(a) => a.is_null(row),
            Self::TimestampMillisecond(a) => a.is_null(row),
            Self::TimestampMicrosecond(a) => a.is_null(row),
            Self::TimestampNanosecond(a) => a.is_null(row),
            Self::MySqlBit1(a) => a.is_null(row),
            Self::MySqlCp1252Text(a) => a.is_null(row),
            Self::MySqlPreciseUnsigned64(a) => a.is_null(row),
            Self::MySqlDecimal { values, .. }
            | Self::MySqlDateTime { values, .. }
            | Self::MySqlTimestamp { values, .. }
            | Self::MySqlTime { values, .. } => values.is_null(row),
            Self::MySqlDate(a) | Self::MySqlYear(a) => a.is_null(row),
            Self::MySqlEnum { values, .. } => values.is_null(row),
            Self::MySqlSet { values, .. } => values.is_null(row),
        }
    }

    fn value_equals(&self, other: &Self, row: usize) -> bool {
        if self.is_null_at(row) || other.is_null_at(row) {
            return self.is_null_at(row) == other.is_null_at(row);
        }
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Utf8(left), Self::Utf8(right)) => left.value(row) == right.value(row),
            (Self::LargeUtf8(left), Self::LargeUtf8(right)) => left.value(row) == right.value(row),
            (Self::Binary(left), Self::Binary(right)) => left.value(row) == right.value(row),
            (Self::LargeBinary(left), Self::LargeBinary(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::FixedSizeBinary(left), Self::FixedSizeBinary(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::Int8(left), Self::Int8(right)) => left.value(row) == right.value(row),
            (Self::Int16(left), Self::Int16(right)) => left.value(row) == right.value(row),
            (Self::Int32(left), Self::Int32(right)) => left.value(row) == right.value(row),
            (Self::Int64(left), Self::Int64(right)) => left.value(row) == right.value(row),
            (Self::UInt8(left), Self::UInt8(right)) => left.value(row) == right.value(row),
            (Self::UInt16(left), Self::UInt16(right)) => left.value(row) == right.value(row),
            (Self::UInt32(left), Self::UInt32(right)) => left.value(row) == right.value(row),
            (Self::UInt64(left), Self::UInt64(right)) => left.value(row) == right.value(row),
            (Self::Float32(left), Self::Float32(right)) => {
                left.value(row).to_bits() == right.value(row).to_bits()
            }
            (Self::Float64(left), Self::Float64(right)) => {
                left.value(row).to_bits() == right.value(row).to_bits()
            }
            (Self::Boolean(left), Self::Boolean(right)) => left.value(row) == right.value(row),
            (Self::Date32(left), Self::Date32(right)) => left.value(row) == right.value(row),
            (Self::Date64(left), Self::Date64(right)) => left.value(row) == right.value(row),
            (Self::TimestampSecond(left), Self::TimestampSecond(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::TimestampMillisecond(left), Self::TimestampMillisecond(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::TimestampMicrosecond(left), Self::TimestampMicrosecond(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::TimestampNanosecond(left), Self::TimestampNanosecond(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::MySqlBit1(left), Self::MySqlBit1(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::MySqlCp1252Text(left), Self::MySqlCp1252Text(right)) => {
                left.value(row) == right.value(row)
            }
            (Self::MySqlPreciseUnsigned64(left), Self::MySqlPreciseUnsigned64(right)) => {
                left.value(row) == right.value(row)
            }
            (
                Self::MySqlDecimal {
                    values: left,
                    precision: left_precision,
                    scale: left_scale,
                    unsigned: left_unsigned,
                },
                Self::MySqlDecimal {
                    values: right,
                    precision: right_precision,
                    scale: right_scale,
                    unsigned: right_unsigned,
                },
            ) => {
                left_precision == right_precision
                    && left_scale == right_scale
                    && left_unsigned == right_unsigned
                    && left.value(row) == right.value(row)
            }
            (Self::MySqlDate(left), Self::MySqlDate(right))
            | (Self::MySqlYear(left), Self::MySqlYear(right)) => {
                left.value(row) == right.value(row)
            }
            (
                Self::MySqlDateTime {
                    values: left,
                    precision: left_precision,
                },
                Self::MySqlDateTime {
                    values: right,
                    precision: right_precision,
                },
            )
            | (
                Self::MySqlTimestamp {
                    values: left,
                    precision: left_precision,
                },
                Self::MySqlTimestamp {
                    values: right,
                    precision: right_precision,
                },
            )
            | (
                Self::MySqlTime {
                    values: left,
                    precision: left_precision,
                },
                Self::MySqlTime {
                    values: right,
                    precision: right_precision,
                },
            ) => left_precision == right_precision && left.value(row) == right.value(row),
            (
                Self::MySqlEnum {
                    values: left_values,
                    members: left_members,
                },
                Self::MySqlEnum {
                    values: right_values,
                    members: right_members,
                },
            ) => {
                left_members == right_members && left_values.value(row) == right_values.value(row)
            }
            (
                Self::MySqlSet {
                    values: left_values,
                    members: left_members,
                },
                Self::MySqlSet {
                    values: right_values,
                    members: right_members,
                },
            ) => {
                left_members == right_members && left_values.value(row) == right_values.value(row)
            }
            _ => false,
        }
    }
}

fn write_mysql_decimal(
    buf: &mut Vec<u8>,
    value: &str,
    precision: u32,
    scale: u32,
    unsigned: bool,
) -> anyhow::Result<()> {
    let unsigned_value = value.strip_prefix('-').unwrap_or(value);
    let (integer, fractional) = if scale == 0 {
        anyhow::ensure!(
            !unsigned_value.contains('.'),
            "MySQL Debezium decimal value '{value}' has a fraction but declared scale is zero"
        );
        (unsigned_value, None)
    } else {
        let (integer, fractional) = unsigned_value.split_once('.').ok_or_else(|| {
            anyhow::anyhow!(
                "MySQL Debezium decimal value '{value}' omits its declared scale {scale}"
            )
        })?;
        anyhow::ensure!(
            fractional.len() == usize::try_from(scale)?
                && fractional.bytes().all(|byte| byte.is_ascii_digit()),
            "MySQL Debezium decimal value '{value}' does not match declared scale {scale}"
        );
        (integer, Some(fractional))
    };
    anyhow::ensure!(
        !integer.is_empty()
            && integer.bytes().all(|byte| byte.is_ascii_digit())
            && fractional.is_none_or(|digits| digits.bytes().all(|byte| byte.is_ascii_digit())),
        "MySQL Debezium decimal value '{value}' is not canonical decimal text"
    );
    let decimal = value.parse::<bigdecimal::BigDecimal>().map_err(|error| {
        anyhow::anyhow!("MySQL Debezium decimal value '{value}' is malformed: {error}")
    })?;
    let (unscaled, actual_scale) = decimal.as_bigint_and_exponent();
    anyhow::ensure!(
        actual_scale == i64::from(scale),
        "MySQL Debezium decimal value '{value}' has scale {actual_scale}, expected {scale}"
    );
    anyhow::ensure!(
        decimal.digits() <= u64::from(precision),
        "MySQL Debezium decimal value '{value}' exceeds declared precision {precision}"
    );
    anyhow::ensure!(
        !unsigned || unscaled.sign() != bigdecimal::num_bigint::Sign::Minus,
        "MySQL Debezium unsigned decimal value '{value}' is negative"
    );
    let mut bytes = unscaled.to_signed_bytes_be();
    if bytes.is_empty() {
        bytes.push(0);
    }
    write_base64(buf, &bytes)
}

fn mysql_date_days(value: &str) -> anyhow::Result<i64> {
    use chrono::Datelike as _;

    anyhow::ensure!(
        value.len() == 10,
        "MySQL Debezium DATE value '{value}' is not a complete YYYY-MM-DD date"
    );
    let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|error| {
        anyhow::anyhow!("MySQL Debezium DATE value '{value}' is invalid: {error}")
    })?;
    anyhow::ensure!(
        date.year() > 0,
        "MySQL Debezium DATE value '{value}' cannot represent a zero year"
    );
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| anyhow::anyhow!("internal error: Unix epoch date is invalid"))?;
    Ok(date.signed_duration_since(epoch).num_days())
}

fn mysql_datetime_timestamp(value: &str, precision: u32) -> anyhow::Result<i64> {
    use chrono::Datelike as _;

    validate_temporal_shape(value, 19, precision, "DATETIME/TIMESTAMP")?;
    let format = if precision == 0 {
        "%Y-%m-%d %H:%M:%S"
    } else {
        "%Y-%m-%d %H:%M:%S%.f"
    };
    let value = chrono::NaiveDateTime::parse_from_str(value, format).map_err(|error| {
        anyhow::anyhow!("MySQL Debezium DATETIME/TIMESTAMP value '{value}' is invalid: {error}")
    })?;
    anyhow::ensure!(
        value.year() > 0,
        "MySQL Debezium DATETIME/TIMESTAMP value '{value}' cannot represent a zero year"
    );
    Ok(if precision <= 3 {
        value.and_utc().timestamp_millis()
    } else {
        value.and_utc().timestamp_micros()
    })
}

fn mysql_time_timestamp(value: &str, precision: u32) -> anyhow::Result<i64> {
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |unsigned| (true, unsigned));
    let (clock, fractional) = if precision == 0 {
        anyhow::ensure!(
            !unsigned.contains('.'),
            "MySQL Debezium TIME value '{value}' has a fraction but declared precision is zero"
        );
        (unsigned, None)
    } else {
        let (clock, fractional) = unsigned.split_once('.').ok_or_else(|| {
            anyhow::anyhow!(
                "MySQL Debezium TIME value '{value}' omits its declared fractional precision"
            )
        })?;
        anyhow::ensure!(
            fractional.len() == usize::try_from(precision)?
                && fractional.bytes().all(|byte| byte.is_ascii_digit()),
            "MySQL Debezium TIME value '{value}' does not match declared precision {precision}"
        );
        (clock, Some(fractional))
    };
    let components = clock.split(':').collect::<Vec<_>>();
    anyhow::ensure!(
        components.len() == 3
            && (2..=3).contains(&components[0].len())
            && components[1].len() == 2
            && components[2].len() == 2
            && components
                .iter()
                .all(|part| part.bytes().all(|byte| byte.is_ascii_digit())),
        "MySQL Debezium TIME value '{value}' is not a complete signed duration"
    );
    let hours = components[0].parse::<u64>()?;
    let minutes = components[1].parse::<u64>()?;
    let seconds = components[2].parse::<u64>()?;
    anyhow::ensure!(
        hours <= 838 && minutes <= 59 && seconds <= 59,
        "MySQL Debezium TIME value '{value}' is outside the MySQL duration range"
    );
    let fractional_micros = if let Some(fractional) = fractional {
        let digits = fractional.parse::<u64>()?;
        digits
            .checked_mul(10_u64.pow(6 - precision))
            .ok_or_else(|| anyhow::anyhow!("MySQL Debezium TIME fraction overflow"))?
    } else {
        0
    };
    let micros = hours
        .checked_mul(3_600_000_000)
        .and_then(|value| value.checked_add(minutes * 60_000_000))
        .and_then(|value| value.checked_add(seconds * 1_000_000))
        .and_then(|value| value.checked_add(fractional_micros))
        .ok_or_else(|| anyhow::anyhow!("MySQL Debezium TIME value overflow"))?;
    anyhow::ensure!(
        !negative || micros != 0,
        "MySQL Debezium TIME value '{value}' is a non-injective negative zero"
    );
    let value = i64::try_from(micros)?;
    Ok(if negative { -value } else { value })
}

fn write_mysql_zoned_timestamp(
    buf: &mut Vec<u8>,
    value: &str,
    precision: u32,
) -> anyhow::Result<()> {
    validate_temporal_shape(value, 19, precision, "TIMESTAMP")?;
    let format = if precision == 0 {
        "%Y-%m-%d %H:%M:%S"
    } else {
        "%Y-%m-%d %H:%M:%S%.f"
    };
    let timestamp = chrono::NaiveDateTime::parse_from_str(value, format).map_err(|error| {
        anyhow::anyhow!("MySQL Debezium TIMESTAMP value '{value}' is invalid: {error}")
    })?;
    use chrono::Datelike as _;
    anyhow::ensure!(
        timestamp.year() > 0,
        "MySQL Debezium TIMESTAMP value '{value}' cannot represent a zero year"
    );
    buf.push(b'"');
    write_json_string_contents(buf, &value[..10]);
    buf.push(b'T');
    write_json_string_contents(buf, &value[11..]);
    buf.extend_from_slice(b"Z\"");
    Ok(())
}

fn mysql_year(value: &str) -> anyhow::Result<i32> {
    anyhow::ensure!(
        value.len() == 4 && value.bytes().all(|byte| byte.is_ascii_digit()),
        "MySQL Debezium YEAR value '{value}' is not a complete four-digit year"
    );
    let year = value.parse::<i32>()?;
    anyhow::ensure!(
        year != 0,
        "MySQL Debezium YEAR value '{value}' cannot represent the zero sentinel"
    );
    Ok(year)
}

fn validate_temporal_shape(
    value: &str,
    base_len: usize,
    precision: u32,
    family: &str,
) -> anyhow::Result<()> {
    let precision = usize::try_from(precision)?;
    let expected_len = base_len
        .checked_add(if precision == 0 { 0 } else { precision + 1 })
        .ok_or_else(|| anyhow::anyhow!("MySQL Debezium {family} length overflow"))?;
    anyhow::ensure!(
        value.len() == expected_len
            && (precision == 0
                || (value.as_bytes()[base_len] == b'.'
                    && value[base_len + 1..]
                        .bytes()
                        .all(|byte| byte.is_ascii_digit()))),
        "MySQL Debezium {family} value '{value}' does not match declared precision {precision}"
    );
    Ok(())
}

fn write_base64(buf: &mut Vec<u8>, value: &[u8]) -> anyhow::Result<()> {
    buf.push(b'"');
    let encoded_len = base64::encoded_len(value.len(), true)
        .ok_or_else(|| anyhow::anyhow!("base64 length overflow"))?;
    let start = buf.len();
    let output_len = start
        .checked_add(encoded_len)
        .ok_or_else(|| anyhow::anyhow!("base64 output length overflow"))?;
    buf.resize(output_len, 0);
    let written = base64::engine::general_purpose::STANDARD
        .encode_slice(value, &mut buf[start..])
        .map_err(|error| anyhow::anyhow!("base64 encoding failed: {error}"))?;
    debug_assert_eq!(written, encoded_len);
    buf.push(b'"');
    Ok(())
}

/// Fast integer formatting via `itoa`.
#[inline]
fn write_int<T: itoa::Integer>(buf: &mut Vec<u8>, v: T) {
    buf.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
}

/// Fast unsigned formatting via `itoa`.
#[inline]
fn write_uint<T: itoa::Integer>(buf: &mut Vec<u8>, v: T) {
    buf.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
}

#[inline]
fn write_float<T: ryu::Float>(
    buf: &mut Vec<u8>,
    value: T,
    non_finite_floats: NonFiniteFloatEncoding,
) {
    let mut formatter = ryu::Buffer::new();
    let formatted = formatter.format(value);
    match formatted {
        "NaN" => match non_finite_floats {
            NonFiniteFloatEncoding::Null => buf.extend_from_slice(b"null"),
            NonFiniteFloatEncoding::ProtobufJsonString => buf.extend_from_slice(b"\"NaN\""),
        },
        "inf" => match non_finite_floats {
            NonFiniteFloatEncoding::Null => buf.extend_from_slice(b"null"),
            NonFiniteFloatEncoding::ProtobufJsonString => buf.extend_from_slice(b"\"Infinity\""),
        },
        "-inf" => match non_finite_floats {
            NonFiniteFloatEncoding::Null => buf.extend_from_slice(b"null"),
            NonFiniteFloatEncoding::ProtobufJsonString => buf.extend_from_slice(b"\"-Infinity\""),
        },
        finite => buf.extend_from_slice(finite.as_bytes()),
    }
}

/// Write a JSON-escaped string (with surrounding quotes) into the buffer.
fn write_json_string(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    write_json_string_contents(buf, s);
    buf.push(b'"');
}

fn write_json_string_contents(buf: &mut Vec<u8>, s: &str) {
    for &b in s.as_bytes() {
        write_json_string_byte(buf, b);
    }
}

fn write_json_string_byte(buf: &mut Vec<u8>, byte: u8) {
    match byte {
        b'"' => buf.extend_from_slice(b"\\\""),
        b'\\' => buf.extend_from_slice(b"\\\\"),
        b'\n' => buf.extend_from_slice(b"\\n"),
        b'\r' => buf.extend_from_slice(b"\\r"),
        b'\t' => buf.extend_from_slice(b"\\t"),
        0x00..=0x1F => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            buf.extend_from_slice(b"\\u00");
            buf.push(HEX[usize::from(byte >> 4)]);
            buf.push(HEX[usize::from(byte & 0x0f)]);
        }
        _ => buf.push(byte),
    }
}

fn write_cp1252_json_string(buf: &mut Vec<u8>, value: &[u8]) -> anyhow::Result<()> {
    buf.push(b'"');
    for &byte in value {
        if byte.is_ascii() {
            write_json_string_byte(buf, byte);
            continue;
        }
        let character = match byte {
            0x80 => '\u{20ac}',
            0x81 | 0x8d | 0x8f | 0x90 | 0x9d => anyhow::bail!(
                "MySQL Debezium latin1 value contains undefined cp1252 byte 0x{byte:02x}"
            ),
            0x82 => '\u{201a}',
            0x83 => '\u{0192}',
            0x84 => '\u{201e}',
            0x85 => '\u{2026}',
            0x86 => '\u{2020}',
            0x87 => '\u{2021}',
            0x88 => '\u{02c6}',
            0x89 => '\u{2030}',
            0x8a => '\u{0160}',
            0x8b => '\u{2039}',
            0x8c => '\u{0152}',
            0x8e => '\u{017d}',
            0x91 => '\u{2018}',
            0x92 => '\u{2019}',
            0x93 => '\u{201c}',
            0x94 => '\u{201d}',
            0x95 => '\u{2022}',
            0x96 => '\u{2013}',
            0x97 => '\u{2014}',
            0x98 => '\u{02dc}',
            0x99 => '\u{2122}',
            0x9a => '\u{0161}',
            0x9b => '\u{203a}',
            0x9c => '\u{0153}',
            0x9e => '\u{017e}',
            0x9f => '\u{0178}',
            _ => char::from(byte),
        };
        let mut encoded = [0_u8; 4];
        buf.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
    }
    buf.push(b'"');
    Ok(())
}

#[cfg(test)]
#[path = "tests/json_serializer.rs"]
mod tests;
