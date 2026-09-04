use std::fmt;
use std::mem::size_of;
use std::sync::Arc;

use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::value::RawValue;
use transferia_core::ChangeOperation;
use ydb_grpc::ydb_proto::r#type::PrimitiveTypeId;

use super::super::types::{ColumnKind, ColumnPlan};

#[derive(Clone, Debug, PartialEq)]
pub(super) enum YdbCdcValue {
    Absent,
    Null,
    Bool(bool),
    Int8(i8),
    UInt8(u8),
    Int16(i16),
    UInt16(u16),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Date32(i32),
    TimestampSecond(i64),
    TimestampMicrosecond(i64),
    DurationMicrosecond(i64),
    Binary(Vec<u8>),
    Utf8(String),
    Uuid([u8; 16]),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct YdbCdcTransactionIdentity([u8; 16]);

impl YdbCdcTransactionIdentity {
    fn new(step: u64, transaction_id: u64) -> Self {
        let mut encoded = [0_u8; 16];
        encoded[..8].copy_from_slice(&step.to_be_bytes());
        encoded[8..].copy_from_slice(&transaction_id.to_be_bytes());
        Self(encoded)
    }

    pub(super) const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub(super) fn step(self) -> u64 {
        let mut step = [0_u8; 8];
        step.copy_from_slice(&self.0[..8]);
        u64::from_be_bytes(step)
    }

    #[cfg(test)]
    pub(super) fn transaction_id(self) -> u64 {
        let mut transaction_id = [0_u8; 8];
        transaction_id.copy_from_slice(&self.0[8..]);
        u64::from_be_bytes(transaction_id)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct DecodedYdbCdcEvent {
    pub operation: ChangeOperation,
    /// Current values in the authoritative discovery schema order. They are
    /// complete for create/update; delete retains only its primary key, with
    /// other columns `Absent` rather than SQL `NULL`.
    pub current: Vec<YdbCdcValue>,
    /// Old values in the same schema order. They are complete for update and
    /// delete and entirely `Absent` for create because no old row existed.
    pub old: Vec<YdbCdcValue>,
    /// Bit `n` denotes that current user column `n` was physically carried by
    /// the event. Primary-key bits are always set.
    pub changed_columns: Vec<u8>,
    pub transaction: YdbCdcTransactionIdentity,
}

/// Strict, allocation-bounded decoder for one YDB `FORMAT = JSON` changefeed
/// message. The caller supplies the configured encoded-message bound; there is
/// no connector-local operational limit.
pub(super) struct YdbCdcDecoder {
    columns: Arc<[ColumnPlan]>,
    columns_by_name: Vec<usize>,
    primary_key_indexes: Vec<usize>,
    max_event_bytes: usize,
}

impl YdbCdcDecoder {
    pub(super) fn new(
        columns: Arc<[ColumnPlan]>,
        max_event_bytes: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(max_event_bytes > 0, "YDB CDC max event bytes must be positive");
        validate_cdc_column_plans(&columns)?;

        let mut columns_by_name = (0..columns.len()).collect::<Vec<_>>();
        columns_by_name
            .sort_unstable_by(|left, right| columns[*left].name.cmp(&columns[*right].name));
        for pair in columns_by_name.windows(2) {
            anyhow::ensure!(
                columns[pair[0]].name != columns[pair[1]].name,
                "YDB CDC schema contains duplicate column '{}'",
                columns[pair[0]].name
            );
        }
        let mut primary_key_indexes = columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                column.primary_key_ordinal.map(|ordinal| (ordinal, index))
            })
            .collect::<Vec<_>>();
        primary_key_indexes.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        anyhow::ensure!(
            primary_key_indexes
                .iter()
                .enumerate()
                .all(|(expected, (actual, _))| expected == *actual),
            "YDB CDC primary-key ordinals are not contiguous"
        );
        let primary_key_indexes = primary_key_indexes
            .into_iter()
            .map(|(_, index)| index)
            .collect();
        Ok(Self {
            columns,
            columns_by_name,
            primary_key_indexes,
            max_event_bytes,
        })
    }

    pub(super) fn decode(&self, payload: &[u8]) -> anyhow::Result<DecodedYdbCdcEvent> {
        anyhow::ensure!(
            payload.len() <= self.max_event_bytes,
            "YDB CDC event has {} encoded bytes, configured maximum is {}",
            payload.len(),
            self.max_event_bytes
        );
        let envelope = serde_json::from_slice::<RawEnvelope>(payload)
            .map_err(|error| anyhow::anyhow!("invalid YDB CDC JSON envelope: {error}"))?;
        anyhow::ensure!(
            envelope.key.len() == self.primary_key_indexes.len(),
            "YDB CDC key has {} values, schema declares {} primary-key columns",
            envelope.key.len(),
            self.primary_key_indexes.len()
        );

        let operation_shapes = [
            envelope.update.is_some(),
            envelope.reset.is_some(),
            envelope.erase.is_some(),
        ]
            .into_iter()
            .filter(|present| *present)
            .count();
        anyhow::ensure!(
            operation_shapes == 1,
            "YDB CDC envelope must contain exactly one of update, reset, or erase"
        );

        let mut current = vec![YdbCdcValue::Absent; self.columns.len()];
        let mut old = vec![YdbCdcValue::Absent; self.columns.len()];
        let mut changed_columns = vec![0_u8; self.columns.len().div_ceil(8)];
        for ((raw, &index), ordinal) in envelope
            .key
            .iter()
            .zip(&self.primary_key_indexes)
            .zip(0..)
        {
            let value = decode_value(raw, &self.columns[index]).map_err(|error| {
                anyhow::anyhow!(
                    "invalid YDB CDC primary-key value {ordinal} for column '{}': {error}",
                    self.columns[index].name
                )
            })?;
            anyhow::ensure!(
                !matches!(value, YdbCdcValue::Null | YdbCdcValue::Absent),
                "YDB CDC primary-key column '{}' is null",
                self.columns[index].name
            );
            current[index] = value.clone();
            set_changed(&mut changed_columns, index);
        }

        let operation = if let Some(erase) = envelope.erase {
            anyhow::ensure!(
                erase.entries.is_empty(),
                "YDB CDC erase object must be empty; its key belongs in key"
            );
            anyhow::ensure!(
                envelope.new_image.is_none(),
                "YDB CDC erase must not contain newImage"
            );
            let old_image = envelope.old_image.ok_or_else(|| {
                anyhow::anyhow!("YDB CDC erase has no required oldImage")
            })?;
            self.copy_primary_key(&current, &mut old);
            self.decode_image(old_image, &mut old, None, "oldImage")?;
            anyhow::ensure!(
                old.iter()
                    .all(|value| !matches!(value, YdbCdcValue::Absent)),
                "YDB CDC oldImage does not contain a complete old row"
            );
            ChangeOperation::Delete
        } else {
            let (write, write_name) = match (envelope.update, envelope.reset) {
                (Some(update), None) => (update, "update"),
                (None, Some(reset)) => (reset, "reset"),
                _ => anyhow::bail!("YDB CDC envelope lost its validated write shape"),
            };
            anyhow::ensure!(
                write.entries.is_empty(),
                "YDB CDC NEW_AND_OLD_IMAGES {write_name} flag must be an empty object"
            );
            let new_image = envelope.new_image.ok_or_else(|| {
                anyhow::anyhow!("YDB CDC {write_name} has no required newImage")
            })?;
            self.decode_image(
                new_image,
                &mut current,
                Some(&mut changed_columns),
                "newImage",
            )?;
            anyhow::ensure!(
                current
                    .iter()
                    .all(|value| !matches!(value, YdbCdcValue::Absent)),
                "YDB CDC newImage does not contain a complete current row"
            );
            if let Some(old_image) = envelope.old_image {
                self.copy_primary_key(&current, &mut old);
                self.decode_image(old_image, &mut old, None, "oldImage")?;
                anyhow::ensure!(
                    old.iter()
                        .all(|value| !matches!(value, YdbCdcValue::Absent)),
                    "YDB CDC oldImage does not contain a complete old row"
                );
                ChangeOperation::Update
            } else {
                ChangeOperation::Create
            }
        };

        Ok(DecodedYdbCdcEvent {
            operation,
            current,
            old,
            changed_columns,
            transaction: YdbCdcTransactionIdentity::new(
                envelope.timestamp[0],
                envelope.timestamp[1],
            ),
        })
    }

    /// Conservative heap admission for serde's borrowed/raw envelope, duplicate-key
    /// validation, and the two decoded row images. Every variable-size JSON item
    /// consumes at least one encoded byte, so charging one slot of every live
    /// representation per encoded byte is deliberately conservative without an
    /// operational hard limit hidden in the decoder.
    pub(super) fn decode_admission_bytes(&self, payload_len: usize) -> anyhow::Result<usize> {
        let per_encoded_byte = size_of::<Box<RawValue>>()
            .checked_add(size_of::<(String, Box<RawValue>)>())
            .and_then(|bytes| bytes.checked_add(size_of::<String>()))
            .and_then(|bytes| bytes.checked_add(4))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC decode admission overflow"))?;
        let envelope_bytes = payload_len
            .checked_mul(per_encoded_byte)
            .ok_or_else(|| anyhow::anyhow!("YDB CDC decode admission overflow"))?;
        let row_slots = self
            .columns
            .len()
            .checked_mul(2)
            .and_then(|slots| slots.checked_mul(size_of::<YdbCdcValue>()))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC row admission overflow"))?;
        let changed_mask = self.columns.len().div_ceil(8);
        envelope_bytes
            .checked_add(row_slots)
            .and_then(|bytes| bytes.checked_add(changed_mask))
            .ok_or_else(|| anyhow::anyhow!("YDB CDC decode admission overflow"))
    }

    fn decode_image(
        &self,
        image: RawObject,
        output: &mut [YdbCdcValue],
        mut changed_columns: Option<&mut [u8]>,
        image_name: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            image.entries.len() <= self.columns.len(),
            "YDB CDC {image_name} has more fields than the discovered schema"
        );
        for (name, raw) in image.entries {
            let index = self.column_index(&name).ok_or_else(|| {
                anyhow::anyhow!("YDB CDC {image_name} contains unknown column '{name}'")
            })?;
            anyhow::ensure!(
                !self.columns[index].primary_key,
                "YDB CDC {image_name} repeats primary-key column '{name}'"
            );
            anyhow::ensure!(
                matches!(output[index], YdbCdcValue::Absent),
                "YDB CDC {image_name} repeats column '{name}'"
            );
            output[index] = decode_value(&raw, &self.columns[index]).map_err(|error| {
                anyhow::anyhow!("invalid YDB CDC {image_name} column '{name}': {error}")
            })?;
            if let Some(mask) = changed_columns.as_deref_mut() {
                set_changed(mask, index);
            }
        }
        Ok(())
    }

    fn copy_primary_key(&self, current: &[YdbCdcValue], old: &mut [YdbCdcValue]) {
        for &index in &self.primary_key_indexes {
            old[index] = current[index].clone();
        }
    }

    fn column_index(&self, name: &str) -> Option<usize> {
        self.columns_by_name
            .binary_search_by(|index| self.columns[*index].name.as_str().cmp(name))
            .ok()
            .map(|sorted_index| self.columns_by_name[sorted_index])
    }
}

/// Startup validator shared by discovery/preparation. Nullable JSON values are
/// rejected because YDB's JSON changefeed representation cannot distinguish an
/// SQL NULL from the JSON value `null`. Decimal is rejected because YDB admits
/// `Inf`, `-Inf`, and `NaN`, which have no lossless Decimal128 representation.
pub(super) fn validate_cdc_column_plans(columns: &[ColumnPlan]) -> anyhow::Result<()> {
    anyhow::ensure!(
        columns.iter().any(|column| column.primary_key),
        "YDB CDC requires at least one primary-key column"
    );
    for column in columns {
        anyhow::ensure!(
            !matches!(&column.kind, ColumnKind::Decimal { .. }),
            "YDB CDC column '{}' is Decimal; replication cannot losslessly represent YDB Decimal special values Inf, -Inf, and NaN",
            column.name
        );
        let primitive = column.primitive_type()?;
        anyhow::ensure!(
            !(column.nullable
                && matches!(
                    primitive,
                    Some(PrimitiveTypeId::Json | PrimitiveTypeId::JsonDocument)
                )),
            "YDB CDC column '{}' is nullable {:?}; FORMAT JSON cannot distinguish SQL NULL from JSON null",
            column.name,
            primitive
        );
    }
    Ok(())
}

fn set_changed(mask: &mut [u8], index: usize) {
    mask[index / 8] |= 1_u8 << (index % 8);
}

fn decode_value(raw: &RawValue, column: &ColumnPlan) -> anyhow::Result<YdbCdcValue> {
    let primitive = column.primitive_type()?;
    let raw_text = raw.get();
    let is_json = matches!(
        primitive,
        Some(PrimitiveTypeId::Json | PrimitiveTypeId::JsonDocument)
    );
    if raw_text == "null" && !is_json {
        anyhow::ensure!(column.nullable, "non-null column received SQL NULL");
        return Ok(YdbCdcValue::Null);
    }

    let primitive = primitive.ok_or_else(|| anyhow::anyhow!("missing YDB primitive type"))?;
    Ok(match primitive {
        PrimitiveTypeId::Bool => YdbCdcValue::Bool(serde_json::from_str(raw_text)?),
        PrimitiveTypeId::Int8 => YdbCdcValue::Int8(json_integer(raw)?),
        PrimitiveTypeId::Uint8 => YdbCdcValue::UInt8(json_integer(raw)?),
        PrimitiveTypeId::Int16 => YdbCdcValue::Int16(json_integer(raw)?),
        PrimitiveTypeId::Uint16 => YdbCdcValue::UInt16(json_integer(raw)?),
        PrimitiveTypeId::Int32 => YdbCdcValue::Int32(json_integer(raw)?),
        PrimitiveTypeId::Uint32 => YdbCdcValue::UInt32(json_integer(raw)?),
        PrimitiveTypeId::Int64 => YdbCdcValue::Int64(json_integer(raw)?),
        PrimitiveTypeId::Uint64 => YdbCdcValue::UInt64(json_integer(raw)?),
        PrimitiveTypeId::Float => {
            let value = json_float(raw)?;
            let narrowed = value as f32;
            anyhow::ensure!(narrowed.is_finite(), "Float value is outside the finite f32 range");
            YdbCdcValue::Float32(narrowed)
        }
        PrimitiveTypeId::Double => YdbCdcValue::Float64(json_float(raw)?),
        PrimitiveTypeId::Date | PrimitiveTypeId::Date32 => {
            let days = parse_cdc_date(&json_string(raw)?)?;
            if primitive == PrimitiveTypeId::Date {
                anyhow::ensure!(
                    (0..=i64::from(u16::MAX)).contains(&days),
                    "Date value is outside the YDB Date range"
                );
            }
            YdbCdcValue::Date32(i32::try_from(days)?)
        }
        PrimitiveTypeId::Datetime | PrimitiveTypeId::Datetime64 => {
            let seconds = parse_datetime(&json_string(raw)?, false)?;
            if primitive == PrimitiveTypeId::Datetime {
                u32::try_from(seconds)
                    .map_err(|_| anyhow::anyhow!("Datetime value is outside the YDB Datetime range"))?;
            }
            YdbCdcValue::TimestampSecond(seconds)
        }
        PrimitiveTypeId::Timestamp | PrimitiveTypeId::Timestamp64 => {
            let micros = parse_datetime(&json_string(raw)?, true)?;
            if primitive == PrimitiveTypeId::Timestamp {
                anyhow::ensure!(micros >= 0, "Timestamp value is outside the YDB Timestamp range");
            }
            YdbCdcValue::TimestampMicrosecond(micros)
        }
        PrimitiveTypeId::Interval | PrimitiveTypeId::Interval64 => {
            YdbCdcValue::DurationMicrosecond(json_integer(raw)?)
        }
        PrimitiveTypeId::String | PrimitiveTypeId::Yson => {
            YdbCdcValue::Binary(decode_base64(&json_string(raw)?)?)
        }
        PrimitiveTypeId::Utf8
        | PrimitiveTypeId::TzDate
        | PrimitiveTypeId::TzDatetime
        | PrimitiveTypeId::TzTimestamp
        | PrimitiveTypeId::Dynumber => YdbCdcValue::Utf8(json_string(raw)?),
        PrimitiveTypeId::Json | PrimitiveTypeId::JsonDocument => {
            serde_json::from_str::<NoDuplicateJson>(raw_text)
                .map_err(|error| anyhow::anyhow!("invalid JSON column value: {error}"))?;
            YdbCdcValue::Utf8(raw_text.to_owned())
        }
        PrimitiveTypeId::Uuid => {
            YdbCdcValue::Uuid(uuid::Uuid::parse_str(&json_string(raw)?)?.into_bytes())
        }
        PrimitiveTypeId::Unspecified => anyhow::bail!("YDB primitive type is unspecified"),
    })
}

fn json_string(raw: &RawValue) -> anyhow::Result<String> {
    serde_json::from_str(raw.get()).map_err(Into::into)
}

fn json_integer<T>(raw: &RawValue) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let text = raw.get();
    anyhow::ensure!(
        !text
            .bytes()
            .any(|byte| matches!(byte, b'.' | b'e' | b'E')),
        "integer value must not contain a fraction or exponent"
    );
    Ok(text.parse()?)
}

fn json_float(raw: &RawValue) -> anyhow::Result<f64> {
    let value: f64 = serde_json::from_str(raw.get())?;
    anyhow::ensure!(value.is_finite(), "floating-point value is not finite");
    Ok(value)
}

fn parse_date(value: &str) -> anyhow::Result<i64> {
    let first_separator = value
        .char_indices()
        .skip(if value.starts_with('-') { 1 } else { 0 })
        .find_map(|(index, character)| (character == '-').then_some(index))
        .ok_or_else(|| anyhow::anyhow!("date must use year-month-day format"))?;
    let year = value[..first_separator].parse::<i64>()?;
    let remainder = &value[first_separator + 1..];
    let (month, day) = remainder
        .split_once('-')
        .ok_or_else(|| anyhow::anyhow!("date must use year-month-day format"))?;
    anyhow::ensure!(
        month.len() == 2 && day.len() == 2,
        "date month and day must contain exactly two digits"
    );
    let month = month.parse::<u32>()?;
    let day = day.parse::<u32>()?;
    anyhow::ensure!((1..=12).contains(&month), "date month is outside 1..=12");
    let leap = year.rem_euclid(4) == 0
        && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0);
    let days_in_month = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    anyhow::ensure!(
        (1..=days_in_month).contains(&day),
        "date day is outside the selected month"
    );

    let adjusted_year = if month <= 2 {
        year.checked_sub(1)
            .ok_or_else(|| anyhow::anyhow!("date year arithmetic overflow"))?
    } else {
        year
    };
    let era = adjusted_year.div_euclid(400);
    let era_years = era
        .checked_mul(400)
        .ok_or_else(|| anyhow::anyhow!("date era arithmetic overflow"))?;
    let year_of_era = adjusted_year
        .checked_sub(era_years)
        .ok_or_else(|| anyhow::anyhow!("date year-of-era arithmetic overflow"))?;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era
        .checked_mul(365)
        .and_then(|value| value.checked_add(year_of_era / 4))
        .and_then(|value| value.checked_sub(year_of_era / 100))
        .and_then(|value| value.checked_add(day_of_year))
        .ok_or_else(|| anyhow::anyhow!("date day-of-era arithmetic overflow"))?;
    era.checked_mul(146_097)
        .and_then(|value| value.checked_add(day_of_era))
        .and_then(|value| value.checked_sub(719_468))
        .ok_or_else(|| anyhow::anyhow!("date is outside the supported arithmetic range"))
}

fn parse_cdc_date(value: &str) -> anyhow::Result<i64> {
    let date = value
        .strip_suffix("T00:00:00.000000Z")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "YDB CDC Date must use the exact midnight UTC form YYYY-MM-DDT00:00:00.000000Z"
            )
        })?;
    parse_date(date)
}

fn parse_datetime(value: &str, fractional: bool) -> anyhow::Result<i64> {
    let value = value
        .strip_suffix('Z')
        .ok_or_else(|| anyhow::anyhow!("YDB CDC timestamp must end with the UTC marker 'Z'"))?;
    let (date, time) = value
        .split_once('T')
        .ok_or_else(|| anyhow::anyhow!("timestamp must separate date and time with 'T'"))?;
    let days = parse_date(date)?;
    let (whole_time, fraction) = match time.split_once('.') {
        Some((whole, fraction)) if fractional => (whole, Some(fraction)),
        Some((whole, fraction)) => {
            anyhow::ensure!(
                fraction == "000000",
                "YDB CDC Datetime must carry exactly six zero fractional digits"
            );
            (whole, None)
        }
        None if fractional => (time, None),
        None => anyhow::bail!(
            "YDB CDC Datetime must carry exactly six zero fractional digits"
        ),
    };
    let mut time_parts = whole_time.split(':');
    let hour = time_parts.next().unwrap_or_default();
    let minute = time_parts.next().unwrap_or_default();
    let second = time_parts.next().unwrap_or_default();
    anyhow::ensure!(
        time_parts.next().is_none()
            && hour.len() == 2
            && minute.len() == 2
            && second.len() == 2,
        "time must use hour:minute:second with two digits per field"
    );
    let hour = hour.parse::<u32>()?;
    let minute = minute.parse::<u32>()?;
    let second = second.parse::<u32>()?;
    anyhow::ensure!(hour < 24, "timestamp hour is outside 0..24");
    anyhow::ensure!(minute < 60, "timestamp minute is outside 0..60");
    anyhow::ensure!(second < 60, "timestamp second is outside 0..60");
    let seconds = days
        .checked_mul(86_400)
        .and_then(|value| {
            value.checked_add(i64::from(hour * 3_600 + minute * 60 + second))
        })
        .ok_or_else(|| anyhow::anyhow!("timestamp seconds overflow i64"))?;
    if !fractional {
        return Ok(seconds);
    }
    let fraction = fraction.unwrap_or_default();
    anyhow::ensure!(
        !fraction.is_empty()
            && fraction.len() <= 6
            && fraction.bytes().all(|byte| byte.is_ascii_digit()),
        "Timestamp fraction must contain one to six decimal digits"
    );
    let mut micros = fraction.parse::<i64>()?;
    for _ in fraction.len()..6 {
        micros *= 10;
    }
    seconds
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(micros))
        .ok_or_else(|| anyhow::anyhow!("timestamp microseconds overflow i64"))
}

fn decode_base64(value: &str) -> anyhow::Result<Vec<u8>> {
    let input = value.as_bytes();
    anyhow::ensure!(
        input.len().is_multiple_of(4),
        "base64 length is not divisible by four"
    );
    let padding = input
        .iter()
        .rev()
        .take_while(|byte| **byte == b'=')
        .count();
    anyhow::ensure!(padding <= 2, "base64 has invalid padding");
    let capacity = input
        .len()
        .checked_div(4)
        .and_then(|groups| groups.checked_mul(3))
        .and_then(|bytes| bytes.checked_sub(padding))
        .ok_or_else(|| anyhow::anyhow!("base64 decoded length overflow"))?;
    let mut output = Vec::with_capacity(capacity);
    for (group_index, group) in input.chunks_exact(4).enumerate() {
        let last = group_index + 1 == input.len() / 4;
        anyhow::ensure!(
            group[0] != b'=' && group[1] != b'=',
            "base64 padding appears before enough input"
        );
        anyhow::ensure!(
            last || (group[2] != b'=' && group[3] != b'='),
            "base64 padding appears before the final group"
        );
        let first = base64_digit(group[0])?;
        let second = base64_digit(group[1])?;
        output.push((first << 2) | (second >> 4));
        if group[2] == b'=' {
            anyhow::ensure!(group[3] == b'=', "base64 has invalid padding order");
            anyhow::ensure!(second & 0x0f == 0, "base64 has non-zero unused bits");
            continue;
        }
        let third = base64_digit(group[2])?;
        output.push((second << 4) | (third >> 2));
        if group[3] == b'=' {
            anyhow::ensure!(third & 0x03 == 0, "base64 has non-zero unused bits");
            continue;
        }
        let fourth = base64_digit(group[3])?;
        output.push((third << 6) | fourth);
    }
    anyhow::ensure!(
        output.len() == capacity,
        "base64 decoded length does not match its padding"
    );
    Ok(output)
}

fn base64_digit(value: u8) -> anyhow::Result<u8> {
    Ok(match value {
        b'A'..=b'Z' => value - b'A',
        b'a'..=b'z' => value - b'a' + 26,
        b'0'..=b'9' => value - b'0' + 52,
        b'+' => 62,
        b'/' => 63,
        _ => anyhow::bail!("invalid standard-base64 byte 0x{value:02x}"),
    })
}

struct RawObject {
    entries: Vec<(String, Box<RawValue>)>,
}

impl<'de> Deserialize<'de> for RawObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawObjectVisitor;

        impl<'de> Visitor<'de> for RawObjectVisitor {
            type Value = RawObject;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a YDB CDC image object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut entries = Vec::<(String, Box<RawValue>)>::new();
                while let Some(name) = map.next_key::<String>()? {
                    entries.push((name, map.next_value()?));
                }
                entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                if let Some(duplicate) = entries
                    .windows(2)
                    .find(|pair| pair[0].0 == pair[1].0)
                {
                    return Err(de::Error::custom(format_args!(
                        "duplicate image column '{}'",
                        duplicate[0].0
                    )));
                }
                Ok(RawObject { entries })
            }
        }

        deserializer.deserialize_map(RawObjectVisitor)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEnvelope {
    key: Vec<Box<RawValue>>,
    update: Option<RawObject>,
    reset: Option<RawObject>,
    erase: Option<RawObject>,
    #[serde(rename = "newImage")]
    new_image: Option<RawObject>,
    #[serde(rename = "oldImage")]
    old_image: Option<RawObject>,
    #[serde(rename = "ts")]
    timestamp: [u64; 2],
}

struct NoDuplicateJson;

impl<'de> Deserialize<'de> for NoDuplicateJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(NoDuplicateJsonVisitor)
    }
}

struct NoDuplicateJsonVisitor;

impl<'de> Visitor<'de> for NoDuplicateJsonVisitor {
    type Value = NoDuplicateJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON without duplicate object keys")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(NoDuplicateJson)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(NoDuplicateJson)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(NoDuplicateJson)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(NoDuplicateJson)
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(NoDuplicateJson)
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(NoDuplicateJson)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(NoDuplicateJson)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(NoDuplicateJson)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<NoDuplicateJson>()?.is_some() {}
        Ok(NoDuplicateJson)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = Vec::<String>::new();
        while let Some(key) = map.next_key::<String>()? {
            keys.push(key);
            map.next_value::<NoDuplicateJson>()?;
        }
        keys.sort_unstable();
        if let Some(duplicate) = keys.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(de::Error::custom(format_args!(
                "duplicate JSON object key '{}'",
                duplicate[0]
            )));
        }
        Ok(NoDuplicateJson)
    }
}

#[cfg(test)]
#[path = "tests/decoder.rs"]
mod tests;
