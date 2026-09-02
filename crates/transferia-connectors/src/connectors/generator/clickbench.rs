use std::sync::{Arc, LazyLock};

use arrow::array::{
    ArrayRef, BinaryBuilder, Date32Array, Int16Array, Int32Array, Int64Array, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde::Deserialize;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

const PERCENTILE_COUNT: usize = 101;
const CLICKBENCH_UNIQUE_ROW_LIMIT: u64 = 1_u64 << 62;
const WATCH_ID_BASE_I64: i64 = 1_i64 << 62;
const WATCH_ID_MULTIPLIER: u64 = 2_862_933_555_777_941_757;
const ALPHABET: &[u8; 64] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

#[derive(Clone, Copy)]
enum ColumnKind {
    Binary,
    Date32,
    Int16,
    Int32,
    Int64,
    TimestampSecond,
}

impl ColumnKind {
    const fn arrow_type(self) -> DataType {
        match self {
            Self::Binary => DataType::Binary,
            Self::Date32 => DataType::Date32,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::TimestampSecond => DataType::Timestamp(TimeUnit::Second, None),
        }
    }

    const fn fixed_width(self) -> Option<u64> {
        match self {
            Self::Binary => None,
            Self::Date32 | Self::Int32 => Some(4),
            Self::Int16 => Some(2),
            Self::Int64 | Self::TimestampSecond => Some(8),
        }
    }

    const fn accepts(self, value: i64) -> bool {
        match self {
            Self::Int16 => value >= i16::MIN as i64 && value <= i16::MAX as i64,
            Self::Date32 | Self::Int32 => value >= i32::MIN as i64 && value <= i32::MAX as i64,
            Self::Binary | Self::Int64 | Self::TimestampSecond => true,
        }
    }
}

#[derive(Clone, Copy)]
struct Column {
    name: &'static str,
    kind: ColumnKind,
    primary_key: bool,
}

const fn column(name: &'static str, kind: ColumnKind) -> Column {
    Column {
        name,
        kind,
        primary_key: false,
    }
}

const fn primary_key(name: &'static str, kind: ColumnKind) -> Column {
    Column {
        name,
        kind,
        primary_key: true,
    }
}

// This is the current 105-column ClickBench `hits` schema. ClickHouse String
// maps to Arrow Binary because the reference CSV contains arbitrary non-UTF-8
// bytes. Date and DateTime remain temporal Arrow types rather than formatted
// strings. The primary-key column set matches ClickBench's table definition.
const COLUMNS: [Column; 105] = [
    primary_key("WatchID", ColumnKind::Int64),
    column("JavaEnable", ColumnKind::Int16),
    column("Title", ColumnKind::Binary),
    column("GoodEvent", ColumnKind::Int16),
    primary_key("EventTime", ColumnKind::TimestampSecond),
    primary_key("EventDate", ColumnKind::Date32),
    primary_key("CounterID", ColumnKind::Int32),
    column("ClientIP", ColumnKind::Int32),
    column("RegionID", ColumnKind::Int32),
    primary_key("UserID", ColumnKind::Int64),
    column("CounterClass", ColumnKind::Int16),
    column("OS", ColumnKind::Int16),
    column("UserAgent", ColumnKind::Int16),
    column("URL", ColumnKind::Binary),
    column("Referer", ColumnKind::Binary),
    column("IsRefresh", ColumnKind::Int16),
    column("RefererCategoryID", ColumnKind::Int16),
    column("RefererRegionID", ColumnKind::Int32),
    column("URLCategoryID", ColumnKind::Int16),
    column("URLRegionID", ColumnKind::Int32),
    column("ResolutionWidth", ColumnKind::Int16),
    column("ResolutionHeight", ColumnKind::Int16),
    column("ResolutionDepth", ColumnKind::Int16),
    column("FlashMajor", ColumnKind::Int16),
    column("FlashMinor", ColumnKind::Int16),
    column("FlashMinor2", ColumnKind::Binary),
    column("NetMajor", ColumnKind::Int16),
    column("NetMinor", ColumnKind::Int16),
    column("UserAgentMajor", ColumnKind::Int16),
    column("UserAgentMinor", ColumnKind::Binary),
    column("CookieEnable", ColumnKind::Int16),
    column("JavascriptEnable", ColumnKind::Int16),
    column("IsMobile", ColumnKind::Int16),
    column("MobilePhone", ColumnKind::Int16),
    column("MobilePhoneModel", ColumnKind::Binary),
    column("Params", ColumnKind::Binary),
    column("IPNetworkID", ColumnKind::Int32),
    column("TraficSourceID", ColumnKind::Int16),
    column("SearchEngineID", ColumnKind::Int16),
    column("SearchPhrase", ColumnKind::Binary),
    column("AdvEngineID", ColumnKind::Int16),
    column("IsArtifical", ColumnKind::Int16),
    column("WindowClientWidth", ColumnKind::Int16),
    column("WindowClientHeight", ColumnKind::Int16),
    column("ClientTimeZone", ColumnKind::Int16),
    column("ClientEventTime", ColumnKind::TimestampSecond),
    column("SilverlightVersion1", ColumnKind::Int16),
    column("SilverlightVersion2", ColumnKind::Int16),
    column("SilverlightVersion3", ColumnKind::Int32),
    column("SilverlightVersion4", ColumnKind::Int16),
    column("PageCharset", ColumnKind::Binary),
    column("CodeVersion", ColumnKind::Int32),
    column("IsLink", ColumnKind::Int16),
    column("IsDownload", ColumnKind::Int16),
    column("IsNotBounce", ColumnKind::Int16),
    column("FUniqID", ColumnKind::Int64),
    column("OriginalURL", ColumnKind::Binary),
    column("HID", ColumnKind::Int32),
    column("IsOldCounter", ColumnKind::Int16),
    column("IsEvent", ColumnKind::Int16),
    column("IsParameter", ColumnKind::Int16),
    column("DontCountHits", ColumnKind::Int16),
    column("WithHash", ColumnKind::Int16),
    column("HitColor", ColumnKind::Binary),
    column("LocalEventTime", ColumnKind::TimestampSecond),
    column("Age", ColumnKind::Int16),
    column("Sex", ColumnKind::Int16),
    column("Income", ColumnKind::Int16),
    column("Interests", ColumnKind::Int16),
    column("Robotness", ColumnKind::Int16),
    column("RemoteIP", ColumnKind::Int32),
    column("WindowName", ColumnKind::Int32),
    column("OpenerName", ColumnKind::Int32),
    column("HistoryLength", ColumnKind::Int16),
    column("BrowserLanguage", ColumnKind::Binary),
    column("BrowserCountry", ColumnKind::Binary),
    column("SocialNetwork", ColumnKind::Binary),
    column("SocialAction", ColumnKind::Binary),
    column("HTTPError", ColumnKind::Int16),
    column("SendTiming", ColumnKind::Int32),
    column("DNSTiming", ColumnKind::Int32),
    column("ConnectTiming", ColumnKind::Int32),
    column("ResponseStartTiming", ColumnKind::Int32),
    column("ResponseEndTiming", ColumnKind::Int32),
    column("FetchTiming", ColumnKind::Int32),
    column("SocialSourceNetworkID", ColumnKind::Int16),
    column("SocialSourcePage", ColumnKind::Binary),
    column("ParamPrice", ColumnKind::Int64),
    column("ParamOrderID", ColumnKind::Binary),
    column("ParamCurrency", ColumnKind::Binary),
    column("ParamCurrencyID", ColumnKind::Int16),
    column("OpenstatServiceName", ColumnKind::Binary),
    column("OpenstatCampaignID", ColumnKind::Binary),
    column("OpenstatAdID", ColumnKind::Binary),
    column("OpenstatSourceID", ColumnKind::Binary),
    column("UTMSource", ColumnKind::Binary),
    column("UTMMedium", ColumnKind::Binary),
    column("UTMCampaign", ColumnKind::Binary),
    column("UTMContent", ColumnKind::Binary),
    column("UTMTerm", ColumnKind::Binary),
    column("FromTag", ColumnKind::Binary),
    column("HasGCLID", ColumnKind::Int16),
    column("RefererHash", ColumnKind::Int64),
    column("URLHash", ColumnKind::Int64),
    column("CLID", ColumnKind::Int32),
];

#[derive(Deserialize)]
struct DistributionFile {
    mean_arrow_row_bytes_upper_bound: u64,
    columns: Vec<Distribution>,
}

#[derive(Deserialize)]
struct Distribution {
    name: String,
    zero_or_empty_ppm: u64,
    estimated_cardinality: u64,
    mean: f64,
    nonzero_percentiles_0_to_100: Vec<i64>,
}

static DISTRIBUTIONS: LazyLock<Result<DistributionFile, String>> = LazyLock::new(|| {
    let parsed: DistributionFile = serde_json::from_str(include_str!("clickbench_profile.json"))
        .map_err(|error| format!("bundled ClickBench profile is invalid JSON: {error}"))?;
    validate_distributions(&parsed)
        .map_err(|error| format!("bundled ClickBench profile is invalid: {error:#}"))?;
    Ok(parsed)
});

static ARROW_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(
        schema()
            .columns
            .into_iter()
            .map(|column| {
                let metadata = column.arrow_metadata();
                Field::new(column.name, column.data_type, column.nullable).with_metadata(metadata)
            })
            .collect::<Vec<_>>(),
    ))
});

pub(super) fn logical_row_bytes() -> anyhow::Result<u64> {
    Ok(logical_row_bytes_from(distributions()?))
}

pub(super) fn validate_range(start: u64, rows: u64) -> anyhow::Result<()> {
    let end = start
        .checked_add(rows)
        .ok_or_else(|| anyhow::anyhow!("ClickBench generator row range overflows u64"))?;
    anyhow::ensure!(
        end <= CLICKBENCH_UNIQUE_ROW_LIMIT,
        "ClickBench generator row range must end at or before {CLICKBENCH_UNIQUE_ROW_LIMIT} so WatchID remains unique"
    );
    Ok(())
}

pub(super) fn schema() -> DatasetSchema {
    DatasetSchema::new(
        COLUMNS
            .iter()
            .map(|column| {
                SchemaColumn::new(column.name.to_owned(), column.kind.arrow_type(), false)
                    .with_constraints(column.primary_key, false, None)
            })
            .collect(),
    )
}

pub(super) fn batch(start: u64, rows: u64) -> anyhow::Result<RecordBatch> {
    validate_range(start, rows)?;
    let distributions = distributions()?;
    let rows = usize::try_from(rows)?;
    let arrays = COLUMNS
        .iter()
        .enumerate()
        .map(|(index, column)| array(index, *column, distributions, start, rows))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(&ARROW_SCHEMA), arrays)?)
}

pub(super) fn batch_bytes(start: u64, rows: u64) -> anyhow::Result<u64> {
    validate_range(start, rows)?;
    let distributions = distributions()?;
    let mut bytes = 0_u64;
    for (index, column) in COLUMNS.iter().enumerate() {
        if let Some(width) = column.kind.fixed_width() {
            bytes = bytes
                .checked_add(aligned_buffer_bytes(rows.checked_mul(width).ok_or_else(
                    || anyhow::anyhow!("ClickBench generator batch size overflow"),
                )?)?)
                .ok_or_else(|| anyhow::anyhow!("ClickBench generator batch size overflow"))?;
        } else {
            bytes = bytes
                .checked_add(aligned_buffer_bytes(
                    rows.checked_add(1)
                        .and_then(|offsets| offsets.checked_mul(4))
                        .ok_or_else(|| {
                            anyhow::anyhow!("ClickBench generator binary offset size overflow")
                        })?,
                )?)
                .ok_or_else(|| anyhow::anyhow!("ClickBench generator batch size overflow"))?;
            let mut value_bytes = 0_u64;
            for row in start..start + rows {
                value_bytes = value_bytes
                    .checked_add(u64::try_from(binary_length(distributions, index, row))?)
                    .ok_or_else(|| anyhow::anyhow!("ClickBench generator batch size overflow"))?;
            }
            bytes = bytes
                .checked_add(aligned_buffer_bytes(value_bytes)?)
                .ok_or_else(|| anyhow::anyhow!("ClickBench generator batch size overflow"))?;
        }
    }
    Ok(bytes)
}

fn aligned_buffer_bytes(bytes: u64) -> anyhow::Result<u64> {
    bytes
        .checked_add(63)
        .map(|value| value & !63)
        .ok_or_else(|| anyhow::anyhow!("ClickBench generator buffer size overflow"))
}

fn array(
    index: usize,
    column: Column,
    distributions: &DistributionFile,
    start: u64,
    rows: usize,
) -> anyhow::Result<ArrayRef> {
    let range = start..start + u64::try_from(rows)?;
    let array: ArrayRef = match column.kind {
        ColumnKind::Binary => {
            let mut lengths = Vec::with_capacity(rows);
            let mut value_bytes = 0_usize;
            for row in range.clone() {
                let length = binary_length(distributions, index, row);
                lengths.push(length);
                value_bytes = value_bytes.checked_add(length).ok_or_else(|| {
                    anyhow::anyhow!("ClickBench generator binary data size overflow")
                })?;
            }
            let mut builder = BinaryBuilder::with_capacity(rows, value_bytes);
            let mut scratch = Vec::new();
            for (row, length) in range.zip(lengths) {
                fill_binary(distributions, index, row, length, &mut scratch);
                builder.append_value(&scratch);
            }
            Arc::new(builder.finish())
        }
        ColumnKind::Date32 => Arc::new(Date32Array::from(
            range
                .map(|row| i32::try_from(integer_value(distributions, index, row)))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ColumnKind::Int16 => Arc::new(Int16Array::from(
            range
                .map(|row| i16::try_from(integer_value(distributions, index, row)))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ColumnKind::Int32 => Arc::new(Int32Array::from(
            range
                .map(|row| i32::try_from(integer_value(distributions, index, row)))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ColumnKind::Int64 => Arc::new(Int64Array::from_iter_values(range.map(|row| {
            if index == 0 {
                watch_id(row)
            } else {
                integer_value(distributions, index, row)
            }
        }))),
        ColumnKind::TimestampSecond => Arc::new(TimestampSecondArray::from_iter_values(
            range.map(|row| integer_value(distributions, index, row)),
        )),
    };
    Ok(array)
}

const fn watch_id(row: u64) -> i64 {
    let permuted = row.wrapping_mul(WATCH_ID_MULTIPLIER) & (CLICKBENCH_UNIQUE_ROW_LIMIT - 1);
    WATCH_ID_BASE_I64 + i64::from_le_bytes(permuted.to_le_bytes())
}

fn integer_value(distributions: &DistributionFile, index: usize, row: u64) -> i64 {
    let profile = &distributions.columns[index];
    let selection = mix64(row ^ column_salt(index));
    if selection % 1_000_000 < profile.zero_or_empty_ppm {
        return 0;
    }
    sampled_nonzero(profile, distinct_id(profile, index, row))
}

fn binary_length(distributions: &DistributionFile, index: usize, row: u64) -> usize {
    let profile = &distributions.columns[index];
    let selection = mix64(row ^ column_salt(index));
    if selection % 1_000_000 < profile.zero_or_empty_ppm {
        return 0;
    }
    sampled_nonzero(profile, distinct_id(profile, index, row)) as usize
}

fn distinct_id(profile: &Distribution, index: usize, row: u64) -> u64 {
    let zero_cardinality = u64::from(profile.zero_or_empty_ppm > 0);
    let nonzero_cardinality = profile
        .estimated_cardinality
        .saturating_sub(zero_cardinality)
        .max(1);
    mix64(row.wrapping_add(column_salt(index).rotate_left(17))) % nonzero_cardinality
}

fn sampled_nonzero(profile: &Distribution, distinct_id: u64) -> i64 {
    let quantiles = &profile.nonzero_percentiles_0_to_100;
    if quantiles.is_empty() {
        return 0;
    }
    let percentile = mix64(distinct_id ^ 0xa076_1d64_78bd_642f) % 1_000_001;
    let scaled = percentile * (PERCENTILE_COUNT as u64 - 1);
    let lower = (scaled / 1_000_000) as usize;
    let upper = (lower + 1).min(PERCENTILE_COUNT - 1);
    let low_value = i128::from(quantiles[lower]);
    let difference = i128::from(quantiles[upper]) - low_value;
    let remainder = i128::from(scaled % 1_000_000);
    (low_value + difference * remainder / 1_000_000) as i64
}

fn fill_binary(
    distributions: &DistributionFile,
    index: usize,
    row: u64,
    length: usize,
    output: &mut Vec<u8>,
) {
    output.clear();
    output.resize(length, b'a');
    if length == 0 {
        return;
    }
    let profile = &distributions.columns[index];
    let value_id = distinct_id(profile, index, row);
    let prefix: &[u8] = match COLUMNS[index].name {
        "URL" | "Referer" | "OriginalURL" | "SocialSourcePage" => b"https://",
        "Title" | "SearchPhrase" => b"page ",
        _ => b"",
    };
    let prefix_length = prefix.len().min(length);
    output[..prefix_length].copy_from_slice(&prefix[..prefix_length]);

    let mut encoded = value_id;
    for byte in &mut output[prefix_length..] {
        *byte = ALPHABET[(encoded & 63) as usize];
        encoded >>= 6;
        if encoded == 0 {
            encoded = mix64(value_id ^ column_salt(index));
        }
    }
}

const fn column_salt(index: usize) -> u64 {
    (index as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

const fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn distributions() -> anyhow::Result<&'static DistributionFile> {
    DISTRIBUTIONS
        .as_ref()
        .map_err(|error| anyhow::anyhow!(error.clone()))
}

fn validate_distributions(distributions: &DistributionFile) -> anyhow::Result<()> {
    anyhow::ensure!(
        distributions.mean_arrow_row_bytes_upper_bound > 0,
        "ClickBench mean Arrow row width must be positive"
    );
    anyhow::ensure!(
        distributions.columns.len() == COLUMNS.len(),
        "ClickBench distribution profile has {} columns but schema has {}",
        distributions.columns.len(),
        COLUMNS.len()
    );
    for (column, distribution) in COLUMNS.iter().zip(&distributions.columns) {
        anyhow::ensure!(
            distribution.name == column.name,
            "ClickBench distribution '{}' does not match schema column '{}'",
            distribution.name,
            column.name
        );
        anyhow::ensure!(
            distribution.zero_or_empty_ppm <= 1_000_000,
            "ClickBench distribution '{}' has invalid empty probability",
            column.name
        );
        anyhow::ensure!(
            distribution.estimated_cardinality > 0,
            "ClickBench distribution '{}' has no values",
            column.name
        );
        anyhow::ensure!(
            distribution.mean.is_finite()
                && (!matches!(column.kind, ColumnKind::Binary) || distribution.mean >= 0.0),
            "ClickBench distribution '{}' has an invalid mean",
            column.name
        );
        let quantiles = &distribution.nonzero_percentiles_0_to_100;
        anyhow::ensure!(
            quantiles.is_empty() || quantiles.len() == PERCENTILE_COUNT,
            "ClickBench distribution '{}' has an invalid quantile count",
            column.name
        );
        anyhow::ensure!(
            quantiles.windows(2).all(|values| values[0] <= values[1]),
            "ClickBench distribution '{}' has unordered quantiles",
            column.name
        );
        anyhow::ensure!(
            quantiles.iter().all(|value| column.kind.accepts(*value)),
            "ClickBench distribution '{}' exceeds its Arrow type",
            column.name
        );
        if matches!(column.kind, ColumnKind::Binary) {
            anyhow::ensure!(
                quantiles.iter().all(|value| *value >= 0),
                "ClickBench distribution '{}' has a negative string length",
                column.name
            );
        }
    }
    anyhow::ensure!(
        logical_row_bytes_from(distributions) <= distributions.mean_arrow_row_bytes_upper_bound,
        "ClickBench mean row width exceeds its conservative bound"
    );
    Ok(())
}

fn logical_row_bytes_from(distributions: &DistributionFile) -> u64 {
    COLUMNS
        .iter()
        .zip(&distributions.columns)
        .map(|(column, distribution)| {
            column
                .kind
                .fixed_width()
                .unwrap_or_else(|| distribution.mean.ceil() as u64 + 4)
        })
        .sum()
}
