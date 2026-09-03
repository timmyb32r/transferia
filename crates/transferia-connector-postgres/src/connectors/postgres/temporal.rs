use std::sync::Arc;

use arrow::array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

const UNIX_EPOCH_JULIAN_DAY: i64 = 2_440_588;
const POSTGRES_EPOCH_JULIAN_DAY: i64 = 2_451_545;
const POSTGRES_DATE_END_JULIAN_DAY: i64 = 2_147_483_494;
const POSTGRES_MIN_TIMESTAMP_MICROS: i64 = -211_813_488_000_000_000;
const POSTGRES_END_TIMESTAMP_MICROS: i64 = 9_223_371_331_200_000_000;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SECOND;

const POSTGRES_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

pub(super) fn postgres_date_to_unix_days(value: i32) -> anyhow::Result<i32> {
    anyhow::ensure!(
        value != i32::MIN && value != i32::MAX,
        "PostgreSQL infinite date is not representable as Arrow Date32"
    );
    let julian = i64::from(value) + POSTGRES_EPOCH_JULIAN_DAY;
    anyhow::ensure!(
        (0..POSTGRES_DATE_END_JULIAN_DAY).contains(&julian),
        "PostgreSQL date value {value} is outside its finite range"
    );
    Ok(i32::try_from(julian - UNIX_EPOCH_JULIAN_DAY)?)
}

pub(super) fn unix_days_to_postgres_date(value: i32) -> anyhow::Result<i32> {
    let julian = i64::from(value) + UNIX_EPOCH_JULIAN_DAY;
    anyhow::ensure!(
        (0..POSTGRES_DATE_END_JULIAN_DAY).contains(&julian),
        "Arrow Date32 day {value} is outside the finite PostgreSQL date range"
    );
    let postgres = i32::try_from(julian - POSTGRES_EPOCH_JULIAN_DAY)?;
    anyhow::ensure!(
        postgres != i32::MIN && postgres != i32::MAX,
        "Arrow Date32 day {value} collides with a PostgreSQL infinite-date sentinel"
    );
    Ok(postgres)
}

pub(super) fn postgres_timestamp_to_unix_micros(value: i64) -> anyhow::Result<i64> {
    anyhow::ensure!(
        value != i64::MIN && value != i64::MAX,
        "PostgreSQL infinite timestamp is not representable as an Arrow timestamp"
    );
    anyhow::ensure!(
        (POSTGRES_MIN_TIMESTAMP_MICROS..POSTGRES_END_TIMESTAMP_MICROS).contains(&value),
        "PostgreSQL timestamp value {value} is outside its finite range"
    );
    value
        .checked_add(POSTGRES_EPOCH_UNIX_MICROS)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "PostgreSQL timestamp value {value} is outside the Arrow microsecond range"
            )
        })
}

pub(super) fn unix_micros_to_postgres_timestamp(value: i64) -> anyhow::Result<i64> {
    let postgres = value
        .checked_sub(POSTGRES_EPOCH_UNIX_MICROS)
        .ok_or_else(|| anyhow::anyhow!("Arrow timestamp {value} is outside PostgreSQL range"))?;
    anyhow::ensure!(
        (POSTGRES_MIN_TIMESTAMP_MICROS..POSTGRES_END_TIMESTAMP_MICROS).contains(&postgres),
        "Arrow timestamp {value} is outside the finite PostgreSQL timestamp range"
    );
    Ok(postgres)
}

pub(super) fn parse_date(value: &str) -> anyhow::Result<i32> {
    let (date, before_common_era) = split_era(value)?;
    let (year, month, day) = parse_date_parts(date, before_common_era)?;
    let julian = date_to_julian_day(year, month, day)?;
    anyhow::ensure!(
        (0..POSTGRES_DATE_END_JULIAN_DAY).contains(&julian),
        "PostgreSQL date '{value}' is outside its finite range"
    );
    Ok(i32::try_from(julian - UNIX_EPOCH_JULIAN_DAY)?)
}

pub(super) fn parse_timestamp(value: &str, with_timezone: bool) -> anyhow::Result<i64> {
    let (timestamp, before_common_era) = split_era(value)?;
    let (date, time) = timestamp
        .split_once(' ')
        .ok_or_else(|| anyhow::anyhow!("invalid PostgreSQL timestamp '{value}'"))?;
    let (year, month, day) = parse_date_parts(date, before_common_era)?;
    let julian = date_to_julian_day(year, month, day)?;
    let (time, offset_seconds) = split_timezone_offset(time, with_timezone)?;
    let time_micros = parse_time_micros(time)?;
    let unix_days = julian
        .checked_sub(UNIX_EPOCH_JULIAN_DAY)
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp '{value}' underflows"))?;
    let local_micros = unix_days
        .checked_mul(MICROS_PER_DAY)
        .and_then(|days| days.checked_add(time_micros))
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp '{value}' overflows Arrow"))?;
    let utc_micros = local_micros
        .checked_sub(offset_seconds * MICROS_PER_SECOND)
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp '{value}' overflows Arrow"))?;
    unix_micros_to_postgres_timestamp(utc_micros)?;
    Ok(utc_micros)
}

pub(super) fn format_date(value: i32) -> anyhow::Result<String> {
    unix_days_to_postgres_date(value)?;
    let (year, month, day) = julian_day_to_date(i64::from(value) + UNIX_EPOCH_JULIAN_DAY)?;
    Ok(format_date_parts(year, month, day))
}

pub(super) fn format_timestamp(value: i64, with_timezone: bool) -> anyhow::Result<String> {
    unix_micros_to_postgres_timestamp(value)?;
    let unix_days = value.div_euclid(MICROS_PER_DAY);
    let time_micros = value.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = julian_day_to_date(
        unix_days
            .checked_add(UNIX_EPOCH_JULIAN_DAY)
            .ok_or_else(|| anyhow::anyhow!("Arrow timestamp {value} is outside calendar range"))?,
    )?;
    let hour = time_micros / (3_600 * MICROS_PER_SECOND);
    let minute = time_micros / (60 * MICROS_PER_SECOND) % 60;
    let second = time_micros / MICROS_PER_SECOND % 60;
    let micros = time_micros % MICROS_PER_SECOND;
    let era = if year <= 0 { " BC" } else { "" };
    let display_year = if year <= 0 { 1 - year } else { year };
    let timezone = if with_timezone { "+00" } else { "" };
    Ok(format!(
        "{display_year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}{timezone}{era}"
    ))
}

pub(super) fn timestamp_micros(
    column: &dyn Array,
    row: usize,
    unit: &TimeUnit,
) -> anyhow::Result<i64> {
    let micros = match unit {
        TimeUnit::Second => downcast::<TimestampSecondArray>(column)?
            .value(row)
            .checked_mul(MICROS_PER_SECOND),
        TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(column)?
            .value(row)
            .checked_mul(1_000),
        TimeUnit::Microsecond => Some(downcast::<TimestampMicrosecondArray>(column)?.value(row)),
        TimeUnit::Nanosecond => {
            let nanos = downcast::<TimestampNanosecondArray>(column)?.value(row);
            anyhow::ensure!(
                nanos.rem_euclid(1_000) == 0,
                "PostgreSQL timestamp has microsecond precision; nanosecond value {nanos} is not lossless"
            );
            Some(nanos / 1_000)
        }
    };
    let micros = micros
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp conversion overflow"))?;
    unix_micros_to_postgres_timestamp(micros)?;
    Ok(micros)
}

pub(super) fn timestamp_has_timezone(data_type: &DataType) -> anyhow::Result<bool> {
    let DataType::Timestamp(_, timezone) = data_type else {
        anyhow::bail!("expected an Arrow timestamp, got {data_type:?}")
    };
    match timezone.as_deref() {
        None => Ok(false),
        Some("UTC") => Ok(true),
        Some(timezone) => anyhow::bail!(
            "PostgreSQL cannot preserve Arrow timestamp timezone '{timezone}'; use explicit UTC or no timezone"
        ),
    }
}

fn split_era(value: &str) -> anyhow::Result<(&str, bool)> {
    anyhow::ensure!(
        value != "infinity" && value != "-infinity",
        "PostgreSQL infinite temporal value is not representable in Arrow"
    );
    if let Some(value) = value.strip_suffix(" BC") {
        Ok((value, true))
    } else {
        Ok((value, false))
    }
}

fn parse_date_parts(value: &str, before_common_era: bool) -> anyhow::Result<(i64, u32, u32)> {
    let mut parts = value.split('-');
    let display_year = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid PostgreSQL date '{value}'"))?
        .parse::<i64>()?;
    let month = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid PostgreSQL date '{value}'"))?
        .parse::<u32>()?;
    let day = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid PostgreSQL date '{value}'"))?
        .parse::<u32>()?;
    anyhow::ensure!(parts.next().is_none(), "invalid PostgreSQL date '{value}'");
    anyhow::ensure!(display_year > 0, "PostgreSQL dates have no year zero");
    let year = if before_common_era {
        1_i64
            .checked_sub(display_year)
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL date year overflows"))?
    } else {
        display_year
    };
    anyhow::ensure!((1..=12).contains(&month), "invalid PostgreSQL month {month}");
    let max_day = days_in_month(year, month);
    anyhow::ensure!(
        (1..=max_day).contains(&day),
        "invalid PostgreSQL day {day} for year {display_year}, month {month}"
    );
    Ok((year, month, day))
}

fn parse_time_micros(value: &str) -> anyhow::Result<i64> {
    let mut parts = value.split(':');
    let hour = parse_time_part(parts.next(), "hour")?;
    let minute = parse_time_part(parts.next(), "minute")?;
    let second = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp has no second"))?;
    anyhow::ensure!(parts.next().is_none(), "invalid PostgreSQL time '{value}'");
    let (second, fraction) = second.split_once('.').map_or((second, ""), |parts| parts);
    let second = second.parse::<i64>()?;
    anyhow::ensure!((0..24).contains(&hour), "invalid PostgreSQL hour {hour}");
    anyhow::ensure!((0..60).contains(&minute), "invalid PostgreSQL minute {minute}");
    anyhow::ensure!((0..60).contains(&second), "invalid PostgreSQL second {second}");
    anyhow::ensure!(
        fraction.len() <= 6 && fraction.bytes().all(|byte| byte.is_ascii_digit()),
        "PostgreSQL timestamp fraction '{fraction}' exceeds microsecond precision"
    );
    let micros = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i64>()? * 10_i64.pow(u32::try_from(6 - fraction.len())?)
    };
    Ok(((hour * 60 + minute) * 60 + second) * MICROS_PER_SECOND + micros)
}

fn parse_time_part(value: Option<&str>, label: &str) -> anyhow::Result<i64> {
    value
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL timestamp has no {label}"))?
        .parse::<i64>()
        .map_err(Into::into)
}

fn split_timezone_offset(value: &str, required: bool) -> anyhow::Result<(&str, i64)> {
    let sign = value
        .char_indices()
        .find(|(index, character)| *index >= 8 && matches!(character, '+' | '-'));
    match sign {
        Some((index, sign)) => {
            anyhow::ensure!(required, "timestamp without time zone contains an offset");
            let seconds = parse_timezone_seconds(&value[index + 1..])?;
            Ok((&value[..index], if sign == '-' { -seconds } else { seconds }))
        }
        None => {
            anyhow::ensure!(!required, "timestamp with time zone has no numeric offset");
            Ok((value, 0))
        }
    }
}

fn parse_timezone_seconds(value: &str) -> anyhow::Result<i64> {
    let fields = if value.contains(':') {
        value.split(':').collect::<Vec<_>>()
    } else {
        match value.len() {
            2 => vec![value],
            4 => vec![&value[..2], &value[2..]],
            6 => vec![&value[..2], &value[2..4], &value[4..]],
            _ => anyhow::bail!("invalid PostgreSQL timezone offset '{value}'"),
        }
    };
    anyhow::ensure!((1..=3).contains(&fields.len()), "invalid timezone offset '{value}'");
    let hours = fields[0].parse::<i64>()?;
    let minutes = fields.get(1).map_or(Ok(0), |value| value.parse::<i64>())?;
    let seconds = fields.get(2).map_or(Ok(0), |value| value.parse::<i64>())?;
    anyhow::ensure!(
        (0..=15).contains(&hours)
            && (0..60).contains(&minutes)
            && (0..60).contains(&seconds),
        "invalid timezone offset '{value}'"
    );
    Ok((hours * 60 + minutes) * 60 + seconds)
}

fn date_to_julian_day(year: i64, month: u32, day: u32) -> anyhow::Result<i64> {
    let mut year = year;
    let month = i64::from(month);
    let month = if month > 2 {
        year += 4_800;
        month + 1
    } else {
        year += 4_799;
        month + 13
    };
    let century = year / 100;
    year
        .checked_mul(365)
        .and_then(|julian| julian.checked_sub(32_167))
        .and_then(|julian| julian.checked_add(year / 4 - century + century / 4))
        .and_then(|julian| julian.checked_add(7_834 * month / 256 + i64::from(day)))
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL date is outside calendar range"))
}

fn julian_day_to_date(value: i64) -> anyhow::Result<(i64, i64, i64)> {
    anyhow::ensure!(value >= 0, "Julian day {value} is outside PostgreSQL range");
    let mut julian = value + 32_044;
    let mut quad = julian / 146_097;
    let extra = (julian - quad * 146_097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146_097;
    quad = julian / 1_461;
    julian -= quad * 1_461;
    let mut year = julian * 4 / 1_461;
    julian = if year != 0 {
        (julian + 305) % 365
    } else {
        (julian + 306) % 366
    } + 123;
    year += quad * 4;
    let month_quad = julian * 2_141 / 65_536;
    let day = julian - 7_834 * month_quad / 256;
    let month = (month_quad + 10) % 12 + 1;
    Ok((year - 4_800, month, day))
}

fn format_date_parts(year: i64, month: i64, day: i64) -> String {
    if year <= 0 {
        format!("{:04}-{month:02}-{day:02} BC", 1 - year)
    } else {
        format!("{year:04}-{month:02}-{day:02}")
    }
}

const fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

const fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn downcast<T: Array + 'static>(column: &dyn Array) -> anyhow::Result<&T> {
    column.as_any().downcast_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "Arrow array does not match declared type {:?}",
            column.data_type()
        )
    })
}

pub(super) fn timestamp_data_type(with_timezone: bool) -> DataType {
    DataType::Timestamp(
        TimeUnit::Microsecond,
        with_timezone.then(|| Arc::from("UTC")),
    )
}
