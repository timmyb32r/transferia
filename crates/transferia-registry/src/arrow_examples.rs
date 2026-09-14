//! Shared concrete Arrow examples for the generator and live type catalog.
//! Null is excluded: it has no typed values. Parameters are examples, not an
//! exhaustive statement about every precision, timezone or nested schema.
use std::sync::Arc;
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionFields, UnionMode};

pub fn types() -> Vec<(&'static str, DataType)> {
    let item = Arc::new(Field::new("item", DataType::Int32, false));
    let fields = vec![Field::new("value", DataType::Int32, false)];
    let union: UnionFields = fields
        .iter()
        .cloned()
        .map(|field| (0, Arc::new(field)))
        .collect();
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ]
            .into(),
        ),
        false,
    ));
    let mut types = vec![
        ("id", DataType::UInt64),
        ("boolean", DataType::Boolean),
        ("int8", DataType::Int8),
        ("int16", DataType::Int16),
        ("int32", DataType::Int32),
        ("int64", DataType::Int64),
        ("uint8", DataType::UInt8),
        ("uint16", DataType::UInt16),
        ("uint32", DataType::UInt32),
        ("uint64", DataType::UInt64),
        ("float16", DataType::Float16),
        ("float32", DataType::Float32),
        ("float64", DataType::Float64),
        ("date32", DataType::Date32),
        ("date64", DataType::Date64),
        ("time32_s", DataType::Time32(TimeUnit::Second)),
        ("time32_ms", DataType::Time32(TimeUnit::Millisecond)),
        ("time64_us", DataType::Time64(TimeUnit::Microsecond)),
        ("time64_ns", DataType::Time64(TimeUnit::Nanosecond)),
        (
            "interval_months",
            DataType::Interval(IntervalUnit::YearMonth),
        ),
        (
            "interval_days_ms",
            DataType::Interval(IntervalUnit::DayTime),
        ),
        (
            "interval_months_days_ns",
            DataType::Interval(IntervalUnit::MonthDayNano),
        ),
        ("binary", DataType::Binary),
        ("large_binary", DataType::LargeBinary),
        ("binary_view", DataType::BinaryView),
        ("fixed_binary", DataType::FixedSizeBinary(4)),
        ("utf8", DataType::Utf8),
        ("large_utf8", DataType::LargeUtf8),
        ("utf8_view", DataType::Utf8View),
        ("decimal32", DataType::Decimal32(9, 2)),
        ("decimal64", DataType::Decimal64(18, 2)),
        ("decimal128", DataType::Decimal128(38, 2)),
        ("decimal256", DataType::Decimal256(76, 2)),
        ("list", DataType::List(Arc::clone(&item))),
        ("large_list", DataType::LargeList(Arc::clone(&item))),
        ("list_view", DataType::ListView(Arc::clone(&item))),
        (
            "large_list_view",
            DataType::LargeListView(Arc::clone(&item)),
        ),
        ("fixed_list", DataType::FixedSizeList(item, 1)),
        ("struct", DataType::Struct(fields.into())),
        (
            "sparse_union",
            DataType::Union(union.clone(), UnionMode::Sparse),
        ),
        ("dense_union", DataType::Union(union, UnionMode::Dense)),
        (
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ),
        ("map", DataType::Map(entries, false)),
        (
            "run_end_encoded",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::Int32, true)),
            ),
        ),
    ];
    for (plain, zoned, duration, unit) in [
        (
            "timestamp_s",
            "timestamp_s_utc",
            "duration_s",
            TimeUnit::Second,
        ),
        (
            "timestamp_ms",
            "timestamp_ms_utc",
            "duration_ms",
            TimeUnit::Millisecond,
        ),
        (
            "timestamp_us",
            "timestamp_us_utc",
            "duration_us",
            TimeUnit::Microsecond,
        ),
        (
            "timestamp_ns",
            "timestamp_ns_utc",
            "duration_ns",
            TimeUnit::Nanosecond,
        ),
    ] {
        types.push((plain, DataType::Timestamp(unit, None)));
        types.push((zoned, DataType::Timestamp(unit, Some(Arc::from("UTC")))));
        types.push((duration, DataType::Duration(unit)));
    }
    types
}
