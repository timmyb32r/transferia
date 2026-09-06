//! One source type contract for discovery, native blocks and Parquet snapshots.
use std::str::FromStr as _;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};
use clickhouse_arrow::{ArrowOptions, Type};
use serde::{Deserialize, Serialize};
use transferia_core::data::schema::SchemaColumn;

use super::config::UnsupportedTypePolicy;

const SOURCE_TYPE_EXTENSION: &str = "transferia.clickhouse.source_type";

#[derive(Deserialize, Serialize)]
struct SourceTypeMetadata {
    source_type: String,
    conversion: SourceConversion,
}

#[derive(Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SourceConversion { Native, ToString }

pub(super) fn source_column(name: &str, declaration: &str, policy: UnsupportedTypePolicy) -> anyhow::Result<SchemaColumn> {
    let parsed = Type::from_str(declaration);
    let conversion_nullable = parsed.as_ref().map_or(true, |parsed| {
        match parsed { Type::LowCardinality(inner) => inner.is_nullable(), _ => parsed.is_nullable() }
    });
    let native = parsed.map_err(anyhow::Error::from)
        .and_then(|parsed| source_arrow_type(&parsed, declaration));
    let (data_type, nullable, conversion) = match native {
        Ok((data_type, nullable)) => (data_type, nullable, SourceConversion::Native),
        Err(error) if policy == UnsupportedTypePolicy::Fail => anyhow::bail!(
            "cannot decode the source type: {error:#}. Choose to_string in Advanced options to explicitly convert unsupported columns to text",
        ),
        Err(_) => (DataType::Utf8, conversion_nullable, SourceConversion::ToString),
    };
    Ok(SchemaColumn::new(name.to_owned(), data_type, nullable).with_arrow_extension_metadata(
        SOURCE_TYPE_EXTENSION,
        serde_json::to_string(&SourceTypeMetadata { source_type: declaration.to_owned(), conversion })?,
    ))
}

fn metadata(column: &SchemaColumn) -> Option<SourceTypeMetadata> {
    (column.arrow_extension_name == Some(SOURCE_TYPE_EXTENSION)).then_some(())?;
    serde_json::from_str(column.arrow_extension_metadata.as_deref()?).ok()
}

pub(super) fn is_string_conversion(column: &SchemaColumn) -> bool {
    metadata(column).is_some_and(|metadata| metadata.conversion == SourceConversion::ToString)
}

pub(super) fn source_declaration(column: &SchemaColumn) -> Option<String> {
    metadata(column).map(|metadata| metadata.source_type)
}

pub(super) fn wire_declaration(column: &SchemaColumn) -> Option<String> {
    metadata(column).map(|metadata| if metadata.conversion == SourceConversion::ToString {
        if column.nullable { "Nullable(String)" } else { "String" }.to_owned()
    } else { metadata.source_type })
}

pub(super) fn validate_wire_type(actual: &Field, column: &SchemaColumn) -> anyhow::Result<bool> {
    validate_wire_declaration(actual, wire_declaration(column).as_deref())
}

pub(super) fn validate_wire_declaration(actual: &Field, expected: Option<&str>) -> anyhow::Result<bool> {
    let name = actual.name();
    let Some(actual) = actual.metadata().get("clickhouse.type") else { return Ok(false) };
    let Some(expected) = expected else { return Ok(false) };
    anyhow::ensure!(actual == expected || canonical_declaration(actual) == canonical_declaration(expected),
        "column '{name}': ClickHouse source type drifted from {expected} to {actual}");
    Ok(true)
}

/// Whitespace outside quoted names/values is formatting, not type identity.
fn canonical_declaration(value: &str) -> String {
    let mut quote = None;
    let mut escaped = false;
    value.chars().filter(|&ch| {
        if escaped { escaped = false; return true; }
        if quote.is_some() && ch == '\\' { escaped = true; return true; }
        if quote == Some(ch) { quote = None; return true; }
        if quote.is_none() && matches!(ch, '\'' | '"' | '`') { quote = Some(ch); return true; }
        quote.is_some() || !ch.is_whitespace()
    }).collect()
}

pub(super) fn source_arrow_type(parsed: &Type, declaration: &str) -> anyhow::Result<(DataType, bool)> {
    let args = arguments(declaration)?;
    let mapped = match parsed {
        Type::UInt8 if declaration.trim() == "Bool" => (DataType::Boolean, false),
        Type::Nullable(inner) => {
            let (data_type, _) = source_arrow_type(inner, argument(&args, 0)?)?;
            (data_type, true)
        }
        Type::LowCardinality(inner) => {
            let (data_type, nullable) = source_arrow_type(inner, argument(&args, 0)?)?;
            (DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type)), nullable)
        }
        Type::Array(inner) => {
            let (data_type, nullable) = source_arrow_type(inner, argument(&args, 0)?)?;
            (DataType::List(Arc::new(Field::new("item", data_type, nullable))), false)
        }
        Type::Tuple(members) => {
            anyhow::ensure!(members.len() == args.len(), "invalid tuple declaration {declaration}");
            let fields = members.iter().zip(&args).enumerate().map(|(index, (member, arg))| {
                let (name, declaration) = tuple_member(arg, index)?;
                let (data_type, nullable) = source_arrow_type(member, declaration)?;
                Ok(Field::new(name, data_type, nullable))
            }).collect::<anyhow::Result<Vec<_>>>()?;
            (DataType::Struct(fields.into()), false)
        }
        Type::Map(key, value) => {
            let (key_type, key_nullable) = source_arrow_type(key, argument(&args, 0)?)?;
            anyhow::ensure!(!key_nullable, "Map keys cannot be nullable");
            let (value_type, value_nullable) = source_arrow_type(value, argument(&args, 1)?)?;
            (DataType::Map(Arc::new(Field::new("entries", DataType::Struct(vec![
                Field::new("key", key_type, false), Field::new("value", value_type, value_nullable),
            ].into()), false)), false), false)
        }
        // These families have no ordinary Arrow scalar representation. Do not silently
        // reinterpret an integer/address as driver-specific fixed-width bytes.
        Type::Int128 | Type::UInt128 | Type::Int256 | Type::UInt256
        | Type::Uuid | Type::Ipv4 | Type::Ipv6 | Type::Object => anyhow::bail!(
            "ClickHouse type {declaration} has no ordinary Arrow source representation",
        ),
        Type::DateTime(timezone) => (DataType::Timestamp(TimeUnit::Second,
            (!args.is_empty()).then(|| Arc::from(timezone.name()))), false),
        Type::DateTime64(precision, timezone) => {
            let unit = match precision { 0 => TimeUnit::Second, 1..=3 => TimeUnit::Millisecond,
                4..=6 => TimeUnit::Microsecond, 7..=9 => TimeUnit::Nanosecond,
                _ => anyhow::bail!("invalid DateTime64 precision {precision}") };
            (DataType::Timestamp(unit, (args.len() == 2).then(|| Arc::from(timezone.name()))), false)
        }
        Type::FixedSizedString(length) | Type::FixedSizedBinary(length) =>
            (DataType::FixedSizeBinary(i32::try_from(*length)?), false),
        Type::Point | Type::Ring | Type::Polygon | Type::MultiPolygon => {
            let (data_type, nullable) = clickhouse_arrow::arrow::ch_to_arrow_type(parsed, Some(ArrowOptions::strict()))?;
            (geo_member_names(data_type), nullable)
        }
        Type::Decimal32(scale) | Type::Decimal64(scale) | Type::Decimal128(scale)
        | Type::Decimal256(scale) if declaration.trim_start().starts_with("Decimal(") => {
            let precision: u8 = argument(&args, 0)?.parse()?;
            let scale = i8::try_from(*scale)?;
            (if precision <= 38 { DataType::Decimal128(precision, scale) }
                else { DataType::Decimal256(precision, scale) }, false)
        }
        _ => clickhouse_arrow::arrow::ch_to_arrow_type(parsed, Some(ArrowOptions::strict()))?,
    };
    Ok(mapped)
}

fn argument<'a>(arguments: &[&'a str], index: usize) -> anyhow::Result<&'a str> {
    arguments.get(index).copied().ok_or_else(|| anyhow::anyhow!("missing type argument {index}"))
}

fn arguments(declaration: &str) -> anyhow::Result<Vec<&str>> {
    let declaration = declaration.trim();
    let Some(open) = declaration.find('(') else { return Ok(Vec::new()) };
    let body = declaration[open + 1..].strip_suffix(')').ok_or_else(|| anyhow::anyhow!("unclosed type declaration"))?;
    if body.trim().is_empty() { return Ok(Vec::new()) }
    let mut result = Vec::new();
    let (mut start, mut depth, mut quote, mut escaped) = (0, 0_u32, None, false);
    let mut chars = body.char_indices().peekable();
    while let Some((offset, ch)) = chars.next() {
        if escaped { escaped = false; continue; }
        if quote.is_some() && ch == '\\' { escaped = true; continue; }
        if quote == Some(ch) {
            if chars.peek().is_some_and(|(_, next)| *next == ch) { chars.next(); }
            else { quote = None; }
            continue;
        }
        if quote.is_some() { continue; }
        match ch {
            '\'' | '"' | '`' => quote = Some(ch),
            '(' => depth += 1,
            ')' => depth = depth.checked_sub(1).ok_or_else(|| anyhow::anyhow!("unbalanced type declaration"))?,
            ',' if depth == 0 => { result.push(body[start..offset].trim()); start = offset + 1; }
            _ => {}
        }
    }
    anyhow::ensure!(quote.is_none() && depth == 0, "unbalanced type declaration");
    result.push(body[start..].trim());
    Ok(result)
}

fn tuple_member(argument: &str, index: usize) -> anyhow::Result<(String, &str)> {
    let argument = argument.trim();
    if Type::from_str(argument).is_ok() { return Ok(((index + 1).to_string(), argument)) }
    let (name, rest) = if argument.starts_with(['`', '"']) {
        Type::parse_quoted_identifier(argument)?
    } else {
        let end = argument.find(char::is_whitespace).ok_or_else(|| anyhow::anyhow!("missing tuple member type"))?;
        (argument[..end].to_owned(), &argument[end..])
    };
    anyhow::ensure!(rest.starts_with(char::is_whitespace), "missing space after tuple member name");
    let declaration = rest.trim();
    Type::from_str(declaration)?;
    Ok((name, declaration))
}

fn geo_member_names(data_type: DataType) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().enumerate().map(|(index, field)| {
            Arc::new(field.as_ref().clone().with_name((index + 1).to_string())
                .with_data_type(geo_member_names(field.data_type().clone())))
        }).collect()),
        DataType::List(field) => DataType::List(Arc::new(field.as_ref().clone()
            .with_data_type(geo_member_names(field.data_type().clone())))),
        data_type => data_type,
    }
}
