//! Exact fixed-precision NUMERIC values. PostgreSQL's typmod supplies precision
//! and scale; no value sampling or floating-point conversion determines a schema.
//! Decimal128 supports 1..=38 significant digits and an i8 scale <= precision.
//! Unbounded/wider typmods require the explicit text policy; non-finite values
//! and any coefficient that would require rounding fail before publication.
use arrow::datatypes::DataType;
use bytes::{BufMut as _, BytesMut};

pub(crate) fn data_type(typmod: i32) -> anyhow::Result<DataType> {
    anyhow::ensure!(typmod >= 4, "unbounded PostgreSQL NUMERIC requires unsupported_types=to_string");
    let modifier = typmod - 4;
    let precision = u8::try_from((modifier >> 16) & 0xffff)?;
    // PostgreSQL stores the signed scale in the low 11 bits.
    let scale = i8::try_from(((modifier & 0x7ff) ^ 1024) - 1024)?;
    validate_type(precision, scale)?;
    Ok(DataType::Decimal128(precision, scale))
}

pub(crate) fn validate_type(precision: u8, scale: i8) -> anyhow::Result<()> {
    anyhow::ensure!((1..=38).contains(&precision) && i16::from(scale) <= i16::from(precision),
        "PostgreSQL NUMERIC precision/scale {precision}/{scale} has no exact Decimal128 representation; use unsupported_types=to_string for batch text");
    Ok(())
}

pub(crate) fn validate_value(value: i128, precision: u8, scale: i8) -> anyhow::Result<()> {
    validate_type(precision, scale)?;
    anyhow::ensure!(value.unsigned_abs() < 10_u128.pow(u32::from(precision)),
        "PostgreSQL NUMERIC coefficient exceeds declared precision {precision}");
    Ok(())
}

/// Parse decimal/scientific PostgreSQL output without rounding. Errors omit the
/// source value: diagnostics must not reproduce user data.
pub(crate) fn parse(value: &str, precision: u8, scale: i8) -> anyhow::Result<i128> {
    validate_type(precision, scale)?;
    let (mantissa, exponent) = match value.find(['e', 'E']) {
        Some(index) => (&value[..index], value[index + 1..].parse::<i32>()
            .map_err(|_| anyhow::anyhow!("invalid PostgreSQL NUMERIC exponent"))?),
        None => (value, 0),
    };
    let (negative, mantissa) = if let Some(rest) = mantissa.strip_prefix('-') {
        (true, rest)
    } else { (false, mantissa.strip_prefix('+').unwrap_or(mantissa)) };
    let mut digits = Vec::with_capacity(mantissa.len());
    let mut fractional = None;
    for byte in mantissa.bytes() {
        if byte == b'.' && fractional.is_none() {
            fractional = Some(0_i32);
        } else {
            anyhow::ensure!(byte.is_ascii_digit(), "invalid or non-finite PostgreSQL NUMERIC cannot be represented as Decimal128");
            digits.push(byte);
            if let Some(count) = &mut fractional { *count = count.checked_add(1).ok_or_else(|| anyhow::anyhow!("NUMERIC scale overflow"))?; }
        }
    }
    anyhow::ensure!(!digits.is_empty(), "empty PostgreSQL NUMERIC coefficient");
    let first = digits.iter().position(|digit| *digit != b'0').unwrap_or(digits.len());
    let digits = &digits[first..];
    if digits.is_empty() { return Ok(0); }
    let shift = i32::from(scale).checked_add(exponent).and_then(|x| x.checked_sub(fractional.unwrap_or(0)))
        .ok_or_else(|| anyhow::anyhow!("NUMERIC scale overflow"))?;
    let (significant, zeros) = if shift < 0 {
        let remove = usize::try_from(shift.unsigned_abs())?;
        anyhow::ensure!(remove <= digits.len() && digits[digits.len() - remove..].iter().all(|digit| *digit == b'0'),
            "PostgreSQL NUMERIC cannot be represented exactly at declared scale {scale}");
        (&digits[..digits.len() - remove], 0)
    } else { (digits, usize::try_from(shift)?) };
    anyhow::ensure!(significant.len().checked_add(zeros).is_some_and(|count| count <= usize::from(precision)),
        "PostgreSQL NUMERIC coefficient exceeds declared precision {precision}");
    let mut coefficient = 0_i128;
    for digit in significant { coefficient = coefficient * 10 + i128::from(*digit - b'0'); }
    for _ in 0..zeros { coefficient *= 10; }
    Ok(if negative { -coefficient } else { coefficient })
}

pub(crate) fn format(value: i128, precision: u8, scale: i8) -> anyhow::Result<String> {
    validate_value(value, precision, scale)?;
    let magnitude = value.unsigned_abs().to_string();
    let mut output = String::new();
    if value < 0 { output.push('-'); }
    if scale <= 0 {
        output.push_str(&magnitude);
        if value != 0 { output.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs()))); }
    } else {
        let scale = usize::try_from(scale)?;
        if magnitude.len() <= scale {
            output.push_str("0.");
            output.extend(std::iter::repeat_n('0', scale - magnitude.len()));
            output.push_str(&magnitude);
        } else {
            let split = magnitude.len() - scale;
            output.push_str(&magnitude[..split]); output.push('.'); output.push_str(&magnitude[split..]);
        }
    }
    Ok(output)
}

/// Append one COPY field, including its length, in PostgreSQL base-10000 format.
/// Eleven stack digits suffice for Decimal128's 38 digits plus 3 alignment zeros.
pub(crate) fn encode_binary(output: &mut BytesMut, value: i128, precision: u8, scale: i8) -> anyhow::Result<()> {
    validate_value(value, precision, scale)?;
    let mut magnitude = value.unsigned_abs();
    let exponent = -i16::from(scale);
    let factor = 10_u128.pow(u32::try_from(exponent.rem_euclid(4))?);
    let mut reversed = [0_i16; 11];
    let mut count = 0;
    let mut carry = 0;
    while magnitude != 0 || carry != 0 {
        let digit = (magnitude % 10_000) * factor + carry;
        reversed[count] = i16::try_from(digit % 10_000)?;
        carry = digit / 10_000;
        magnitude /= 10_000;
        count += 1;
    }
    let weight = if count == 0 { 0 } else { i16::try_from(count - 1)? + exponent.div_euclid(4) };
    let first = reversed[..count].iter().position(|digit| *digit != 0).unwrap_or(count);
    output.put_i32(i32::try_from(8 + (count - first) * 2)?);
    output.put_i16(i16::try_from(count - first)?);
    output.put_i16(weight);
    output.put_i16(if value < 0 { 0x4000 } else { 0 });
    output.put_i16(i16::from(scale.max(0)));
    for digit in reversed[first..count].iter().rev() { output.put_i16(*digit); }
    Ok(())
}

#[cfg(test)]
#[path = "tests/numeric.rs"]
mod tests;
