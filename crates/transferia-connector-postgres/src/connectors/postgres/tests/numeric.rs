use super::*;

fn typmod(precision: i32, scale: i32) -> i32 { ((precision << 16) | (scale & 0x7ff)) + 4 }

#[test]
fn discovery_requires_an_exact_fixed_decimal_contract() {
    for (precision, scale) in [(20, 0), (38, 18), (4, -3), (1, -128)] {
        assert_eq!(data_type(typmod(precision, scale)).unwrap(), DataType::Decimal128(precision as u8, scale as i8));
    }
    for modifier in [-1, typmod(39, 0), typmod(1000, 0), typmod(3, 5), typmod(3, -129)] {
        assert!(data_type(modifier).is_err());
        use crate::connectors::postgres::source::UnsupportedTypePolicy;
        assert!(UnsupportedTypePolicy::Fail.arrow_type_with_modifier(&tokio_postgres::types::Type::NUMERIC, modifier).is_err());
        assert_eq!(UnsupportedTypePolicy::ToString.arrow_type_with_modifier(&tokio_postgres::types::Type::NUMERIC, modifier).unwrap(), DataType::Utf8);
    }
}

#[test]
fn decimal_text_is_exact_above_float_integer_precision_and_at_scale_edges() {
    for (text, precision, scale, coefficient) in [
        ("18446744073709551615", 20, 0, 18_446_744_073_709_551_615),
        ("-99999999999999999999999999999999999999", 38, 0, -(10_i128.pow(38) - 1)),
        ("0.000000000000000001", 38, 18, 1),
        ("-1.234500", 8, 4, -12345),
        ("1.2345e2", 8, 4, 1234500),
        ("12000", 4, -3, 12),
        ("0.00000", 1, -128, 0),
        ("0000001.00", 3, 2, 100),
    ] {
        assert_eq!(parse(text, precision, scale).unwrap(), coefficient);
        assert_eq!(parse(&format(coefficient, precision, scale).unwrap(), precision, scale).unwrap(), coefficient);
    }
    for value in ["NaN", "Infinity", "-Infinity", "1.001", "12345", "1.1e-9", "", "1..0", "1e2147483647"] {
        assert!(parse(value, 4, 2).is_err(), "accepted {value}");
    }
    assert!(parse("12001", 4, -3).is_err());
    assert!(validate_value(1000, 3, 0).is_err());
    assert!(format(i128::MIN, 38, 0).is_err());
}

#[test]
fn binary_numeric_encodes_sign_weight_scale_and_base_10000_digits() {
    for (value, precision, scale, expected) in [
        (123456789_i128, 12, 4, vec![0,0,0,14, 0,3, 0,1, 0,0, 0,4, 0,1, 9,41, 26,133]),
        (-1, 2, 2, vec![0,0,0,10, 0,1, 255,255, 64,0, 0,2, 0,100]),
        (12, 4, -3, vec![0,0,0,12, 0,2, 0,1, 0,0, 0,0, 0,1, 7,208]),
        (0, 38, 18, vec![0,0,0,8, 0,0, 0,0, 0,0, 0,18]),
    ] {
        let mut bytes = BytesMut::new(); encode_binary(&mut bytes, value, precision, scale).unwrap();
        assert_eq!(bytes.as_ref(), expected, "{value}/{scale}");
    }
}
