use std::str::FromStr;

use crate::Type;

#[test]
fn decimal_zero_scale_is_valid_for_every_storage_width() {
    for (declaration, expected) in [
        ("Decimal(1, 0)", Type::Decimal32(0)),
        ("Decimal(9, 0)", Type::Decimal32(0)),
        ("Decimal(18, 0)", Type::Decimal64(0)),
        ("Decimal(38, 0)", Type::Decimal128(0)),
        ("Decimal(76, 0)", Type::Decimal256(0)),
        ("Decimal32(0)", Type::Decimal32(0)),
        ("Decimal64(0)", Type::Decimal64(0)),
        ("Decimal128(0)", Type::Decimal128(0)),
        ("Decimal256(0)", Type::Decimal256(0)),
    ] {
        let parsed = Type::from_str(declaration).unwrap();
        assert_eq!(parsed, expected, "{declaration}");
        assert!(parsed.validate().is_ok(), "{declaration}");
    }
}

#[test]
fn decimal_scale_is_bounded_by_declared_precision_not_backing_width() {
    for declaration in [
        "Decimal(0, 0)", "Decimal(77, 0)", "Decimal(1, 2)", "Decimal(10, 11)",
        "Decimal(19, 20)", "Decimal(39, 40)", "Decimal32(10)", "Decimal64(19)",
        "Decimal128(39)", "Decimal256(77)",
    ] {
        assert!(Type::from_str(declaration).is_err(), "{declaration}");
    }
    for precision in 1..=76 {
        let declaration = format!("Decimal({precision}, {precision})");
        assert!(Type::from_str(&declaration).is_ok(), "{declaration}");
    }
}

#[test]
fn datetime64_precision_is_zero_through_nine_in_parse_and_validation() {
    for precision in 0..=9 {
        let parsed = Type::from_str(&format!("DateTime64({precision}, 'Europe/Moscow')"))
            .unwrap();
        assert_eq!(parsed, Type::DateTime64(precision, chrono_tz::Europe::Moscow));
        assert!(parsed.validate().is_ok());
    }
    for precision in [10, 18, usize::MAX] {
        assert!(Type::from_str(&format!("DateTime64({precision})")).is_err());
        assert!(Type::DateTime64(precision, chrono_tz::UTC).validate().is_err());
    }
}

#[test]
fn fixed_string_size_must_fit_the_arrow_fixed_binary_width() {
    let maximum = i32::MAX as usize;
    assert_eq!(
        Type::from_str(&format!("FixedString({maximum})")).unwrap(),
        Type::FixedSizedString(maximum),
    );
    for size in [0, maximum + 1, usize::MAX] {
        assert!(Type::from_str(&format!("FixedString({size})")).is_err());
        assert!(Type::FixedSizedString(size).validate().is_err());
        assert!(Type::FixedSizedBinary(size).validate().is_err());
    }
}

#[test]
fn enum_labels_decode_clickhouse_quoted_bytes_without_losing_escapes() {
    let declaration = r#"Enum16('quote\'' = -32768, 'double''quote' = 1, 'back\\slash' = 2, 'line\nfeed' = 3, 'nul\0byte' = 4, '\xD0\xAF' = 5, '\a\b\e\f\r\t\v' = 6, '\q' = 7, 'a\Nb' = 8, '\"\`\/\=' = 9)"#;
    assert_eq!(
        Type::from_str(declaration).unwrap(),
        Type::Enum16(vec![
            ("quote'".into(), -32768),
            ("double'quote".into(), 1),
            ("back\\slash".into(), 2),
            ("line\nfeed".into(), 3),
            ("nul\0byte".into(), 4),
            ("Я".into(), 5),
            ("\u{7}\u{8}\u{1b}\u{c}\r\t\u{b}".into(), 6),
            ("\\q".into(), 7),
            ("ab".into(), 8),
            ("\"`/=".into(), 9),
        ]),
    );
}

#[test]
fn enum_invalid_utf8_and_malformed_escapes_fail_without_replacement() {
    for declaration in [
        r"Enum8('\xFF' = 1)",
        r"Enum8('\xD0' = 1)",
        r"Enum8('\xZ1' = 1)",
        r"Enum8('\x1' = 1)",
        r"Enum8('unfinished\' = 1)",
        "Enum8('a' = 1 2)",
    ] {
        assert!(Type::from_str(declaration).is_err(), "{declaration}");
    }
}

#[test]
fn duplicate_enum_labels_and_codes_fail_in_parse_and_validation() {
    for family in ["Enum8", "Enum16"] {
        for arguments in ["'same'=1, 'same'=2", "'a'=1, 'b'=1", r"'a'=1, '\x61'=2"] {
            let declaration = format!("{family}({arguments})");
            assert!(Type::from_str(&declaration).is_err(), "{declaration}");
        }
    }
    for invalid in [
        Type::Enum8(vec![("same".into(), 1), ("same".into(), 2)]),
        Type::Enum8(vec![("a".into(), 1), ("b".into(), 1)]),
        Type::Enum16(vec![("same".into(), 1), ("same".into(), 2)]),
        Type::Enum16(vec![("a".into(), 1), ("b".into(), 1)]),
    ] {
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn nested_enum_delimiters_and_escaped_backslash_keep_their_meaning() {
    let parsed = Type::from_str(r"Tuple(Enum8('ends\\'=1, 'a,b)'=2), Decimal(4,0))")
        .unwrap();
    assert_eq!(
        parsed,
        Type::Tuple(vec![
            Type::Enum8(vec![("ends\\".into(), 1), ("a,b)".into(), 2)]),
            Type::Decimal32(0),
        ]),
    );
    for declaration in ["Tuple(UInt8))", "Array(Enum8('unclosed=1))"] {
        assert!(Type::from_str(declaration).is_err(), "{declaration}");
    }
}

#[test]
fn enum_display_roundtrips_literal_backslashes_quotes_and_control_characters() {
    let label = "literal\\n and \\N, quote' and actual\nnewline\0nul";
    for type_hint in [
        Type::Enum8(vec![(label.into(), -128)]),
        Type::Enum16(vec![(label.into(), 32767)]),
    ] {
        assert_eq!(Type::from_str(&type_hint.to_string()).unwrap(), type_hint);
    }
}

#[test]
fn constructed_decimal_types_reject_invalid_scale_at_validation() {
    for type_hint in [
        Type::Decimal32(10),
        Type::Decimal64(19),
        Type::Decimal128(39),
        Type::Decimal256(77),
    ] {
        assert!(type_hint.validate().is_err());
    }
}

#[test]
fn named_tuples_parse_spaces_delimiters_doubled_quotes_and_nested_types() {
    let declaration = r#"Tuple(`hello world` String, "comma,parenthesis)" Array(UInt8), `back``tick` Tuple("double""quote" Int64), `trailing\\` Decimal(4,0))"#;
    assert_eq!(Type::from_str(declaration).unwrap(), Type::Tuple(vec![
        Type::String,
        Type::Array(Box::new(Type::UInt8)),
        Type::Tuple(vec![Type::Int64]),
        Type::Decimal32(0),
    ]));
}

#[test]
fn quoted_identifiers_share_lossless_clickhouse_escape_semantics() {
    for (input, expected) in [
        (r"`line\nbreak` String", "line\nbreak"),
        (r"`\xD0\xAF` String", "Я"),
        (r"`back\\slash` String", "back\\slash"),
        (r"`back``tick` String", "back`tick"),
        (r#""double""quote" String"#, "double\"quote"),
        (r"`nul\0byte` String", "nul\0byte"),
        (r"`unknown\q` String", "unknown\\q"),
    ] {
        let (name, rest) = Type::parse_quoted_identifier(input).unwrap();
        assert_eq!(name, expected);
        assert_eq!(rest, " String");
    }
}

#[test]
fn malformed_named_tuples_never_drop_invalid_identifier_or_type_bytes() {
    for declaration in [
        r"Tuple(`\xFF` String)", r"Tuple(`\xD0` String)",
        r"Tuple(`\x0` String)", r"Tuple(`unclosed String)",
        "Tuple(`name`Int64)", "Tuple(`name` Int64 garbage)",
        "Tuple(\"name\"\" String)", "Tuple(name\tUnknownType)",
    ] {
        assert!(Type::from_str(declaration).is_err(), "{declaration}");
    }
    assert_eq!(Type::from_str("Tuple(name\tString)").unwrap(), Type::Tuple(vec![Type::String]));
}
