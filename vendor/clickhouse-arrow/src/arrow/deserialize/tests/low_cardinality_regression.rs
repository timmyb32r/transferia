use std::io::Cursor;

use super::*;

fn wire_dictionary(values: &[&str], width: usize, indices: &[u64]) -> Vec<u8> {
    let index_type = match width {
        1 => TUINT8,
        2 => TUINT16,
        4 => TUINT32,
        8 => TUINT64,
        _ => panic!("invalid test key width"),
    };
    let mut bytes = (HAS_ADDITIONAL_KEYS_BIT | index_type).to_le_bytes().to_vec();
    bytes.extend_from_slice(&u64::try_from(values.len()).unwrap().to_le_bytes());
    for value in values {
        // All fixture strings have a one-byte varuint length.
        bytes.push(u8::try_from(value.len()).unwrap());
        bytes.extend_from_slice(value.as_bytes());
    }
    bytes.extend_from_slice(&u64::try_from(indices.len()).unwrap().to_le_bytes());
    for index in indices {
        assert!(width == 8 || *index < (1_u64 << (width * 8)));
        bytes.extend_from_slice(&index.to_le_bytes()[..width]);
    }
    bytes
}

async fn decode_both(
    nullable: bool,
    key_type: DataType,
    bytes: Vec<u8>,
    rows: usize,
    nulls: &[u8],
) -> [Result<ArrayRef>; 2] {
    let inner = if nullable { Type::Nullable(Box::new(Type::String)) } else { Type::String };
    let data_type = DataType::Dictionary(Box::new(key_type), Box::new(DataType::Utf8));
    let mut async_builder =
        TypedBuilder::try_new(&Type::LowCardinality(Box::new(inner.clone())), &data_type).unwrap();
    let asynchronous = deserialize_async(
        &inner,
        &mut async_builder,
        &data_type,
        &mut Cursor::new(bytes.clone()),
        rows,
        nulls,
        &mut Vec::new(),
    ).await;
    let mut sync_builder = LowCardinalityBuilder::try_new(&inner, &data_type).unwrap();
    let synchronous = deserialize(
        &mut sync_builder,
        &mut Cursor::new(bytes),
        &inner,
        &data_type,
        rows,
        nulls,
        &mut Vec::new(),
    );
    [asynchronous, synchronous]
}

#[tokio::test]
async fn empty_nullable_dictionary_with_no_rows_is_valid() {
    for result in decode_both(true, DataType::Int32, wire_dictionary(&[], 1, &[]), 0, &[]).await {
        let array = result.unwrap();
        let dictionary = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert!(dictionary.is_empty());
        assert!(dictionary.values().is_empty());
    }
}

#[tokio::test]
async fn every_wire_width_rejects_dictionary_indexes_outside_the_dictionary() {
    for width in [1, 2, 4, 8] {
        for nullable in [false, true] {
            for (values, index) in [(vec![], 0), (vec!["", "a"], 2)] {
                for result in decode_both(nullable, DataType::Int32,
                    wire_dictionary(&values, width, &[index]), 1, &[]).await {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("outside dictionary"), "{error}");
                }
            }
        }
    }
}

#[tokio::test]
async fn uint64_keys_cannot_wrap_into_valid_int32_indexes() {
    for index in [i32::MAX as u64 + 1, 1_u64 << 32, (1_u64 << 32) + 1, (1_u64 << 63) + 1, u64::MAX] {
        for result in decode_both(false, DataType::Int32,
            wire_dictionary(&["a", "b"], 8, &[index]), 1, &[]).await {
            let error = result.unwrap_err().to_string();
            assert!(error.contains("outside dictionary"), "{error}");
            assert!(error.contains(&index.to_string()), "{error}");
        }
    }
}

#[tokio::test]
async fn valid_dictionary_indexes_must_fit_the_arrow_key_type() {
    for (key_type, index) in [(DataType::Int8, 128_u64), (DataType::UInt8, 256_u64)] {
        let values = vec!["a"; usize::try_from(index).unwrap() + 1];
        for result in decode_both(false, key_type, wire_dictionary(&values, 8, &[index]), 1, &[]).await {
            let error = result.unwrap_err().to_string();
            assert!(error.contains("cannot be represented"), "{error}");
        }
    }
}

#[tokio::test]
async fn every_wire_and_arrow_key_width_preserves_the_same_values() {
    for width in [1, 2, 4, 8] {
        for key_type in [DataType::UInt8, DataType::UInt16, DataType::UInt32, DataType::UInt64,
            DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64] {
            for result in decode_both(false, key_type,
                wire_dictionary(&["b", "a"], width, &[1, 0, 1]), 3, &[]).await {
                let array = result.unwrap();
                let unpacked = arrow::compute::cast(array.as_ref(), &DataType::Utf8).unwrap();
                assert_eq!(unpacked.as_any().downcast_ref::<StringArray>().unwrap(),
                    &StringArray::from(vec!["a", "b", "a"]));
            }
        }
    }
}

#[tokio::test]
async fn nullable_dictionary_and_outer_null_mask_keep_distinct_semantics() {
    for result in decode_both(true, DataType::Int32,
        wire_dictionary(&["", "a", "b"], 8, &[0, u64::MAX, 2, 1]), 4, &[0, 1, 0, 0]).await {
        let array = result.unwrap();
        let dictionary = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(dictionary.keys(), &Int32Array::from(vec![Some(0), None, Some(2), Some(1)]));
        assert_eq!(dictionary.values().as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![None, Some("a"), Some("b")]));
        let unpacked = arrow::compute::cast(array.as_ref(), &DataType::Utf8).unwrap();
        assert_eq!(unpacked.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![None, None, Some("b"), Some("a")]));
    }
}

#[tokio::test]
async fn malformed_null_mask_and_truncated_key_payload_return_errors() {
    for result in decode_both(false, DataType::Int32,
        wire_dictionary(&["a"], 1, &[0, 0]), 2, &[0]).await {
        assert!(result.unwrap_err().to_string().contains("null mask length"));
    }
    let mut bytes = wire_dictionary(&["a"], 8, &[0]);
    bytes.pop();
    for result in decode_both(false, DataType::Int32, bytes, 1, &[]).await {
        assert!(result.is_err());
    }
}

#[test]
fn key_buffer_size_overflow_is_rejected_before_allocation() {
    let error = prepare_key_buffer(usize::MAX, 8, &[], &mut Vec::new()).unwrap_err();
    assert!(error.to_string().contains("key byte count"));
}

#[tokio::test]
async fn impossible_nullable_dictionary_size_returns_an_error_instead_of_panicking() {
    let mut bytes = HAS_ADDITIONAL_KEYS_BIT.to_le_bytes().to_vec();
    bytes.extend_from_slice(&u64::MAX.to_le_bytes());
    for result in decode_both(true, DataType::Int32, bytes, 0, &[]).await {
        let error = result.unwrap_err().to_string();
        assert!(error.contains("dictionary null mask") || error.contains("dictionary size"), "{error}");
    }
}
