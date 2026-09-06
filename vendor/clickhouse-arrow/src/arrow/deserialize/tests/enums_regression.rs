use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray};
use arrow::datatypes::{DataType, Int32Type};

use super::*;
use crate::arrow::ch_to_arrow_type;
use crate::arrow::deserialize::ClickHouseArrowDeserializer;
use crate::arrow::serialize::ClickHouseArrowSerializer;
use crate::formats::SerializerState;

async fn decode_both(type_: &Type, bytes: &[u8], rows: usize, nulls: &[u8]) -> [Result<ArrayRef>; 2] {
    let data_type = ch_to_arrow_type(type_, None).unwrap().0;
    assert_eq!(data_type, DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)));
    let mut asynchronous = TypedBuilder::try_new(type_, &data_type).unwrap();
    let async_result = type_.deserialize_arrow_async(
        &mut asynchronous, &mut Cursor::new(bytes), &data_type, rows, nulls, &mut Vec::new(),
    ).await;
    let mut synchronous = TypedBuilder::try_new(type_, &data_type).unwrap();
    let sync_result = type_.deserialize_arrow(
        &mut synchronous, &mut Cursor::new(bytes), &data_type, rows, nulls, &mut Vec::new(),
    );
    [async_result, sync_result]
}

async fn assert_complete_enum(type_: &Type, bytes: &[u8], labels: Vec<&str>) {
    for result in decode_both(type_, bytes, labels.len(), &[]).await {
        let array = result.unwrap();
        let dictionary = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(dictionary.values().as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(labels.clone()));
        let expected_keys: Vec<i32> = (0..labels.len()).rev().map(|index| i32::try_from(index).unwrap()).collect();
        assert_eq!(dictionary.keys(), &Int32Array::from(expected_keys));

        let mut sync_wire = Vec::new();
        type_.serialize(&mut sync_wire, &array, array.data_type(), &mut SerializerState::default()).unwrap();
        assert_eq!(sync_wire, bytes);
        let mut async_wire = Vec::new();
        type_.serialize_async(&mut async_wire, &array, array.data_type(), &mut SerializerState::default()).await.unwrap();
        assert_eq!(async_wire, bytes);
    }
}

#[tokio::test]
async fn enum8_preserves_all_256_labels_and_signed_codes_on_the_wire() {
    let pairs: Vec<_> = (i8::MIN..=i8::MAX).map(|code| (format!("label_{code}"), code)).collect();
    let wire: Vec<_> = (i8::MIN..=i8::MAX).rev().flat_map(i8::to_le_bytes).collect();
    let type_ = Type::Enum8(pairs.clone());
    assert_complete_enum(&type_, &wire, pairs.iter().map(|(label, _)| label.as_str()).collect()).await;
}

#[tokio::test]
async fn enum16_preserves_all_65536_labels_and_signed_codes_on_the_wire() {
    let pairs: Vec<_> = (i16::MIN..=i16::MAX).map(|code| (format!("label_{code}"), code)).collect();
    let wire: Vec<_> = (i16::MIN..=i16::MAX).rev().flat_map(i16::to_le_bytes).collect();
    let type_ = Type::Enum16(pairs.clone());
    assert_complete_enum(&type_, &wire, pairs.iter().map(|(label, _)| label.as_str()).collect()).await;
}

#[tokio::test]
async fn enum_dictionary_order_is_declared_order_not_first_seen_row_order() {
    let type_ = Type::Enum8(vec![("unused".into(), 4), ("quote' and slash\\".into(), -128), ("z".into(), 127)]);
    let wire: Vec<_> = [127_i8, -128, 127].into_iter().flat_map(i8::to_le_bytes).collect();
    for result in decode_both(&type_, &wire, 3, &[]).await {
        let array = result.unwrap();
        let dictionary = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(dictionary.keys(), &Int32Array::from(vec![2, 1, 2]));
        assert_eq!(dictionary.values().as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["unused", "quote' and slash\\", "z"]));
        let mut encoded = Vec::new();
        type_.serialize(&mut encoded, &array, array.data_type(), &mut SerializerState::default()).unwrap();
        assert_eq!(encoded, wire);
    }
}

#[tokio::test]
async fn unknown_signed_codes_fail_but_null_payloads_are_not_interpreted_as_codes() {
    for (type_, wire) in [
        (Type::Enum8(vec![("a".into(), 7)]), i8::MIN.to_le_bytes().to_vec()),
        (Type::Enum16(vec![("a".into(), 7)]), i16::MIN.to_le_bytes().to_vec()),
    ] {
        for result in decode_both(&type_, &wire, 1, &[]).await {
            assert!(result.unwrap_err().to_string().contains("Invalid enum code"));
        }
        for result in decode_both(&type_, &wire, 1, &[1]).await {
            let array = result.unwrap();
            let dictionary = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
            assert!(dictionary.is_null(0));
            assert_eq!(dictionary.values().as_any().downcast_ref::<StringArray>().unwrap(), &StringArray::from(vec!["a"]));
        }
    }
}

#[tokio::test]
async fn malformed_enum_null_mask_returns_an_error() {
    let type_ = Type::Enum8(vec![("a".into(), 7)]);
    for result in decode_both(&type_, &[7, 7], 2, &[0]).await {
        assert!(result.unwrap_err().to_string().contains("null mask length"));
    }
}

#[tokio::test]
async fn enum_builder_reuses_the_same_complete_dictionary_across_blocks() {
    let type_ = Type::Enum8(vec![("a".into(), -1), ("b".into(), 7)]);
    let data_type = ch_to_arrow_type(&type_, None).unwrap().0;
    let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
    let first = deserialize_async(&type_, &mut builder, &mut Cursor::new(vec![7]), 1, &[]).await.unwrap();
    let second = deserialize_async(&type_, &mut builder, &mut Cursor::new((-1_i8).to_le_bytes()), 1, &[]).await.unwrap();
    let first = first.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
    let second = second.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
    assert_eq!(first.keys(), &Int32Array::from(vec![1]));
    assert_eq!(second.keys(), &Int32Array::from(vec![0]));
    assert!(Arc::ptr_eq(first.values(), second.values()));
}

#[test]
fn duplicate_enum_codes_are_rejected_instead_of_selecting_a_label() {
    let type_ = Type::Enum8(vec![("a".into(), 1), ("b".into(), 1)]);
    let data_type = ch_to_arrow_type(&type_, None).unwrap().0;
    let error = TypedBuilder::try_new(&type_, &data_type).unwrap_err();
    assert!(error.to_string().contains("Duplicate enum code"));
}

#[tokio::test]
async fn changed_enum_labels_or_codes_cannot_reuse_a_stale_dictionary() {
    let original = Type::Enum8(vec![("a".into(), 1), ("b".into(), 2)]);
    let data_type = ch_to_arrow_type(&original, None).unwrap().0;
    for changed in [
        Type::Enum8(vec![("changed".into(), 1), ("b".into(), 2)]),
        Type::Enum8(vec![("a".into(), 2), ("b".into(), 1)]),
        Type::Enum8(vec![("a".into(), 1)]),
    ] {
        let mut builder = TypedBuilder::try_new(&original, &data_type).unwrap();
        let error = deserialize_async(&changed, &mut builder, &mut Cursor::new(vec![1]), 1, &[]).await.unwrap_err();
        assert!(error.to_string().contains("definition changed"));
        let mut builder = TypedBuilder::try_new(&original, &data_type).unwrap();
        let error = changed.deserialize_arrow(&mut builder, &mut Cursor::new(vec![1]), &data_type, 1, &[], &mut Vec::new()).unwrap_err();
        assert!(error.to_string().contains("definition changed"));
    }
}
