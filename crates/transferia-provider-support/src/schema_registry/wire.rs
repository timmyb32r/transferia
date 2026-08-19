const MAGIC_SCHEMA_ID: u8 = 0;

pub struct ConfluentEnvelope<'a> {
    pub schema_id: i32,
    pub payload: &'a [u8],
}

impl<'a> ConfluentEnvelope<'a> {
    pub fn decode(bytes: &'a [u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() >= 5,
            "Confluent wire message must contain a magic byte and 4-byte schema id"
        );
        anyhow::ensure!(
            bytes[0] == MAGIC_SCHEMA_ID,
            "unsupported Confluent wire magic byte {}; expected schema-id format 0",
            bytes[0]
        );
        let schema_id = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        anyhow::ensure!(schema_id >= 0, "Confluent schema id must be nonnegative");
        Ok(Self {
            schema_id,
            payload: &bytes[5..],
        })
    }

    pub fn encode(schema_id: i32, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(schema_id >= 0, "Confluent schema id must be nonnegative");
        let mut output = Vec::with_capacity(5 + payload.len());
        output.push(MAGIC_SCHEMA_ID);
        output.extend_from_slice(&schema_id.to_be_bytes());
        output.extend_from_slice(payload);
        Ok(output)
    }
}

pub fn decode_message_indexes(bytes: &[u8]) -> anyhow::Result<(Vec<i32>, &[u8])> {
    anyhow::ensure!(
        !bytes.is_empty(),
        "Confluent Protobuf message has no message-index array"
    );
    if bytes[0] == 0 {
        return Ok((vec![0], &bytes[1..]));
    }
    let (length, mut offset) = decode_zigzag_varint(bytes)?;
    anyhow::ensure!(length > 0, "Protobuf message-index array must not be empty");
    let length = usize::try_from(length)
        .map_err(|_| anyhow::anyhow!("Protobuf message-index count exceeds usize"))?;
    let mut indexes = Vec::with_capacity(length);
    for _ in 0..length {
        let (index, consumed) = decode_zigzag_varint(&bytes[offset..])?;
        anyhow::ensure!(index >= 0, "Protobuf message index must be nonnegative");
        indexes.push(index);
        offset = offset
            .checked_add(consumed)
            .ok_or_else(|| anyhow::anyhow!("Protobuf message-index offset overflow"))?;
    }
    Ok((indexes, &bytes[offset..]))
}

pub fn encode_message_indexes(indexes: &[i32], output: &mut Vec<u8>) -> anyhow::Result<()> {
    anyhow::ensure!(
        !indexes.is_empty(),
        "Protobuf message-index array must not be empty"
    );
    if indexes == [0] {
        output.push(0);
        return Ok(());
    }
    encode_zigzag_varint(i32::try_from(indexes.len())?, output);
    for index in indexes {
        anyhow::ensure!(*index >= 0, "Protobuf message index must be nonnegative");
        encode_zigzag_varint(*index, output);
    }
    Ok(())
}

fn decode_zigzag_varint(bytes: &[u8]) -> anyhow::Result<(i32, usize)> {
    let mut raw = 0_u32;
    for (position, byte) in bytes.iter().copied().take(5).enumerate() {
        raw |= u32::from(byte & 0x7f) << (position * 7);
        if byte & 0x80 == 0 {
            let value = (raw >> 1).cast_signed() ^ -(raw & 1).cast_signed();
            return Ok((value, position + 1));
        }
    }
    anyhow::bail!("invalid Confluent Protobuf zigzag varint")
}

fn encode_zigzag_varint(value: i32, output: &mut Vec<u8>) {
    let mut raw = ((value << 1) ^ (value >> 31)) as u32;
    loop {
        if raw < 0x80 {
            output.push(raw as u8);
            break;
        }
        output.push((raw as u8) | 0x80);
        raw >>= 7;
    }
}
