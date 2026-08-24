use arrow::datatypes::DataType;
use bytes::{Buf as _, Bytes, BytesMut};
use serde_json::{Value, json};
use transferia_core::data::schema::DatasetSchema;

use super::config::YTsaurusReadFormat;

pub(super) fn output_format(
    format: YTsaurusReadFormat,
    schema: &DatasetSchema,
) -> anyhow::Result<String> {
    Ok(match format {
        YTsaurusReadFormat::Arrow => serde_json::to_string("arrow")?,
        YTsaurusReadFormat::Json => serde_json::to_string("json")?,
        YTsaurusReadFormat::YsonBinary => serde_json::to_string("yson")?,
        YTsaurusReadFormat::YsonText => serde_json::to_string(&json!({
            "$value": "yson",
            "$attributes": {"format": "text"},
        }))?,
        YTsaurusReadFormat::SchemafulDsv => schemaful_dsv_format(schema)?,
        YTsaurusReadFormat::Skiff => skiff_format(schema)?,
    })
}

fn schemaful_dsv_format(schema: &DatasetSchema) -> anyhow::Result<String> {
    let columns = schema
        .columns
        .iter()
        .map(|column| Value::String(column.name.clone()))
        .collect::<Vec<_>>();
    Ok(serde_json::to_string(&json!({
        "$value": "schemaful_dsv",
        "$attributes": {
            "columns": columns,
            "enable_escaping": true,
            "missing_value_mode": "print_sentinel",
            "missing_value_sentinel": "",
        },
    }))?)
}

#[derive(Clone, Copy)]
enum SkiffWireType {
    Fixed(usize, &'static str),
    String32,
}

impl SkiffWireType {
    fn from_arrow(data_type: &DataType) -> anyhow::Result<Self> {
        Ok(match data_type {
            DataType::Int8 => Self::Fixed(1, "int8"),
            DataType::Int16 => Self::Fixed(2, "int16"),
            DataType::Int32 => Self::Fixed(4, "int32"),
            DataType::Int64 => Self::Fixed(8, "int64"),
            DataType::UInt8 => Self::Fixed(1, "uint8"),
            DataType::UInt16 => Self::Fixed(2, "uint16"),
            DataType::UInt32 => Self::Fixed(4, "uint32"),
            DataType::UInt64 => Self::Fixed(8, "uint64"),
            DataType::Float32 | DataType::Float64 => Self::Fixed(8, "double"),
            DataType::Boolean => Self::Fixed(1, "boolean"),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                Self::String32
            }
            _ => anyhow::bail!(
                "YTsaurus Skiff benchmark discard does not support Arrow type {data_type:?}"
            ),
        })
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Fixed(_, name) => name,
            Self::String32 => "string32",
        }
    }
}

fn skiff_format(schema: &DatasetSchema) -> anyhow::Result<String> {
    let children = schema
        .columns
        .iter()
        .map(|column| {
            let wire_type = SkiffWireType::from_arrow(&column.data_type)?.name();
            if column.nullable {
                Ok(json!({
                    "name": &column.name,
                    "wire_type": "variant8",
                    "children": [
                        {"wire_type": "nothing"},
                        {"wire_type": wire_type},
                    ],
                }))
            } else {
                Ok(json!({"name": &column.name, "wire_type": wire_type}))
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(serde_json::to_string(&json!({
        "$value": "skiff",
        "$attributes": {
            "table_skiff_schemas": [{
                "wire_type": "tuple",
                "children": children,
            }],
        },
    }))?)
}

pub(super) enum DiscardDecoder {
    Arrow(ArrowRowCounter),
    Lines(LineRowCounter),
    Skiff(SkiffRowCounter),
    Yson(YsonRowCounter),
}

impl DiscardDecoder {
    pub(super) fn new(
        format: YTsaurusReadFormat,
        schema: &DatasetSchema,
    ) -> anyhow::Result<Self> {
        Ok(match format {
            YTsaurusReadFormat::Arrow => Self::Arrow(ArrowRowCounter::default()),
            YTsaurusReadFormat::Json | YTsaurusReadFormat::SchemafulDsv => {
                Self::Lines(LineRowCounter::default())
            }
            YTsaurusReadFormat::Skiff => Self::Skiff(SkiffRowCounter::new(schema)?),
            YTsaurusReadFormat::YsonBinary | YTsaurusReadFormat::YsonText => {
                Self::Yson(YsonRowCounter::default())
            }
        })
    }

    pub(super) fn decode(&mut self, bytes: Bytes) -> anyhow::Result<u64> {
        match self {
            Self::Arrow(decoder) => decoder.decode(bytes),
            Self::Lines(decoder) => Ok(decoder.decode(&bytes)),
            Self::Skiff(decoder) => decoder.decode(bytes),
            Self::Yson(decoder) => decoder.decode(&bytes),
        }
    }

    pub(super) fn finish(&mut self) -> anyhow::Result<u64> {
        match self {
            Self::Arrow(decoder) => decoder.finish(),
            Self::Lines(decoder) => Ok(decoder.finish()),
            Self::Skiff(decoder) => decoder.finish(),
            Self::Yson(decoder) => decoder.finish(),
        }
    }
}

#[derive(Default)]
pub(super) struct LineRowCounter {
    pending: bool,
}

impl LineRowCounter {
    fn decode(&mut self, bytes: &[u8]) -> u64 {
        if bytes.is_empty() {
            return 0;
        }
        let rows = bytes.iter().filter(|byte| **byte == b'\n').count() as u64;
        self.pending = bytes.last().is_some_and(|byte| *byte != b'\n');
        rows
    }

    fn finish(&mut self) -> u64 {
        u64::from(std::mem::take(&mut self.pending))
    }
}

pub(super) struct ArrowRowCounter {
    state: ArrowState,
    buffer: BytesMut,
}

impl Default for ArrowRowCounter {
    fn default() -> Self {
        Self {
            state: ArrowState::Header,
            buffer: BytesMut::new(),
        }
    }
}

#[derive(Clone, Copy, Default)]
enum ArrowState {
    #[default]
    Header,
    Metadata { size: usize },
    Body { remaining: usize, rows: u64 },
    Finished,
}

impl ArrowRowCounter {
    fn decode(&mut self, mut bytes: Bytes) -> anyhow::Result<u64> {
        let mut rows = 0_u64;
        loop {
            match self.state {
                ArrowState::Header => {
                    let prefix_needed = 4_usize.saturating_sub(self.buffer.len());
                    let copied = prefix_needed.min(bytes.len());
                    self.buffer.extend_from_slice(&bytes[..copied]);
                    bytes.advance(copied);
                    if self.buffer.len() < 4 {
                        break;
                    }
                    let header_size = if self.buffer[..4] == [0xff; 4] { 8 } else { 4 };
                    let header_needed = header_size - self.buffer.len();
                    let copied = header_needed.min(bytes.len());
                    self.buffer.extend_from_slice(&bytes[..copied]);
                    bytes.advance(copied);
                    if self.buffer.len() < header_size {
                        break;
                    }
                    let size = if header_size == 8 {
                        if self.buffer.len() < 8 {
                            break;
                        }
                        u32::from_le_bytes(self.buffer[4..8].try_into()?) as usize
                    } else {
                        u32::from_le_bytes(self.buffer[..4].try_into()?) as usize
                    };
                    self.buffer.clear();
                    self.state = if size == 0 {
                        ArrowState::Finished
                    } else {
                        ArrowState::Metadata { size }
                    };
                }
                ArrowState::Metadata { size } => {
                    let needed = size.saturating_sub(self.buffer.len());
                    let copied = needed.min(bytes.len());
                    self.buffer.extend_from_slice(&bytes[..copied]);
                    bytes.advance(copied);
                    if self.buffer.len() < size {
                        break;
                    }
                    let message = arrow::ipc::root_as_message(&self.buffer).map_err(|error| {
                        anyhow::anyhow!("invalid Arrow IPC message metadata: {error:?}")
                    })?;
                    let body_length = usize::try_from(message.bodyLength()).map_err(|_| {
                        anyhow::anyhow!("Arrow IPC message has a negative body length")
                    })?;
                    let message_rows = match message.header_type() {
                        arrow::ipc::MessageHeader::RecordBatch => {
                            let record_batch = message.header_as_record_batch().ok_or_else(|| {
                                anyhow::anyhow!("Arrow IPC record-batch metadata is missing")
                            })?;
                            u64::try_from(record_batch.length()).map_err(|_| {
                                anyhow::anyhow!("Arrow IPC record batch has a negative row count")
                            })?
                        }
                        _ => 0,
                    };
                    self.buffer.clear();
                    self.state = ArrowState::Body {
                        remaining: body_length,
                        rows: message_rows,
                    };
                }
                ArrowState::Body {
                    remaining,
                    rows: message_rows,
                } => {
                    // Record-batch bodies are irrelevant in discard mode. Skip
                    // them directly in the response Bytes without copying them
                    // into the framing buffer.
                    let consumed = remaining.min(bytes.len());
                    bytes.advance(consumed);
                    let remaining = remaining - consumed;
                    if remaining == 0 {
                        rows = rows.saturating_add(message_rows);
                        self.state = ArrowState::Header;
                    } else {
                        self.state = ArrowState::Body {
                            remaining,
                            rows: message_rows,
                        };
                        break;
                    }
                }
                ArrowState::Finished => {
                    anyhow::ensure!(
                        bytes.iter().all(|byte| *byte == 0),
                        "Arrow IPC stream contains data after its end marker"
                    );
                    bytes.clear();
                    break;
                }
            }
            if bytes.is_empty()
                && !matches!(self.state, ArrowState::Body { remaining: 0, .. })
            {
                break;
            }
        }
        Ok(rows)
    }

    fn finish(&self) -> anyhow::Result<u64> {
        anyhow::ensure!(
            matches!(self.state, ArrowState::Finished | ArrowState::Header),
            "Arrow IPC stream ended inside a message"
        );
        anyhow::ensure!(self.buffer.is_empty(), "Arrow IPC stream has trailing bytes");
        Ok(0)
    }
}

pub(super) struct SkiffRowCounter {
    fields: Vec<(bool, SkiffWireType)>,
    buffer: BytesMut,
}

impl SkiffRowCounter {
    fn new(schema: &DatasetSchema) -> anyhow::Result<Self> {
        Ok(Self {
            fields: schema
                .columns
                .iter()
                .map(|column| {
                    Ok((
                        column.nullable,
                        SkiffWireType::from_arrow(&column.data_type)?,
                    ))
                })
                .collect::<anyhow::Result<_>>()?,
            buffer: BytesMut::new(),
        })
    }

    fn decode(&mut self, bytes: Bytes) -> anyhow::Result<u64> {
        self.buffer.extend_from_slice(&bytes);
        let mut cursor = 0_usize;
        let mut rows = 0_u64;
        'rows: loop {
            let row_start = cursor;
            if self.buffer.len().saturating_sub(cursor) < 2 {
                break;
            }
            let table_index = u16::from_le_bytes(self.buffer[cursor..cursor + 2].try_into()?);
            anyhow::ensure!(
                table_index == 0,
                "Skiff row references unexpected table schema {table_index}"
            );
            cursor += 2;
            for (nullable, wire_type) in &self.fields {
                if *nullable {
                    let Some(tag) = self.buffer.get(cursor).copied() else {
                        cursor = row_start;
                        break 'rows;
                    };
                    cursor += 1;
                    match tag {
                        0 => continue,
                        1 => {}
                        _ => anyhow::bail!("Skiff optional value has invalid variant8 tag {tag}"),
                    }
                }
                match wire_type {
                    SkiffWireType::Fixed(size, _) => {
                        if self.buffer.len().saturating_sub(cursor) < *size {
                            cursor = row_start;
                            break 'rows;
                        }
                        cursor += size;
                    }
                    SkiffWireType::String32 => {
                        if self.buffer.len().saturating_sub(cursor) < 4 {
                            cursor = row_start;
                            break 'rows;
                        }
                        let size = u32::from_le_bytes(
                            self.buffer[cursor..cursor + 4].try_into()?,
                        ) as usize;
                        if self.buffer.len().saturating_sub(cursor + 4) < size {
                            cursor = row_start;
                            break 'rows;
                        }
                        cursor += 4 + size;
                    }
                }
            }
            rows = rows.saturating_add(1);
        }
        self.buffer.advance(cursor);
        Ok(rows)
    }

    fn finish(&self) -> anyhow::Result<u64> {
        anyhow::ensure!(self.buffer.is_empty(), "Skiff stream ended inside a row");
        Ok(0)
    }
}

#[derive(Default)]
pub(super) struct YsonRowCounter {
    depth: usize,
    state: YsonState,
    row_open: bool,
}

#[derive(Default)]
enum YsonState {
    #[default]
    Normal,
    Quoted { escaped: bool },
    BinaryStringLength { value: u32, shift: u32 },
    BinaryPayload { remaining: usize },
    BinaryVarint,
    BinaryDouble { remaining: usize },
}

impl YsonRowCounter {
    fn decode(&mut self, bytes: &[u8]) -> anyhow::Result<u64> {
        let mut rows = 0_u64;
        for byte in bytes {
            match &mut self.state {
                YsonState::Quoted { escaped } => {
                    if *escaped {
                        *escaped = false;
                    } else if *byte == b'\\' {
                        *escaped = true;
                    } else if *byte == b'"' {
                        self.state = YsonState::Normal;
                    }
                }
                YsonState::BinaryStringLength { value, shift } => {
                    anyhow::ensure!(*shift < 35, "YSON binary string length varint is too long");
                    *value |= u32::from(*byte & 0x7f) << *shift;
                    if *byte & 0x80 == 0 {
                        anyhow::ensure!(
                            *value & 1 == 0,
                            "YSON binary string has a negative length"
                        );
                        let length = (*value >> 1) as usize;
                        self.state = if length == 0 {
                            YsonState::Normal
                        } else {
                            YsonState::BinaryPayload { remaining: length }
                        };
                    } else {
                        *shift += 7;
                    }
                }
                YsonState::BinaryPayload { remaining } | YsonState::BinaryDouble { remaining } => {
                    *remaining -= 1;
                    if *remaining == 0 {
                        self.state = YsonState::Normal;
                    }
                }
                YsonState::BinaryVarint => {
                    if *byte & 0x80 == 0 {
                        self.state = YsonState::Normal;
                    }
                }
                YsonState::Normal => match *byte {
                    b'"' => {
                        self.row_open |= self.depth == 0;
                        self.state = YsonState::Quoted { escaped: false };
                    }
                    0x01 => {
                        self.row_open |= self.depth == 0;
                        self.state = YsonState::BinaryStringLength { value: 0, shift: 0 };
                    }
                    0x02 | 0x06 => {
                        self.row_open |= self.depth == 0;
                        self.state = YsonState::BinaryVarint;
                    }
                    0x03 => {
                        self.row_open |= self.depth == 0;
                        self.state = YsonState::BinaryDouble { remaining: 8 };
                    }
                    b'{' | b'[' | b'<' => {
                        self.row_open |= self.depth == 0;
                        self.depth += 1;
                    }
                    b'}' | b']' | b'>' => {
                        anyhow::ensure!(self.depth > 0, "YSON stream has an unmatched closing token");
                        self.depth -= 1;
                    }
                    b';' if self.depth == 0 => {
                        anyhow::ensure!(self.row_open, "YSON stream has an empty top-level item");
                        self.row_open = false;
                        rows = rows.saturating_add(1);
                    }
                    byte if !byte.is_ascii_whitespace() => {
                        self.row_open |= self.depth == 0;
                    }
                    _ => {}
                },
            }
        }
        Ok(rows)
    }

    fn finish(&mut self) -> anyhow::Result<u64> {
        anyhow::ensure!(self.depth == 0, "YSON stream ended inside a container");
        anyhow::ensure!(
            matches!(self.state, YsonState::Normal),
            "YSON stream ended inside a scalar"
        );
        Ok(u64::from(std::mem::take(&mut self.row_open)))
    }
}
