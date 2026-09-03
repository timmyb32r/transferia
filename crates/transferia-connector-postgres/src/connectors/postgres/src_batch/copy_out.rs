use std::pin::Pin;

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use tokio_postgres::CopyOutStream;

use crate::connectors::postgres::common::PostgresCopyFormat;
use crate::metrics::SourceCounters;
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};

const BINARY_MAGIC: &[u8] = b"PGCOPY\n\xFF\r\n\0";
const BINARY_FIXED_HEADER_BYTES: usize = 19;

#[derive(Debug)]
pub(super) struct RawCopyRow {
    pub(super) fields: Vec<Option<Bytes>>,
}

pub(super) struct CopyOutReader {
    stream: Pin<Box<CopyOutStream>>,

    decoder: CopyDecoder,

    stream_finished: bool,
}

impl CopyOutReader {
    pub(super) fn new(
        stream: CopyOutStream,
        format: PostgresCopyFormat,
        columns: usize,
    ) -> Self {
        Self {
            stream: Box::pin(stream),
            decoder: CopyDecoder::new(format, columns),
            stream_finished: false,
        }
    }

    pub(super) async fn next_row(
        &mut self,
        counters: &SourceCounters,
    ) -> DataPlaneResult<Option<RawCopyRow>> {
        loop {
            match self.decoder.next().map_err(DataPlaneFailure::fatal)? {
                DecodeState::Row(row) => return Ok(Some(row)),
                DecodeState::End if self.stream_finished => return Ok(None),
                DecodeState::End | DecodeState::NeedMore => {}
            }
            match self.stream.as_mut().next().await {
                Some(Ok(chunk)) => {
                    counters.add_network_decoded_bytes(chunk.len() as u64);
                    self.decoder
                        .push(&chunk)
                        .map_err(DataPlaneFailure::fatal)?;
                }
                Some(Err(error)) => return Err(DataPlaneFailure::retryable(error.into())),
                None => {
                    self.stream_finished = true;
                    self.decoder.finish().map_err(DataPlaneFailure::fatal)?;
                }
            }
        }
    }
}

pub(super) struct CopyDecoder {
    inner: Decoder,
}

impl CopyDecoder {
    pub(super) fn new(format: PostgresCopyFormat, columns: usize) -> Self {
        let inner = match format {
            PostgresCopyFormat::Binary => Decoder::Binary(BinaryDecoder::new(columns)),
            PostgresCopyFormat::Text => Decoder::Text(TextDecoder::new(columns)),
        };
        Self { inner }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        match &mut self.inner {
            Decoder::Binary(decoder) => decoder.push(bytes),
            Decoder::Text(decoder) => decoder.push(bytes),
        }
    }

    pub(super) fn next(&mut self) -> anyhow::Result<DecodeState> {
        match &mut self.inner {
            Decoder::Binary(decoder) => decoder.next(),
            Decoder::Text(decoder) => decoder.next(),
        }
    }

    pub(super) fn finish(&mut self) -> anyhow::Result<()> {
        match &mut self.inner {
            Decoder::Binary(decoder) => decoder.finish(),
            Decoder::Text(decoder) => decoder.finish(),
        }
    }
}

enum Decoder {
    Binary(BinaryDecoder),
    Text(TextDecoder),
}

pub(super) enum DecodeState {
    Row(RawCopyRow),
    NeedMore,
    End,
}

struct BinaryDecoder {
    columns: usize,

    buffer: BytesMut,

    header_parsed: bool,

    ended: bool,
}

impl BinaryDecoder {
    fn new(columns: usize) -> Self {
        Self {
            columns,
            buffer: BytesMut::new(),
            header_parsed: false,
            ended: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.ended || bytes.is_empty(),
            "PostgreSQL binary COPY sent bytes after its trailer"
        );
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    fn next(&mut self) -> anyhow::Result<DecodeState> {
        if self.ended {
            return Ok(DecodeState::End);
        }
        if !self.header_parsed {
            if self.buffer.len() < BINARY_FIXED_HEADER_BYTES {
                return Ok(DecodeState::NeedMore);
            }
            anyhow::ensure!(
                self.buffer.starts_with(BINARY_MAGIC),
                "PostgreSQL binary COPY returned an invalid signature"
            );
            let flags = read_i32(&self.buffer[BINARY_MAGIC.len()..BINARY_MAGIC.len() + 4]);
            anyhow::ensure!(
                flags == 0,
                "PostgreSQL binary COPY returned unsupported flags {flags:#x}"
            );
            let extension_length = read_i32(
                &self.buffer[BINARY_MAGIC.len() + 4..BINARY_FIXED_HEADER_BYTES],
            );
            anyhow::ensure!(
                extension_length >= 0,
                "PostgreSQL binary COPY returned a negative header extension length"
            );
            let header_length = BINARY_FIXED_HEADER_BYTES
                .checked_add(usize::try_from(extension_length)?)
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL binary COPY header overflow"))?;
            if self.buffer.len() < header_length {
                return Ok(DecodeState::NeedMore);
            }
            drop(self.buffer.split_to(header_length));
            self.header_parsed = true;
        }
        if self.buffer.len() < 2 {
            return Ok(DecodeState::NeedMore);
        }
        let field_count = read_i16(&self.buffer[..2]);
        if field_count == -1 {
            drop(self.buffer.split_to(2));
            anyhow::ensure!(
                self.buffer.is_empty(),
                "PostgreSQL binary COPY returned bytes after its trailer"
            );
            self.ended = true;
            return Ok(DecodeState::End);
        }
        anyhow::ensure!(
            field_count >= 0 && usize::try_from(field_count)? == self.columns,
            "PostgreSQL binary COPY row has {field_count} fields, expected {}",
            self.columns
        );
        let mut cursor = 2_usize;
        let mut ranges = Vec::with_capacity(self.columns);
        for _ in 0..self.columns {
            if self.buffer.len().saturating_sub(cursor) < 4 {
                return Ok(DecodeState::NeedMore);
            }
            let length = read_i32(&self.buffer[cursor..cursor + 4]);
            cursor += 4;
            if length == -1 {
                ranges.push(None);
                continue;
            }
            anyhow::ensure!(
                length >= 0,
                "PostgreSQL binary COPY field has invalid negative length {length}"
            );
            let end = cursor
                .checked_add(usize::try_from(length)?)
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL binary COPY row length overflow"))?;
            if self.buffer.len() < end {
                return Ok(DecodeState::NeedMore);
            }
            ranges.push(Some(cursor..end));
            cursor = end;
        }
        let row = self.buffer.split_to(cursor).freeze();
        Ok(DecodeState::Row(RawCopyRow {
            fields: ranges
                .into_iter()
                .map(|range| range.map(|range| row.slice(range)))
                .collect(),
        }))
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.ended && self.buffer.is_empty(),
            "PostgreSQL binary COPY ended before a complete trailer"
        );
        Ok(())
    }
}

struct TextDecoder {
    columns: usize,

    buffer: BytesMut,

    ended: bool,
}

impl TextDecoder {
    fn new(columns: usize) -> Self {
        Self {
            columns,
            buffer: BytesMut::new(),
            ended: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(!self.ended, "PostgreSQL text COPY continued after EOF");
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    fn next(&mut self) -> anyhow::Result<DecodeState> {
        if self.ended {
            return Ok(DecodeState::End);
        }
        let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') else {
            return Ok(DecodeState::NeedMore);
        };
        let mut line = self.buffer.split_to(newline + 1).freeze();
        line.truncate(newline);
        let fields = split_text_row(line)?;
        anyhow::ensure!(
            fields.len() == self.columns,
            "PostgreSQL text COPY row has {} fields, expected {}",
            fields.len(),
            self.columns
        );
        Ok(DecodeState::Row(RawCopyRow { fields }))
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.buffer.is_empty(),
            "PostgreSQL text COPY ended in the middle of a row"
        );
        self.ended = true;
        Ok(())
    }
}

fn split_text_row(line: Bytes) -> anyhow::Result<Vec<Option<Bytes>>> {
    let mut fields = Vec::new();
    let mut start = 0_usize;
    for end in line
        .iter()
        .enumerate()
        .filter_map(|(index, byte)| (*byte == b'\t').then_some(index))
        .chain(std::iter::once(line.len()))
    {
        let raw = line.slice(start..end);
        fields.push(if raw.as_ref() == b"\\N" {
            None
        } else {
            Some(unescape_text_field(raw)?)
        });
        start = end.saturating_add(1);
    }
    Ok(fields)
}

fn unescape_text_field(raw: Bytes) -> anyhow::Result<Bytes> {
    if !raw.contains(&b'\\') {
        return Ok(raw);
    }
    let mut output = Vec::with_capacity(raw.len());
    let mut cursor = 0_usize;
    while cursor < raw.len() {
        if raw[cursor] != b'\\' {
            output.push(raw[cursor]);
            cursor += 1;
            continue;
        }
        cursor += 1;
        anyhow::ensure!(
            cursor < raw.len(),
            "PostgreSQL text COPY field ends with an incomplete escape"
        );
        let escaped = raw[cursor];
        cursor += 1;
        match escaped {
            b'b' => output.push(0x08),
            b'f' => output.push(0x0c),
            b'n' => output.push(b'\n'),
            b'r' => output.push(b'\r'),
            b't' => output.push(b'\t'),
            b'v' => output.push(0x0b),
            b'x' => {
                let (value, consumed) = escaped_integer(&raw[cursor..], 16, 2)?;
                anyhow::ensure!(consumed > 0, "PostgreSQL text COPY has an empty hex escape");
                output.push(value);
                cursor += consumed;
            }
            b'0'..=b'7' => {
                let mut digits = vec![escaped];
                while digits.len() < 3
                    && cursor < raw.len()
                    && matches!(raw[cursor], b'0'..=b'7')
                {
                    digits.push(raw[cursor]);
                    cursor += 1;
                }
                let (value, consumed) = escaped_integer(&digits, 8, 3)?;
                anyhow::ensure!(consumed == digits.len(), "invalid PostgreSQL octal escape");
                output.push(value);
            }
            other => output.push(other),
        }
    }
    Ok(Bytes::from(output))
}

fn escaped_integer(bytes: &[u8], radix: u32, maximum_digits: usize) -> anyhow::Result<(u8, usize)> {
    let mut value = 0_u16;
    let mut consumed = 0_usize;
    for byte in bytes.iter().copied().take(maximum_digits) {
        let Some(digit) = (byte as char).to_digit(radix) else {
            break;
        };
        value = value
            .checked_mul(u16::try_from(radix)?)
            .and_then(|value| value.checked_add(u16::try_from(digit).ok()?))
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL text COPY escape overflow"))?;
        consumed += 1;
    }
    Ok((u8::try_from(value)?, consumed))
}

fn read_i16(bytes: &[u8]) -> i16 {
    i16::from_be_bytes([bytes[0], bytes[1]])
}

fn read_i32(bytes: &[u8]) -> i32 {
    i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}
