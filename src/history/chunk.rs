use crate::{Readable, Reader, ReaderError, Serializable, VarUint, Writable, WriterError};
use std::iter::{FusedIterator, Peekable, Take};

use super::HistoryEntry;

/// A stored history chunk whose entries are decoded only as they are requested.
/// Owns the backend's raw buffer without copying it or allocating an entry list.
/// Dropping a partially consumed chunk skips validation of its unread entries.
#[derive(Debug)]
pub struct HistoryChunk<R = bytes::Bytes> {
    bytes: R,
    offset: usize,
    remaining: u64,
    last: bool,
}

impl<R: AsRef<[u8]>> HistoryChunk<R> {
    pub(super) fn new(bytes: R) -> Result<Self, ReaderError> {
        let mut reader = Reader::new(bytes.as_ref());
        let last = bool::read(&mut reader)?;
        let remaining = VarUint::read(&mut reader)?.0;
        // Every entry has at least a column, key length, and version byte.
        // Reject impossible counts without allocating from untrusted framing.
        if remaining > (reader.remaining() / 3) as u64
            || (remaining == 0 && (reader.has_more() || !last))
        {
            return Err(ReaderError::UnexpectedValue);
        }
        let offset = reader.total_read();
        Ok(Self {
            bytes,
            offset,
            remaining,
            last,
        })
    }

    /// Whether this is the final chunk for its history key.
    pub fn is_last(&self) -> bool {
        self.last
    }
}

impl<R: AsRef<[u8]>> Iterator for HistoryChunk<R> {
    type Item = Result<HistoryEntry, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut reader = Reader::new(&self.bytes.as_ref()[self.offset..]);
        let entry = HistoryEntry::read(&mut reader).and_then(|entry| {
            if self.remaining == 1 && reader.has_more() {
                Err(ReaderError::UnexpectedValue)
            } else {
                Ok(entry)
            }
        });
        if entry.is_ok() {
            self.offset += reader.total_read();
            self.remaining -= 1;
        } else {
            // Report a malformed entry once and terminate the iterator.
            self.remaining = 0;
        }
        Some(entry)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // A decoding error can terminate before the advertised entry count.
        (
            usize::from(self.remaining != 0),
            usize::try_from(self.remaining).ok(),
        )
    }
}

impl<R: AsRef<[u8]>> FusedIterator for HistoryChunk<R> {}

// Escape zero bytes and terminate the user key. This is prefix-free while
// preserving its byte ordering, including for variable-length keys.
pub(super) fn prefix(key: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(key.len() + 2);
    for byte in key {
        bytes.push(*byte);
        if *byte == 0 {
            bytes.push(255);
        }
    }
    bytes.extend_from_slice(&[0, 0]);
    bytes
}

pub(super) fn chunk_key(prefix: &[u8], index: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(prefix.len() + 8);
    bytes.extend_from_slice(prefix);
    bytes.extend_from_slice(&index.to_be_bytes());
    bytes
}

pub(super) fn decode_chunk_key(bytes: &[u8]) -> Result<(Vec<u8>, u64), ReaderError> {
    let mut reader = Reader::new(bytes);
    let mut key = Vec::new();
    loop {
        match reader.next_byte()? {
            0 => match reader.next_byte()? {
                0 => break,
                255 => key.push(0),
                _ => return Err(ReaderError::UnexpectedValue),
            },
            byte => key.push(byte),
        }
    }
    let index = u64::read(&mut reader)?;
    if reader.has_more() {
        return Err(ReaderError::UnexpectedValue);
    }
    Ok((key, index))
}

/// A bounded view of an iterator, without an intermediate vector of references.
pub(super) struct PackedChunk<I: Iterator> {
    entries: Take<Peekable<I>>,
    last: bool,
    count: usize,
    size: usize,
}

impl<I> Serializable for PackedChunk<I>
where
    I: Iterator + Clone,
    I::Item: Serializable + Clone,
{
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.last.write(writer)?;
        VarUint(self.count as u64).write(writer)?;
        for entry in self.entries.clone() {
            entry.write(writer)?;
        }
        Ok(())
    }

    fn read<R: Readable>(_: &mut Reader<R>) -> Result<Self, ReaderError> {
        Err(ReaderError::NotSerializable)
    }

    fn size(&self) -> usize {
        self.size
    }
}

/// Inspect and serialize only the requested chunk. Individual entries have
/// already been checked against the limit when staged. An empty batch still
/// writes a last chunk to distinguish it from a missing history key.
pub(super) fn pack<I>(entries: I, limit: usize) -> impl Iterator<Item = PackedChunk<I>>
where
    I: Iterator + Clone,
    I::Item: Serializable + Clone,
{
    let mut entries = entries.peekable();
    let mut finished = false;
    std::iter::from_fn(move || {
        if finished {
            return None;
        }
        let start = entries.clone();
        let mut count = 0;
        let mut payload_size = 0;
        while let Some(entry) = entries.next_if(|entry| {
            count == 0
                || 1 + VarUint::encoded_size(count + 1) + payload_size + entry.size() <= limit
        }) {
            payload_size += entry.size();
            count += 1;
        }
        finished = entries.peek().is_none();
        Some(PackedChunk {
            last: finished,
            entries: start.take(count),
            count,
            size: 1 + VarUint::encoded_size(count) + payload_size,
        })
    })
}
