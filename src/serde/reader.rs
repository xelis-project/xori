use std::{
    borrow::Cow,
    fmt::Display,
    io::{ErrorKind, Read},
};

use matriochka::Error as ContextError;
use thiserror::Error;

/// Errors that can occur while reading from a byte slice
#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("Unexpected value")]
    UnexpectedValue,
    #[error("Data is not serializable")]
    NotSerializable,
    #[error("Requested {requested} bytes but only {available} available")]
    OutOfBounds { requested: usize, available: usize },
    #[error("Failed to convert bytes")]
    ErrorTryInto,
    #[error(transparent)]
    Any(#[from] ContextError),
}

impl ReaderError {
    /// Add diagnostic context while preserving the concrete reader error.
    pub fn context(self, context: impl Display + Send + Sync + 'static) -> Self {
        let error = match self {
            Self::Any(error) => error,
            error => ContextError::new(error),
        };
        Self::Any(error.context(context))
    }
}

/// A fallible source of serialized bytes. Return zero only at end of input.
/// Successful reads must return at most the size of the supplied buffer.
pub trait Readable {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, ReaderError>;

    /// Known remaining length, if available without reading from the source.
    fn remaining(&self) -> Option<usize> {
        None
    }
}

impl<R: Read> Readable for R {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, ReaderError> {
        loop {
            match Read::read(self, buffer) {
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                result => {
                    return result.map_err(|error| ReaderError::from(ContextError::new(error)));
                }
            }
        }
    }
}

/// Byte slice or owned buffer source, retaining borrowed access to unread bytes.
pub struct SliceSource<'a> {
    data: Cow<'a, [u8]>,
    offset: usize,
}

impl Readable for SliceSource<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, ReaderError> {
        let count = buffer.len().min(self.data.len() - self.offset);
        buffer[..count].copy_from_slice(&self.data[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }

    fn remaining(&self) -> Option<usize> {
        Some(self.data.len() - self.offset)
    }
}

/// Reader for deserializing from any fallible byte source.
pub struct Reader<R> {
    source: R,
    total: usize,
}

impl<R: Readable> Reader<R> {
    pub fn from_source(source: R) -> Self {
        Self { source, total: 0 }
    }

    pub fn into_inner(self) -> R {
        self.source
    }

    pub fn total_read(&self) -> usize {
        self.total
    }

    /// Fill a caller-provided buffer without a temporary allocation.
    /// On a partial failure, total_read includes bytes already consumed.
    pub fn read_exact(&mut self, buffer: &mut [u8]) -> Result<(), ReaderError> {
        if let Some(available) = self.source.remaining() {
            if buffer.len() > available {
                return Err(ReaderError::OutOfBounds {
                    requested: buffer.len(),
                    available,
                });
            }
        }

        let mut offset = 0;
        while offset < buffer.len() {
            let count = self
                .source
                .read(&mut buffer[offset..])
                .map_err(|error| error.context(format!("reading source at byte {}", self.total)))?;
            if count > buffer.len() - offset {
                return Err(ReaderError::UnexpectedValue);
            }
            if count == 0 {
                return Err(ReaderError::OutOfBounds {
                    requested: buffer.len(),
                    available: offset,
                });
            }
            offset += count;
            self.total += count;
        }
        Ok(())
    }

    pub fn next_byte(&mut self) -> Result<u8, ReaderError> {
        let mut byte = [0];
        self.read_exact(&mut byte)?;
        Ok(byte[0])
    }

    /// Read into the final owned buffer, growing only as input arrives.
    pub fn read_vec(&mut self, n: usize) -> Result<Vec<u8>, ReaderError> {
        if let Some(available) = self.source.remaining() {
            if n > available {
                return Err(ReaderError::OutOfBounds {
                    requested: n,
                    available,
                });
            }
        }

        let mut bytes = Vec::with_capacity(n.min(4096));
        while bytes.len() < n {
            let offset = bytes.len();
            let count = (n - offset).min(4096);
            bytes.resize(offset + count, 0);
            self.read_exact(&mut bytes[offset..])?;
        }
        Ok(bytes)
    }

    pub fn read_bytes<T>(&mut self, n: usize) -> Result<T, ReaderError>
    where
        T: for<'b> TryFrom<&'b [u8]>,
    {
        let bytes = self.read_vec(n)?;
        bytes
            .as_slice()
            .try_into()
            .map_err(|_| ReaderError::ErrorTryInto.context(format!("trying to convert {n} bytes")))
    }

    /// Read an unframed value until EOF. Use a bounded source for embedded values.
    pub fn read_remaining_bytes(&mut self) -> Result<Vec<u8>, ReaderError> {
        let mut bytes = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            let count = self
                .source
                .read(&mut buffer)
                .map_err(|error| error.context(format!("reading source at byte {}", self.total)))?;
            if count > buffer.len() {
                return Err(ReaderError::UnexpectedValue);
            }
            if count == 0 {
                break;
            }
            self.total += count;
            bytes.extend_from_slice(&buffer[..count]);
        }
        if bytes.is_empty() {
            return Err(ReaderError::OutOfBounds {
                requested: 1,
                available: 0,
            });
        }
        Ok(bytes)
    }
}

impl<'a> Reader<SliceSource<'a>> {
    /// Create a reader from a byte slice or owned byte vector.
    pub fn new(data: impl Into<Cow<'a, [u8]>>) -> Self {
        Self::from_source(SliceSource {
            data: data.into(),
            offset: 0,
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.source.data
    }

    pub fn read_bytes_left(&mut self) -> &[u8] {
        let offset = self.source.offset;
        self.total += self.remaining();
        self.source.offset = self.source.data.len();
        &self.source.data[offset..]
    }

    pub fn has_more(&self) -> bool {
        self.remaining() != 0
    }

    pub fn remaining(&self) -> usize {
        self.source.data.len() - self.source.offset
    }

    /// Borrow bytes directly from a slice source without copying.
    pub fn read_bytes_ref(&mut self, n: usize) -> Result<&[u8], ReaderError> {
        if n > self.remaining() {
            return Err(ReaderError::OutOfBounds {
                requested: n,
                available: self.remaining(),
            });
        }
        let offset = self.source.offset;
        self.source.offset += n;
        self.total += n;
        Ok(&self.source.data[offset..offset + n])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Serializable, SerializedBytes};

    #[test]
    fn deserialization_errors_preserve_type_and_offset_context() {
        // Two u16 values are declared, but only the first is present.
        let error = Vec::<u16>::from_bytes([2, 0, 1]).unwrap_err();
        let ReaderError::Any(error) = error else {
            panic!("expected contextual reader error");
        };

        assert!(matches!(
            error.downcast_ref::<ReaderError>(),
            Some(ReaderError::OutOfBounds {
                requested: 2,
                available: 0
            })
        ));
        let diagnostic = format!("{error:#}");
        assert!(diagnostic.contains("deserializing alloc::vec::Vec<u16> at byte 3"));
        assert!(diagnostic.contains("Requested 2 bytes but only 0 available"));
    }

    #[test]
    fn custom_reader_errors_retain_their_cause_and_context() {
        #[derive(Debug, thiserror::Error)]
        #[error("invalid record")]
        struct InvalidRecord;

        let error =
            ReaderError::from(ContextError::new(InvalidRecord)).context("reading account metadata");
        let ReaderError::Any(error) = error else {
            panic!("expected contextual reader error");
        };

        assert!(error.downcast_ref::<InvalidRecord>().is_some());
        assert_eq!(
            format!("{error:#}"),
            "reading account metadata: invalid record"
        );
    }

    struct FragmentedSource {
        bytes: std::io::Cursor<Vec<u8>>,
        interrupt: bool,
    }

    impl Read for FragmentedSource {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.interrupt {
                self.interrupt = false;
                return Err(std::io::Error::from(ErrorKind::Interrupted));
            }

            let count = buffer.len().min(1);
            Read::read(&mut self.bytes, &mut buffer[..count])
        }
    }

    #[test]
    fn streams_decode_nested_values_with_partial_reads() {
        let value = Some(vec!["first".to_owned(), "second".repeat(1000)]);
        let mut bytes = value.to_bytes().unwrap().into_vec();
        bytes.extend_from_slice(&42u64.to_be_bytes());
        let mut reader = Reader::from_source(FragmentedSource {
            bytes: std::io::Cursor::new(bytes),
            interrupt: true,
        });

        assert_eq!(Option::<Vec<String>>::read(&mut reader).unwrap(), value);
        assert_eq!(reader.total_read(), value.size());
        assert_eq!(u64::read(&mut reader).unwrap(), 42);
        assert_eq!(reader.total_read(), value.size() + 8);
    }

    #[test]
    fn truncated_stream_tracks_consumed_bytes() {
        let mut reader = Reader::from_source(std::io::Cursor::new([0, 1, 2]));
        let error = u64::read(&mut reader).unwrap_err();

        assert!(matches!(
            error,
            ReaderError::OutOfBounds {
                requested: 8,
                available: 3
            }
        ));
        assert_eq!(reader.total_read(), 3);
    }

    #[test]
    fn slice_helpers_keep_borrowed_access_and_consistent_offsets() {
        let bytes = [1, 2, 3, 4];
        let mut reader = Reader::new(bytes.as_slice());

        assert_eq!(reader.next_byte().unwrap(), 1);
        let borrowed = reader.read_bytes_ref(2).unwrap();
        assert_eq!(borrowed.as_ptr(), bytes[1..].as_ptr());
        assert_eq!(borrowed, &[2, 3]);
        assert_eq!(reader.total_read(), 3);
        assert_eq!(reader.remaining(), 1);
        assert_eq!(reader.read_bytes_left(), &[4]);
        assert_eq!(reader.total_read(), 4);
        assert!(!reader.has_more());
        assert_eq!(reader.bytes(), &bytes);
    }

    #[derive(Debug, thiserror::Error)]
    #[error("source unavailable")]
    struct SourceUnavailable;

    struct UnavailableSource;

    impl Readable for UnavailableSource {
        fn read(&mut self, _: &mut [u8]) -> Result<usize, ReaderError> {
            Err(ContextError::new(SourceUnavailable)
                .context("reading custom source")
                .into())
        }
    }

    #[test]
    fn custom_sources_preserve_non_io_errors() {
        let mut reader = Reader::from_source(UnavailableSource);
        let ReaderError::Any(error) = u64::read(&mut reader).unwrap_err() else {
            panic!("expected contextual source error");
        };

        assert!(error.downcast_ref::<SourceUnavailable>().is_some());
        assert_eq!(
            format!("{error:#}"),
            "reading source at byte 0: reading custom source: source unavailable"
        );
        assert_eq!(reader.total_read(), 0);
    }

    #[test]
    fn unframed_byte_values_read_until_source_eof() {
        let bytes = vec![7; 10_000];
        let mut reader = Reader::from_source(FragmentedSource {
            bytes: std::io::Cursor::new(bytes.clone()),
            interrupt: false,
        });

        assert_eq!(SerializedBytes::read(&mut reader).unwrap().as_ref(), bytes);
        assert_eq!(reader.total_read(), bytes.len());
    }
}
