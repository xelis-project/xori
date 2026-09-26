use std::ops::Bound;

use bytes::Bytes;

/// Iterator direction for iterating over entries in a column
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IteratorDirection {
    Forward,
    Backward,
}

impl IteratorMode<'_> {
    /// Shared byte bounds keep backend and snapshot iteration consistent.
    /// Direction changes traversal order, not the set of keys in the range.
    pub(crate) fn bounds(self) -> (Bound<Bytes>, Bound<Bytes>, IteratorDirection) {
        use Bound::{Excluded, Included, Unbounded};

        match self {
            Self::All(direction) => (Unbounded, Unbounded, direction),
            Self::Prefix(prefix, direction) => {
                // Increment the last non-FF byte and truncate to obtain the
                // exclusive upper bound. Empty/all-FF prefixes have no upper bound.
                let mut end = prefix.to_vec();
                let upper = match end.iter().rposition(|&byte| byte != u8::MAX) {
                    Some(index) => {
                        end[index] += 1;
                        end.truncate(index + 1);
                        Excluded(Bytes::from(end))
                    }
                    None => Unbounded,
                };
                (Included(Bytes::copy_from_slice(prefix)), upper, direction)
            }
            Self::Range {
                start,
                end,
                direction,
            } => (
                Included(Bytes::copy_from_slice(start)),
                Excluded(Bytes::copy_from_slice(end)),
                direction,
            ),
            Self::From(start, direction) => (
                Included(Bytes::copy_from_slice(start)),
                Unbounded,
                direction,
            ),
        }
    }
}

/// Iterator mode for iterating over entries in a column
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IteratorMode<'a> {
    // iterate over all entries in a column
    // based on the direction, the order of entries will be determined by the backend
    All(IteratorDirection),
    // allow for prefix iteration only, where the iterator will yield all entries with keys that start with the given prefix
    Prefix(&'a [u8], IteratorDirection),
    Range {
        // key >= start
        start: &'a [u8],
        // key < end
        end: &'a [u8],
        direction: IteratorDirection,
    },
    // iterate over all entries with keys >= start
    From(&'a [u8], IteratorDirection),
}
