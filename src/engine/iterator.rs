/// Iterator direction for iterating over entries in a column
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IteratorDirection {
    Forward,
    Backward,
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