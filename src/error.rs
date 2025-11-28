use std::{collections, error::Error, fmt::Display, io};

/// An error resulting from a database operation.
#[derive(Clone, Debug, PartialEq)]
pub enum DbError {
    /// An attempt to allocate memory failed,
    /// or the buffer pool is unable to evict a page and make space.
    Oom,
    /// Tried to perform a scan with a range where start > end.
    InvalidScanRange,
    /// An error involving I/O occurred.
    IoError(String),
    /// Tried to create a database with an invalid configuration.
    InvalidConfiguration,
    /// File corruption was detected for an SST.
    CorruptSst,
    /// Tried to insert a key-value pair where the value is `u64::MAX` (reserved for tombstones).
    InvalidValue,
}

impl Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Oom => write!(f, "out of memory"),
            DbError::InvalidScanRange => write!(f, "invalid scan range"),
            DbError::IoError(s) => write!(f, "(I/O) {s}"),
            DbError::InvalidConfiguration => write!(f, "invalid database configuration"),
            DbError::CorruptSst => write!(f, "Corrupt SST file"),
            DbError::InvalidValue => write!(f, "invalid value (cannot use u64::MAX)"),
        }
    }
}

impl Error for DbError {}

impl From<io::Error> for DbError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value.to_string())
    }
}

impl From<serde_json::Error> for DbError {
    fn from(value: serde_json::Error) -> Self {
        Self::IoError(value.to_string())
    }
}

impl From<collections::TryReserveError> for DbError {
    fn from(_: collections::TryReserveError) -> Self {
        Self::Oom
    }
}
