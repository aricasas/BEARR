use std::{collections, error::Error, fmt::Display, io};

#[derive(Clone, Debug, PartialEq)]
pub enum DbError {
    Oom,
    InvalidScanRange,
    IoError(String),
    InvalidConfiguration,
    CorruptSst,
}

impl Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Oom => write!(f, "out of memory"),
            DbError::InvalidScanRange => write!(f, "invalid scan range"),
            DbError::IoError(s) => write!(f, "(I/O) {s}"),
            DbError::InvalidConfiguration => write!(f, "invalid database configuration"),
            DbError::CorruptSst => write!(f, "Corrupt SST file"),
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
