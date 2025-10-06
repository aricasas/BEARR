use std::{error::Error, fmt::Display, io};

#[derive(Clone, Debug, PartialEq)]
pub enum DBError {
    OOM,
    MemTableFull,
    InvalidScanRange,
    IOError(String),
}

impl Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::OOM => write!(f, "out of memory"),
            DBError::MemTableFull => write!(f, "cannot insert new node, memtable is full"),
            DBError::InvalidScanRange => write!(f, "invalid scan range"),
            DBError::IOError(s) => write!(f, "(I/O) {s}"),
        }
    }
}

impl Error for DBError {}

impl From<io::Error> for DBError {
    fn from(value: io::Error) -> Self {
        Self::IOError(value.to_string())
    }
}

impl From<serde_json::Error> for DBError {
    fn from(value: serde_json::Error) -> Self {
        Self::IOError(value.to_string())
    }
}
