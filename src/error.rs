use std::{error::Error, fmt::Display};

#[derive(Debug, PartialEq)]
pub enum DBError {
    OOM,
    InvalidScanRange,
}

impl Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::OOM => write!(f, "out of memory"),
            DBError::InvalidScanRange => write!(f, "invalid scan range"),
        }
    }
}
impl Error for DBError {}
