use std::{error::Error, fmt::Display};

#[derive(Debug, PartialEq)]
pub enum DBError {
    OOM,
    MemTableFull,
}

impl Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::OOM => write!(f, "out of memory"),
            DBError::MemTableFull => write!(f, "cannot insert new node, memtable is full"),
        }
    }
}
impl Error for DBError {}
