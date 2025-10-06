#![allow(warnings)]

use std::{
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use crate::DBError;

/// A handle to an SST of a database
pub struct SST {
    filename: PathBuf,
}

impl SST {
    pub fn create(key_values: Vec<(u64, u64)>, path: &Path) -> Result<SST, DBError> {
        todo!()
    }

    pub fn open(path: &Path) -> Result<SST, DBError> {
        todo!()
    }

    pub fn get(&self, key: u64) -> Result<Option<u64>, DBError> {
        todo!()
    }

    pub fn scan(&self, range: RangeInclusive<u64>) -> Result<SSTIter, DBError> {
        todo!()
    }
}

pub struct SSTIter {}
impl Iterator for SSTIter {
    type Item = Result<(u64, u64), DBError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
