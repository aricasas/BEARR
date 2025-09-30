use std::{
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use crate::{memtable::MemTable, sst::SST};

/// An open connection to a database
pub struct Database {
    configuration: DBConfiguration,
    name: PathBuf,
    memtable: MemTable<u64, u64>,
    ssts: Vec<SST>, // ?
}

/// Configuration options for a database
pub struct DBConfiguration {
    pub memtable_size: usize,
}

impl Database {
    pub fn create(name: &Path, configuration: DBConfiguration) -> Result<Self, ()> {
        todo!()
    }

    pub fn open(name: &Path) -> Result<Self, ()> {
        todo!()
    }

    pub fn put(&mut self, key: u64, value: u64) -> Result<(), ()> {
        self.memtable.put(key, value);

        if self.memtable.size() < self.configuration.memtable_size {
            return Ok(());
        }

        // SST stuff
        todo!()
    }

    pub fn get(&self, key: u64) -> Result<Option<u64>, ()> {
        let val = self.memtable.get(key);

        if val.is_some() {
            return Ok(val);
        }

        // Search in SSTs
        todo!()
    }

    pub fn scan(&self, range: RangeInclusive<u64>) -> Result<Vec<(u64, u64)>, ()> {
        todo!()
    }

    pub fn close(self) -> Result<(), ()> {
        todo!()
    }
}
