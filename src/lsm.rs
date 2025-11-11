use std::ops::RangeInclusive;

use serde::{Deserialize, Serialize};

use crate::{
    DbError,
    file_system::{FileId, FileSystem},
    memtable::MemTable,
    merge::{self, MergedIterator},
    sst::Sst,
};

#[derive(Serialize, Deserialize)]
pub struct LsmConfiguration {
    pub size_ratio: usize,
    pub memtable_capacity: usize,
}

impl LsmConfiguration {
    pub fn validate(&self) -> Result<(), DbError> {
        if self.memtable_capacity > 0 && self.size_ratio >= 2 {
            Ok(())
        } else {
            Err(DbError::InvalidConfiguration)
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct LsmMetadata {
    pub ssts_per_level: Vec<usize>,
}

impl LsmMetadata {
    pub fn empty() -> Self {
        Self {
            ssts_per_level: Vec::new(),
        }
    }
}

pub const TOMBSTONE: u64 = u64::MAX;

pub struct LsmTree {
    memtable: MemTable<u64, u64>,
    /// levels[0] is top level
    /// levels[0][0] is oldest sst in level 0
    levels: Vec<Vec<Sst>>,
    configuration: LsmConfiguration,
}

impl LsmTree {
    pub fn open(
        metadata: LsmMetadata,
        configuration: LsmConfiguration,
        file_system: &FileSystem,
    ) -> Result<Self, DbError> {
        let num_levels = metadata.ssts_per_level.len();
        let mut levels = Vec::with_capacity(num_levels);

        for lsm_level in 0..num_levels {
            let num_ssts = metadata.ssts_per_level[lsm_level];
            let mut level = Vec::with_capacity(num_ssts);

            for sst_number in 0..num_ssts {
                let sst = Sst::open(
                    FileId {
                        lsm_level,
                        sst_number,
                    },
                    file_system,
                )?;
                level.push(sst);
            }

            levels.push(level);
        }

        Ok(Self {
            memtable: MemTable::new(configuration.memtable_capacity)?,
            levels,
            configuration,
        })
    }

    pub fn get(&self, key: u64, file_system: &FileSystem) -> Result<Option<u64>, DbError> {
        let val = self.memtable.get(key);
        if val.is_some() {
            return Ok(val);
        }

        // Search in order of level, then latest sst in level
        for level in &self.levels {
            for sst in level.iter().rev() {
                let val = sst.get(key, file_system)?;
                if val.is_some() {
                    return Ok(val);
                }
            }
        }

        Ok(None)
    }

    pub fn put(
        &mut self,
        key: u64,
        value: u64,
        file_system: &mut FileSystem,
    ) -> Result<(), DbError> {
        self.memtable.put(key, value);

        if self.memtable.size() >= self.configuration.memtable_capacity {}

        todo!()
    }

    pub fn delete(&mut self, key: u64, file_system: &mut FileSystem) -> Result<(), DbError> {
        self.put(key, TOMBSTONE, file_system)?;
        todo!()
    }

    pub fn scan<'a, 'b: 'a>(
        &'a self,
        range: RangeInclusive<u64>,
        file_system: &'b FileSystem,
    ) -> Result<MergedIterator<merge::Sources<'a>>, DbError> {
        let mut scans = Vec::new();

        let memtable_scan = self.memtable.scan(range.clone())?;
        scans.push(merge::Sources::MemTable(memtable_scan));

        for level in &self.levels {
            for sst in level.iter().rev() {
                let sst_scan = sst.scan(range.clone(), file_system)?;
                scans.push(merge::Sources::BTree(sst_scan));
            }
        }

        MergedIterator::new(scans)
    }

    pub fn flush_memtable(&mut self, file_system: &mut FileSystem) -> Result<(), DbError> {
        if self.memtable.size() == 0 {
            return Ok(());
        }

        let key_values = self.memtable.scan(u64::MIN..=u64::MAX)?;
        let file_id = FileId {
            lsm_level: 0,
            sst_number: self.levels.first().map(|l| l.len()).unwrap_or(0),
        };

        let sst = Sst::create(key_values.map(Ok), file_id, file_system)?;

        self.memtable.clear();

        // TODO stuff with self.levels
        // and compaction
        todo!();
    }

    pub fn metadata(&self) -> LsmMetadata {
        LsmMetadata {
            ssts_per_level: self.levels.iter().map(|level| level.len()).collect(),
        }
    }
}
