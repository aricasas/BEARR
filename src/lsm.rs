use std::ops::RangeInclusive;

use serde::{Deserialize, Serialize};

use crate::{
    DbError,
    file_system::{FileId, FileSystem},
    memtable::MemTable,
    merge::{self, MergedIterator},
    sst::Sst,
};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct LsmConfiguration {
    pub size_ratio: usize,
    pub memtable_capacity: usize,
    /// The number of bits per entry for bloom filters at the topmost LSM level.
    pub bloom_filter_bits: usize,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct LsmMetadata {
    pub ssts_per_level: Vec<usize>,
    pub bottom_leveling: usize,
}

impl LsmMetadata {
    pub fn empty() -> Self {
        Self {
            ssts_per_level: Vec::new(),
            bottom_leveling: 0,
        }
    }
}

pub const TOMBSTONE: u64 = u64::MAX;

pub struct LsmTree {
    memtable: MemTable<u64, u64>,
    /// levels[0] is top level
    /// levels[0][0] is oldest sst in level 0
    levels: Vec<Vec<Sst>>,
    /// The number of original SSTs that the SST at the bottom level consists of.
    bottom_leveling: usize,
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
            bottom_leveling: metadata.bottom_leveling,
            configuration,
        })
    }

    pub fn get(&self, key: u64, file_system: &FileSystem) -> Result<Option<u64>, DbError> {
        let val = self.memtable.get(key);
        if let Some(value) = val {
            if value == TOMBSTONE {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // Search in order of level, then latest sst in level
        for level in &self.levels {
            for sst in level.iter().rev() {
                let val = sst.get(key, file_system)?;
                if let Some(value) = val {
                    if value == TOMBSTONE {
                        return Ok(None);
                    }
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    pub fn put(&mut self, key: u64, value: u64, file_system: &FileSystem) -> Result<(), DbError> {
        self.memtable.put(key, value);

        if self.memtable.size() >= self.configuration.memtable_capacity {
            self.flush_memtable(file_system)?;
        }

        Ok(())
    }

    pub fn delete(&mut self, key: u64, file_system: &FileSystem) -> Result<(), DbError> {
        self.put(key, TOMBSTONE, file_system)
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

        MergedIterator::new(scans, true)
    }

    fn bottom_level_number(&self) -> Option<usize> {
        self.levels.len().checked_sub(1)
    }

    /// Returns the maximum possible size of an SST at the given level.
    ///
    /// For the bottom level, this will be the maximum size of the entire level.
    fn sst_size_at_level(&self, level: usize) -> usize {
        assert!(
            level < self.levels.len(),
            "can only query the max SST size of a level that exists"
        );
        let p = self.configuration.memtable_capacity;
        let t = self.configuration.size_ratio;
        if level == self.bottom_level_number().unwrap() {
            p * t.pow(level as u32 + 1)
        } else {
            p * t.pow(level as u32)
        }
    }

    fn monkey(&self, level: usize) -> usize {
        let t = self.configuration.size_ratio as f64;
        let m_0 = self.configuration.bloom_filter_bits as f64;
        // TODO: explain in more detail
        // ep(M): false positive rate for M bits
        // T: size ratio, k: level, M_k: bits for kth level
        // ep(M) = 2^(-M ln 2)
        // ep(M_0) = ep(M_k) / T^k
        // 2^(-M_0 ln 2) = 2^(-M_k ln 2) / T^k
        //               = 2^(-M_k ln 2) / 2^(k log2(T))
        //               = 2^(-M_k ln 2 - k log2(T))
        //               = 2^(-M_k ln 2 - k (log2(T) / ln 2) ln 2) ==>
        //     -M_0 ln 2 = -M_k ln 2 - k (log2(T) / ln 2) ln 2
        //          -M_0 = -M_k - k log2(T) / ln 2
        //           M_0 =  M_k + k log2(T) / ln 2
        //           M_k =  M_0 - k log2(T) / ln 2
        (m_0 - (level as f64) * t.log2() / 2_f64.ln()) as usize
    }

    pub fn flush_memtable(&mut self, file_system: &FileSystem) -> Result<(), DbError> {
        if self.memtable.size() == 0 {
            return Ok(());
        }

        if self.levels.is_empty() {
            self.levels.push(Vec::new());
            self.bottom_leveling = 1;
        }

        let mem_table_size = self.memtable.size();
        let key_values = self.memtable.scan(u64::MIN..=u64::MAX)?;
        let file_id = FileId {
            lsm_level: 0,
            sst_number: self.levels[0].len(),
        };

        let sst = Sst::create(
            key_values.map(Ok),
            mem_table_size,
            self.monkey(0),
            file_id,
            file_system,
        )?;

        self.levels[0].push(sst);

        self.memtable.clear();

        self.merge_levels(file_system)?;

        Ok(())
    }

    /// Ensures that each level of the LSM tree does not have too many SSTs.
    fn merge_levels(&mut self, file_system: &FileSystem) -> Result<(), DbError> {
        // Don't have to do anything if there are no levels
        let Some(bottom_level_number) = self.bottom_level_number() else {
            return Ok(());
        };

        let t = self.configuration.size_ratio;

        for i in 0..bottom_level_number {
            let bits_per_entry = self.monkey(i + 1);

            let [level, level_below] = self.levels.get_disjoint_mut([i, i + 1]).unwrap();

            if level.len() < t {
                continue;
            }

            let mut scans = Vec::new();
            let mut n_entries_hint = 0;
            for sst in level.iter().rev() {
                let sst_scan = sst.scan(u64::MIN..=u64::MAX, file_system)?;
                scans.push(merge::Sources::BTree(sst_scan));
                n_entries_hint += sst.num_entries();
            }
            let key_values = MergedIterator::new(scans, false)?;

            let file_id = FileId {
                lsm_level: i + 1,
                sst_number: level_below.len(),
            };

            let sst = Sst::create(
                key_values,
                n_entries_hint,
                bits_per_entry,
                file_id,
                file_system,
            )?;
            level_below.push(sst);

            for sst in level.drain(..) {
                sst.destroy(file_system)?;
            }
        }

        let bottom_bits_per_entry = self.monkey(bottom_level_number);
        let bottom_level = &mut self.levels[bottom_level_number];
        debug_assert_ne!(bottom_level.len(), 0);
        if bottom_level.len() > 1 {
            self.bottom_leveling += bottom_level.len() - 1;

            let mut scans = Vec::new();
            let mut n_entries_hint = 0;
            for sst in bottom_level.iter().rev() {
                let sst_scan = sst.scan(u64::MIN..=u64::MAX, file_system)?;
                scans.push(merge::Sources::BTree(sst_scan));
                n_entries_hint += sst.num_entries();
            }
            let key_values = MergedIterator::new(scans, false)?;

            // Pick some file ID that doesn't exist, to avoid overwriting files that we're reading
            // Rename into the correct position after fully writing everything, if needed
            let file_id = FileId {
                lsm_level: bottom_level_number + 1,
                sst_number: 0,
            };

            let mut new_sst = Sst::create(
                key_values,
                n_entries_hint,
                bottom_bits_per_entry,
                file_id,
                file_system,
            )?;

            for sst in bottom_level.drain(..) {
                sst.destroy(file_system)?;
            }

            let new_file_id = FileId {
                lsm_level: bottom_level_number,
                sst_number: 0,
            };
            new_sst.rename(new_file_id, file_system)?;
            bottom_level.push(new_sst);
        }

        if self.bottom_leveling >= t {
            self.levels.push(Vec::new());

            let [former_bottom_level, new_bottom_level] = self
                .levels
                .get_disjoint_mut([bottom_level_number, bottom_level_number + 1])
                .unwrap();

            let mut sst = former_bottom_level.pop().unwrap();
            debug_assert_eq!(former_bottom_level.len(), 0);

            let new_file_id = FileId {
                lsm_level: bottom_level_number + 1,
                sst_number: 0,
            };
            sst.rename(new_file_id, file_system)?;
            new_bottom_level.push(sst);

            self.bottom_leveling = 1;
        }

        Ok(())
    }

    pub fn metadata(&self) -> LsmMetadata {
        LsmMetadata {
            ssts_per_level: self.levels.iter().map(|level| level.len()).collect(),
            bottom_leveling: self.bottom_leveling,
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::test_util::{TestFs, TestPath};

    use super::*;

    fn test_path(name: &str) -> TestPath {
        TestPath::new("lsm", name)
    }

    fn empty_lsm(prefix: &TestPath) -> Result<LsmTree> {
        let prefix = &test_path("monkey");
        let fs = FileSystem::new(prefix, 16, 1)?;
        let lsm = LsmTree::open(
            LsmMetadata::empty(),
            LsmConfiguration {
                size_ratio: 3,
                memtable_capacity: 4,
                bloom_filter_bits: 4,
            },
            &fs,
        )?;
        Ok(lsm)
    }

    #[test]
    fn test_monkey() -> Result<()> {
        let prefix = &test_path("monkey");
        let lsm = empty_lsm(prefix);
        Ok(())
    }
}
