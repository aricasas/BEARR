use std::ops::RangeInclusive;

use serde::{Deserialize, Serialize};

use crate::{
    DbError,
    file_system::{FileId, FileSystem},
    memtable::MemTable,
    merge::{self, MergedIterator},
    sst::Sst,
};

/// Configuration options for an LSM tree.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct LsmConfiguration {
    /// The size ratio of the LSM tree.
    /// Must be at least 2.
    pub size_ratio: usize,
    /// The number of key-value pairs that the memtable can hold.
    /// Must be nonzero.
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

/// Metadata for an LSM tree, persisted separately from the actual data.
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

/// An LSM tree, consisting of a memtable and several levels of SSTs.
///
/// Makes use of Monkey for assigning bloom filter bits
/// (unless the `uniform_bits` feature is enabled)
/// and Dostoevsky for compaction.
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
    /// Opens an LSM tree in the given file system,
    /// opening all of its component SSTs based on the given metadata
    /// and storing the given configuration and bottom leveling.
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

    // Returns whether an SST flush happened
    pub fn put(&mut self, key: u64, value: u64, file_system: &FileSystem) -> Result<bool, DbError> {
        self.memtable.put(key, value);

        if self.memtable.size() >= self.configuration.memtable_capacity {
            self.flush_memtable(file_system)?;
            return Ok(true);
        }

        Ok(false)
    }

    // Returns whether an SST flush happened
    pub fn delete(&mut self, key: u64, file_system: &FileSystem) -> Result<bool, DbError> {
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

    /// The index of the bottom level, or None if there are no levels.
    fn bottom_level_number(&self) -> Option<usize> {
        self.levels.len().checked_sub(1)
    }

    #[cfg(feature = "uniform_bits")]
    fn monkey(&self, _level: usize) -> usize {
        self.configuration.bloom_filter_bits
    }

    /// Returns the number of bits per entry for a bloom filter at the given level according to Monkey.
    #[cfg(not(feature = "uniform_bits"))]
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
        f64::max(m_0 - (level as f64) * t.log2() / 2_f64.ln(), 0.0).ceil() as usize
    }

    /// Flushes the memtable into an SST, and merges SSTs as necessary
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

        // Merge non-bottom levels
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
                scans.push(sst_scan);
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

        // Merge bottom level
        let bottom_bits_per_entry = self.monkey(bottom_level_number);
        let bottom_level = &mut self.levels[bottom_level_number];
        debug_assert_ne!(bottom_level.len(), 0);
        if bottom_level.len() > 1 {
            self.bottom_leveling += bottom_level.len() - 1;

            let mut scans = Vec::new();
            let mut n_entries_hint = 0;
            for sst in bottom_level.iter().rev() {
                let sst_scan = sst.scan(u64::MIN..=u64::MAX, file_system)?;
                scans.push(sst_scan);
                n_entries_hint += sst.num_entries();
            }
            let key_values = MergedIterator::new(scans, true)?;

            // Pick some file ID that doesn't exist, to avoid overwriting files that we're reading
            // Rename into the correct position after fully writing everything, if needed
            let file_id = FileId {
                lsm_level: bottom_level_number + 1,
                sst_number: 0,
            };

            let new_sst = Sst::create(
                key_values,
                n_entries_hint,
                bottom_bits_per_entry,
                file_id,
                file_system,
            )?;

            // Hacky workaround: if the bottom level initially entirely of tombstones,
            // then merging while deleting tombstones will cause it to be empty,
            // which works poorly with the rest of the codebase.
            // Have it instead consist of a single tombstone.
            let mut new_sst = if new_sst.num_entries() == 0 {
                new_sst.destroy(file_system)?;
                Sst::create(
                    [Ok((0, TOMBSTONE))],
                    1,
                    bottom_bits_per_entry,
                    file_id,
                    file_system,
                )?
            } else {
                new_sst
            };

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

        // Create a new bottom level if needed
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

    /// Metadata for the LSM tree calculated from its fields.
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

    use crate::test_util::TestFs;

    use super::*;

    fn test_fs(name: &str) -> TestFs {
        TestFs::create("lsm", name)
    }

    fn empty_lsm(fs: &TestFs) -> Result<LsmTree> {
        let lsm = LsmTree::open(
            LsmMetadata::empty(),
            LsmConfiguration {
                size_ratio: 3,
                memtable_capacity: 6,
                bloom_filter_bits: 5,
            },
            fs,
        )?;
        Ok(lsm)
    }

    fn assert_state(
        lsm: &LsmTree,
        expected_sst_sizes: &[&[usize]],
        expected_bottom_leveling: usize,
    ) {
        let expected_sst_sizes: Vec<Vec<usize>> = expected_sst_sizes
            .iter()
            .map(|level| level.to_vec())
            .collect();
        let actual_sst_sizes: Vec<Vec<usize>> = lsm
            .levels
            .iter()
            .map(|level| level.iter().map(|sst| sst.num_entries()).collect())
            .collect();
        assert_eq!(actual_sst_sizes, expected_sst_sizes);
        assert_eq!(lsm.bottom_leveling, expected_bottom_leveling);
    }

    fn put_and_assert(
        lsm: &mut LsmTree,
        fs: &TestFs,
        key: u64,
        value: u64,
        expected_sst_sizes: &[&[usize]],
        expected_bottom_leveling: usize,
    ) -> Result<()> {
        lsm.put(key, value, fs)?;
        assert_state(lsm, expected_sst_sizes, expected_bottom_leveling);
        Ok(())
    }

    fn delete_and_assert(
        lsm: &mut LsmTree,
        fs: &TestFs,
        key: u64,
        expected_sst_sizes: &[&[usize]],
        expected_bottom_leveling: usize,
    ) -> Result<()> {
        lsm.delete(key, fs)?;
        assert_state(lsm, expected_sst_sizes, expected_bottom_leveling);
        Ok(())
    }

    #[test]
    fn test_basic() -> Result<()> {
        let fs = &test_fs("basic");
        let lsm = &mut empty_lsm(fs)?;
        assert_state(lsm, &[], 0);

        {
            put_and_assert(lsm, fs, 30, 0, &[], 0)?;
            put_and_assert(lsm, fs, 10, 1, &[], 0)?;
            put_and_assert(lsm, fs, 40, 2, &[], 0)?;
            put_and_assert(lsm, fs, 11, 3, &[], 0)?;
            put_and_assert(lsm, fs, 50, 4, &[], 0)?;
            put_and_assert(lsm, fs, 90, 5, &[&[6]], 1)?;

            put_and_assert(lsm, fs, 20, 6, &[&[6]], 1)?;
            put_and_assert(lsm, fs, 60, 7, &[&[6]], 1)?;
            put_and_assert(lsm, fs, 51, 8, &[&[6]], 1)?;
            put_and_assert(lsm, fs, 31, 9, &[&[6]], 1)?;
            put_and_assert(lsm, fs, 52, 10, &[&[6]], 1)?;
            put_and_assert(lsm, fs, 80, 11, &[&[12]], 2)?;

            put_and_assert(lsm, fs, 91, 12, &[&[12]], 2)?;
            put_and_assert(lsm, fs, 70, 13, &[&[12]], 2)?;
            put_and_assert(lsm, fs, 92, 14, &[&[12]], 2)?;
            put_and_assert(lsm, fs, 32, 15, &[&[12]], 2)?;
            put_and_assert(lsm, fs, 21, 16, &[&[12]], 2)?;
            put_and_assert(lsm, fs, 33, 17, &[&[], &[18]], 1)?;
        }

        {
            delete_and_assert(lsm, fs, 81, &[&[], &[18]], 1)?;
            put_and_assert(lsm, fs, 41, 19, &[&[], &[18]], 1)?;
            put_and_assert(lsm, fs, 61, 20, &[&[], &[18]], 1)?;
            delete_and_assert(lsm, fs, 21, &[&[], &[18]], 1)?;
            put_and_assert(lsm, fs, 62, 22, &[&[], &[18]], 1)?;
            put_and_assert(lsm, fs, 42, 23, &[&[6], &[18]], 1)?;

            delete_and_assert(lsm, fs, 31, &[&[6], &[18]], 1)?;
            put_and_assert(lsm, fs, 32, 25, &[&[6], &[18]], 1)?;
            put_and_assert(lsm, fs, 82, 26, &[&[6], &[18]], 1)?;
            delete_and_assert(lsm, fs, 33, &[&[6], &[18]], 1)?;
            put_and_assert(lsm, fs, 22, 28, &[&[6], &[18]], 1)?;
            put_and_assert(lsm, fs, 71, 29, &[&[6, 6], &[18]], 1)?;

            delete_and_assert(lsm, fs, 91, &[&[6, 6], &[18]], 1)?;
            put_and_assert(lsm, fs, 51, 31, &[&[6, 6], &[18]], 1)?;
            put_and_assert(lsm, fs, 1, 32, &[&[6, 6], &[18]], 1)?;
            delete_and_assert(lsm, fs, 23, &[&[6, 6], &[18]], 1)?;
            put_and_assert(lsm, fs, 83, 34, &[&[6, 6], &[18]], 1)?;
            put_and_assert(lsm, fs, 84, 35, &[&[], &[24]], 2)?;
        }

        {
            delete_and_assert(lsm, fs, 42, &[&[], &[24]], 2)?;
            put_and_assert(lsm, fs, 12, 37, &[&[], &[24]], 2)?;
            put_and_assert(lsm, fs, 92, 38, &[&[], &[24]], 2)?;
            delete_and_assert(lsm, fs, 72, &[&[], &[24]], 2)?;
            put_and_assert(lsm, fs, 13, 40, &[&[], &[24]], 2)?;
            put_and_assert(lsm, fs, 62, 41, &[&[6], &[24]], 2)?;

            delete_and_assert(lsm, fs, 93, &[&[6], &[24]], 2)?;
            put_and_assert(lsm, fs, 32, 43, &[&[6], &[24]], 2)?;
            put_and_assert(lsm, fs, 94, 44, &[&[6], &[24]], 2)?;
            delete_and_assert(lsm, fs, 95, &[&[6], &[24]], 2)?;
            put_and_assert(lsm, fs, 33, 46, &[&[6], &[24]], 2)?;
            put_and_assert(lsm, fs, 73, 47, &[&[6, 6], &[24]], 2)?;

            delete_and_assert(lsm, fs, 52, &[&[6, 6], &[24]], 2)?;
            put_and_assert(lsm, fs, 14, 49, &[&[6, 6], &[24]], 2)?;
            put_and_assert(lsm, fs, 2, 50, &[&[6, 6], &[24]], 2)?;
            delete_and_assert(lsm, fs, 53, &[&[6, 6], &[24]], 2)?;
            put_and_assert(lsm, fs, 82, 52, &[&[6, 6], &[24]], 2)?;
            put_and_assert(lsm, fs, 22, 53, &[&[], &[], &[29]], 1)?;
        }

        Ok(())
    }

    #[test]
    fn test_full_delete() -> Result<()> {
        let fs = &test_fs("full_delete");
        let lsm = &mut empty_lsm(fs)?;

        for i in 0..18 {
            lsm.put(i, i, fs)?;
        }

        for i in 0..18 {
            lsm.delete(i, fs)?;
        }

        // See the "Hacky workaround:" comment.
        assert_state(lsm, &[&[], &[1]], 2);

        Ok(())
    }
}
