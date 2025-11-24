use std::ops::RangeInclusive;

use crate::{
    DbError,
    bloom_filter::BloomFilter,
    btree::{BTree, BTreeIter, BTreeMetadata},
    file_system::FileId,
};

#[cfg(not(feature = "mock"))]
use crate::file_system::FileSystem;

#[cfg(feature = "mock")]
use crate::mock::FileSystem;

/// A handle to an SST of a database
#[derive(Debug)]
pub struct Sst {
    pub btree_metadata: BTreeMetadata,
    pub file_id: FileId,
    pub filter: BloomFilter,
}

impl Sst {
    /*
     * Create an SST table to store contents on disk
     *
     * `n_entries_hint` is an upper bound
     * */
    pub fn create(
        key_values: impl IntoIterator<Item = Result<(u64, u64), DbError>>,
        n_entries_hint: usize,
        bits_per_entry: usize,
        file_id: FileId,
        file_system: &FileSystem,
    ) -> Result<Sst, DbError> {
        let key_values = key_values.into_iter();

        let (btree_metadata, filter) = BTree::write(
            file_id,
            key_values,
            n_entries_hint,
            bits_per_entry,
            file_system,
        )?;

        Ok(Sst {
            file_id,
            btree_metadata,
            filter,
        })
    }

    /* Open the file and add it to opened files
     * find the file's SST and give it back
     * */
    pub fn open(file_id: FileId, file_system: &FileSystem) -> Result<Sst, DbError> {
        let (btree_metadata, filter) = BTree::open(file_id, file_system)?;

        Ok(Sst {
            file_id,
            btree_metadata,
            filter,
        })
    }

    pub fn get(&self, key: u64, file_system: &FileSystem) -> Result<Option<u64>, DbError> {
        if !self.filter.query(key) {
            return Ok(None);
        }

        BTree::get(self, key, file_system)
    }

    pub fn scan<'a, 'b>(
        &'a self,
        range: RangeInclusive<u64>,
        file_system: &'b FileSystem,
    ) -> Result<BTreeIter<'a, 'b>, DbError> {
        BTreeIter::new(self, range, file_system)
    }

    pub fn num_entries(&self) -> usize {
        self.btree_metadata.n_entries as usize
    }

    pub fn destroy(self, file_system: &FileSystem) -> Result<(), DbError> {
        file_system.delete_file(self.file_id)?;
        Ok(())
    }

    pub fn rename(&mut self, new_file_id: FileId, file_system: &FileSystem) -> Result<(), DbError> {
        file_system.rename_file(self.file_id, new_file_id)?;
        self.file_id = new_file_id;
        Ok(())
    }
}

/* Tests for SSTs */
#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::test_util::TestFs;

    use super::*;

    fn test_file_id(num: usize) -> FileId {
        FileId {
            lsm_level: 0,
            sst_number: num,
        }
    }

    /* Create an SST and then open it up to see if sane */
    #[test]
    fn test_create_open_sst() -> Result<()> {
        let mut fs = TestFs::new("./db/2/");

        let path = test_file_id(2);

        Sst::create(vec![], 1, 1, path, &mut fs)?;

        assert!(matches!(Sst::open(path, &fs), Err(DbError::CorruptSst)));

        Ok(())
    }

    /* Write contents to SST and read them afterwards */
    #[test]
    fn test_read_write_to_sst() -> Result<()> {
        let mut fs = TestFs::new("./db/3/");

        let path = test_file_id(3);

        Sst::create(
            [
                (1, 2),
                (3, 4),
                (5, 6),
                (7, 8),
                (9, 10),
                (11, 12),
                (13, 14),
                (15, 16),
            ]
            .into_iter()
            .map(Ok),
            8,
            8,
            path,
            &mut fs,
        )?;

        let sst = Sst::open(path, &fs)?;

        let scan = sst.scan(11..=12, &fs)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 1);
        assert_eq!(scan.item_number, 5);

        let scan = sst.scan(2..=12, &fs)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 1);
        assert_eq!(scan.item_number, 1);

        Ok(())
    }

    #[test]
    fn test_scan_sst() -> Result<()> {
        let mut fs = TestFs::new("./db/4/");

        let path = test_file_id(4);

        Sst::create(
            [
                (1, 2),
                (3, 4),
                (5, 6),
                (7, 8),
                (9, 10),
                (11, 12),
                (13, 14),
                (15, 16),
            ]
            .into_iter()
            .map(Ok),
            8,
            8,
            path,
            &mut fs,
        )?;

        let sst = Sst::open(path, &fs)?;

        let mut scan = sst.scan(2..=12, &fs)?;
        assert_eq!(scan.next().unwrap()?, (3, 4));
        assert_eq!(scan.next().unwrap()?, (5, 6));
        assert_eq!(scan.next().unwrap()?, (7, 8));
        assert_eq!(scan.next().unwrap()?, (9, 10));
        assert_eq!(scan.next().unwrap()?, (11, 12));
        assert_eq!(scan.next(), None);

        Ok(())
    }

    /*
     * Huge test with writing a vector of 400000 elements to file
     * and then doing scans over it
     * */
    #[test]
    fn test_huge_test() -> Result<()> {
        let mut fs = TestFs::new("./db/5/");

        let path = test_file_id(5);

        let mut test_vec = Vec::<(u64, u64)>::new();
        for i in 1..400_000 {
            test_vec.push((i, i));
        }

        Sst::create(test_vec.into_iter().map(Ok), 400_000, 8, path, &mut fs)?;

        let sst = Sst::open(path, &fs)?;

        // let file_size = sst.num_pages * PAGE_SIZE;
        // println!("Current File Size : {}", file_size);

        let range_start = 1;
        let range_end = 4000;

        let mut scan = sst.scan(range_start..=range_end, &fs)?;

        let mut page_number = 0;
        for i in range_start..range_end {
            if scan.page_number != page_number {
                page_number = scan.page_number;
                println!("New page moved to memory : {}", page_number);
            }

            assert_eq!(scan.next().unwrap()?, (i, i));
        }

        Ok(())
    }
}
