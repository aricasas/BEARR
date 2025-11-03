use std::{
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use crate::{DbError, btree::BTree, btree::BTreeIter};

#[cfg(not(feature = "mock"))]
use crate::file_system::FileSystem;

#[cfg(feature = "mock")]
use crate::mock::FileSystem;

/// A handle to an SST of a database
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub struct Sst {
    // TODO: store paths to all levels of the Btree, possibly with their file sizes?
    // TODO add whatever metadata is needed for Btree traversal
    pub path: PathBuf,
    pub nodes_offset: u64,
    pub leafs_offset: u64,
    pub tree_depth: u64,
}

impl Sst {
    /*
     * Create an SST table to store contents on disk
     * */
    pub fn create(
        key_values: impl IntoIterator<Item = Result<(u64, u64), DbError>>,
        path: impl AsRef<Path>,
        file_system: &mut FileSystem,
    ) -> Result<Sst, DbError> {
        // TODO make a directory at path, and
        // TODO call write_btree_to_files to write the Btree inside it

        let key_values = key_values.into_iter();

        let (nodes_offset, leafs_offset, tree_depth) =
            BTree::write(&path, key_values, file_system)?;

        Ok(Sst {
            path: path.as_ref().to_owned(),
            nodes_offset,
            leafs_offset,
            tree_depth,
        })
    }

    /* Open the file and add it to opened files
     * find the file's SST and give it back
     * */
    pub fn open(path: impl AsRef<Path>, file_system: &FileSystem) -> Result<Sst, DbError> {
        // TODO change this since now a Sst is a directory containing Btree files
        let (nodes_offset, leafs_offset, tree_depth) = BTree::open(&path, file_system)?;

        Ok(Sst {
            path: path.as_ref().to_owned(),
            nodes_offset,
            leafs_offset,
            tree_depth,
        })
    }

    pub fn get(&self, key: u64, file_system: &FileSystem) -> Result<Option<u64>, DbError> {
        BTree::get(self, key, file_system)
    }

    pub fn scan<'a, 'b>(
        &'a self,
        range: RangeInclusive<u64>,
        file_system: &'b FileSystem,
    ) -> Result<BTreeIter<'a, 'b>, DbError> {
        BTreeIter::new(self, range, file_system)
    }
}

/* Tests for SSTs */
#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use anyhow::Result;

    use super::*;

    struct TestPath {
        path: PathBuf,
    }

    impl TestPath {
        fn new(path: impl AsRef<Path>) -> Self {
            Self {
                path: path.as_ref().to_path_buf(),
            }
        }
    }

    impl AsRef<Path> for TestPath {
        fn as_ref(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            _ = fs::remove_file(&self.path);
        }
    }

    #[test]
    fn test_problematic_ssts() {
        let path = &TestPath::new("/xyz/abc/file");
        Sst::create(vec![], path, &mut Default::default()).unwrap_err();

        let path = &TestPath::new("./db/SST_Duplicate");
        _ = Sst::create(vec![], path, &mut Default::default());
        Sst::create(vec![], path, &mut Default::default()).unwrap_err();
    }

    /* Create an SST and then open it up to see if sane */
    #[test]
    fn test_create_open_sst() -> Result<()> {
        let file_name = "./db/SST_Test_Create_Open";
        let path = &TestPath::new(file_name);
        let mut file_system = FileSystem::new(1, 1)?;

        Sst::create(vec![], path, &mut file_system)?;

        assert!(matches!(
            Sst::open(path, &file_system),
            Err(DbError::CorruptSst)
        ));

        Ok(())
    }

    /* Write contents to SST and read them afterwards */
    #[test]
    fn test_read_write_to_sst() -> Result<()> {
        let file_name = "./db/SST_Test_Read_Write";
        let path = &TestPath::new(file_name);

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
            path,
            &mut Default::default(),
        )?;

        let file_system = FileSystem::default();
        let sst = Sst::open(path, &file_system)?;

        let scan = sst.scan(11..=12, &file_system)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 1);
        assert_eq!(scan.item_number, 5);

        let scan = sst.scan(2..=12, &file_system)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 1);
        assert_eq!(scan.item_number, 1);

        Ok(())
    }

    #[test]
    fn test_scan_sst() -> Result<()> {
        let file_name = "./db/SST_Test_Scan";
        let path = &TestPath::new(file_name);

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
            path,
            &mut Default::default(),
        )?;

        let file_system = FileSystem::default();
        let sst = Sst::open(path, &file_system)?;

        let mut scan = sst.scan(2..=12, &file_system)?;
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
        let file_name = "./db/SST_Test_Huge";
        let path = &TestPath::new(file_name);

        let mut test_vec = Vec::<(u64, u64)>::new();
        for i in 1..400_000 {
            test_vec.push((i, i));
        }

        let mut file_system = FileSystem::default();
        Sst::create(test_vec.into_iter().map(Ok), path, &mut file_system)?;

        let sst = Sst::open(path, &file_system)?;

        // let file_size = sst.num_pages * PAGE_SIZE;
        // println!("Current File Size : {}", file_size);

        let range_start = 1;
        let range_end = 4000;
        let file_system = FileSystem::default();

        let mut scan = sst.scan(range_start..=range_end, &file_system)?;

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
