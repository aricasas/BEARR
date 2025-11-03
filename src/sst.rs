use std::{
    fs,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    rc::Rc,
};

use crate::{DbError, PAGE_SIZE, btree::BTree, btree::BTreeIter, file_system::Aligned};

#[cfg(not(feature = "mock"))]
use crate::file_system::FileSystem;

#[cfg(feature = "mock")]
use crate::mock::FileSystem;

// TODO remove this constants, define any page stuff in btree.rs
const PAIRS_PER_CHUNK: usize = (PAGE_SIZE - 8) / 16;
const PADDING: usize = PAGE_SIZE - 8 - PAIRS_PER_CHUNK * 16;

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

// TODO remove this and use whatever page structs you need for the Btree in the btree.rs file
#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
struct Page {
    /// Number of pairs stored in this page
    length: u64,
    pairs: [[u64; 2]; PAIRS_PER_CHUNK],
    padding: [u8; PADDING],
}

impl Default for Page {
    fn default() -> Self {
        Self {
            length: 0,
            pairs: [Default::default(); _],
            padding: [Default::default(); _],
        }
    }
}

impl Page {
    fn new() -> Box<Self> {
        Box::new(Self::default())
    }
}

const _: () = assert!(size_of::<Page>() == PAGE_SIZE);

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
        let mut scanner = self.scan(key..=key, file_system)?;

        match scanner.next() {
            None => Ok(None),
            Some(Err(e)) => Err(e),
            Some(Ok((_, v))) => Ok(Some(v)),
        }
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
    use std::path::PathBuf;

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

        Sst::create(vec![], path, &mut Default::default())?;

        Sst::open(path)?;

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

        let sst = Sst::open(path)?;

        let file_system = FileSystem::default();

        let scan = sst.scan(11..=12, &file_system)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 0);
        assert_eq!(scan.item_number, 5);

        let scan = sst.scan(2..=12, &file_system)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 0);
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

        let sst = Sst::open(path)?;
        let file_system = FileSystem::default();

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
        Sst::create(test_vec.into_iter().map(Ok), path, &mut Default::default())?;

        let sst = Sst::open(path)?;

        let file_size = sst.num_pages * PAGE_SIZE;
        println!("Current File Size : {}", file_size);

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
