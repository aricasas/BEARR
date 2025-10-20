use std::{
    fs,
    io::Write,
    ops::RangeInclusive,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

use crate::{DbError, PAGE_SIZE, file_system::Aligned};

#[cfg(not(feature = "mock"))]
use crate::file_system::FileSystem;

#[cfg(feature = "mock")]
use crate::mock::FileSystem;

const PAIRS_PER_CHUNK: usize = (PAGE_SIZE - 8) / 16;
const PADDING: usize = PAGE_SIZE - 8 - PAIRS_PER_CHUNK * 16;

/// A handle to an SST of a database
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub struct Sst {
    path: PathBuf,
    file_size: usize,
}

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
    fn empty() -> Box<Self> {
        Box::new(Self::default())
    }

    fn encode(&self) -> &Aligned {
        bytemuck::cast_ref(self)
        // bytemuck::bytes_of(self)
    }

    fn decode(bytes: &Aligned) -> &Self {
        bytemuck::cast_ref(bytes)
    }
}

const _: () = assert!(size_of::<Page>() == PAGE_SIZE);

impl Sst {
    /*
     * Create an SST table to store contents on disk
     * */
    pub fn create(
        key_values: Vec<(u64, u64)>,
        path: impl AsRef<Path>,
        file_system: &mut FileSystem,
    ) -> Result<Sst, DbError> {
        let (chunks, remainder) = key_values.as_chunks::<PAIRS_PER_CHUNK>();

        let mut page = Page::empty();
        let mut file_size = 0;

        for chunk in chunks {
            for (pair, &(key, value)) in page.pairs.iter_mut().zip(chunk) {
                *pair = [key, value];
            }

            page.length = chunk.len() as u64;

            let page_bytes = page.encode();

            file_system.append(&path, page_bytes)?;
            file_size += PAGE_SIZE;
        }

        if !remainder.is_empty() {
            for (pair, &(key, value)) in page.pairs.iter_mut().zip(remainder) {
                *pair = [key, value];
            }

            page.pairs[remainder.len()..].fill(Default::default());

            page.length = remainder.len() as u64;

            let page_bytes = page.encode();

            file_system.append(&path, page_bytes);
            file_size += PAGE_SIZE;
        }

        Ok(Sst {
            path: path.as_ref().to_owned(),
            file_size,
        })
    }

    /* Open the file and add it to opened files
     * find the file's SST and give it back
     * */
    pub fn open(path: impl AsRef<Path>) -> Result<Sst, DbError> {
        let file = fs::OpenOptions::new().open(&path)?;
        let file_size = file.metadata()?.len() as usize;

        Ok(Sst {
            path: path.as_ref().to_owned(),
            file_size,
        })
    }

    pub fn get(&self, key: u64, file_system: &mut FileSystem) -> Result<Option<u64>, DbError> {
        let mut scanner = self.scan(key..=key, file_system)?;

        match scanner.next() {
            None => Ok(None),
            Some(Err(e)) => Err(e),
            Some(Ok((_, v))) => Ok(Some(v)),
        }
    }

    pub fn scan<'a, 'b>(
        &'b self,
        range: RangeInclusive<u64>,
        file_system: &'a mut FileSystem,
    ) -> Result<SstIter<'a, 'b>, DbError> {
        SstIter::new(self, range, file_system)
    }
}

/* SST iterator
 * Contains a 4KB buffer that keeps the wanted SST page in memory
 *
 *
 * */
pub struct SstIter<'a, 'b> {
    file_system: &'a mut FileSystem,
    sst: &'b Sst,
    page_number: usize,
    item_number: usize,
    range: RangeInclusive<u64>,
    num_pages: usize,
    ended: bool,
}

impl<'a, 'b> Iterator for SstIter<'a, 'b> {
    type Item = Result<(u64, u64), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.go_to_next()
    }
}

impl<'a, 'b> SstIter<'a, 'b> {
    fn new(
        sst: &'b Sst,
        range: RangeInclusive<u64>,
        file_system: &'a mut FileSystem,
    ) -> Result<Self, DbError> {
        if range.start() > range.end() {
            return Err(DbError::InvalidScanRange);
        }

        let mut page_number = 0;
        let mut item_number = 0;
        let mut found = false;

        if !sst.file_size.is_multiple_of(PAGE_SIZE) {
            return Err(DbError::IoError("SST file size not aligned".to_string()));
        }

        let num_pages = sst.file_size / PAGE_SIZE;

        // Linear search
        'outer: for page in 0..num_pages {
            let page_bytes = file_system.get(&sst.path, page)?;

            let buffered_page = Page::decode(&page_bytes);

            for i in 0..buffered_page.length as usize {
                let [key, _] = buffered_page.pairs[i];

                if &key >= range.start() {
                    // Found starting key
                    page_number = page;
                    item_number = i;
                    found = true;
                    break 'outer;
                }
            }
        }

        let ended = !found;

        let iter = Self {
            sst,
            file_system,
            page_number,
            item_number,
            range,
            num_pages,
            ended,
        };

        Ok(iter)
    }

    /*
     * Finding the next item in a range
     *
     * While we have not reached the end of the range, go to the next item in the buffer,
     * If we reach the end of the buffer, bring in the next page
     * */
    fn go_to_next(&mut self) -> Option<Result<(u64, u64), DbError>> {
        if self.ended {
            return None;
        }

        let page_bytes = self.file_system.get(&self.sst.path, self.page_number);
        if let Err(error) = page_bytes {
            return Some(Err(error));
        }
        let buffered_page = Page::decode(page_bytes.unwrap());

        let [key, value] = buffered_page.pairs[self.item_number];
        let item = (key, value);

        if &key > self.range.end() {
            self.ended = true;
            return None;
        }

        self.item_number += 1;

        if self.item_number < buffered_page.length as usize {
            return Some(Ok(item));
        }

        // Have to buffer a new page
        self.page_number += 1;
        self.item_number = 0;

        if self.page_number >= self.num_pages {
            // EOF
            self.ended = true;
            return Some(Ok(item));
        }

        Some(Ok(item))
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
            vec![
                (1, 2),
                (3, 4),
                (5, 6),
                (7, 8),
                (9, 10),
                (11, 12),
                (13, 14),
                (15, 16),
            ],
            path,
            &mut Default::default(),
        )?;

        let sst = Sst::open(path)?;

        let mut file_system = FileSystem::new(1)?;

        let scan = sst.scan(11..=12, &mut file_system)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 0);
        assert_eq!(scan.item_number, 5);

        let scan = sst.scan(2..=12, &mut file_system)?;
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
            vec![
                (1, 2),
                (3, 4),
                (5, 6),
                (7, 8),
                (9, 10),
                (11, 12),
                (13, 14),
                (15, 16),
            ],
            path,
            &mut Default::default(),
        )?;

        let sst = Sst::open(path)?;
        let mut file_system = FileSystem::new(1)?;

        let mut scan = sst.scan(2..=12, &mut file_system)?;
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
        Sst::create(test_vec, path, &mut Default::default())?;

        let sst = Sst::open(path)?;

        let file_size = sst.file_size;
        println!("Current File Size : {}", file_size);

        let range_start = 1;
        let range_end = 4000;
        let mut file_system = FileSystem::new(1)?;

        let mut scan = sst.scan(range_start..=range_end, &mut file_system)?;

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
