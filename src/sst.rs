use std::{
    fs,
    io::Write,
    ops::RangeInclusive,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
};

use bincode::config::{Fixint, LittleEndian, NoLimit};

use crate::DBError;

const CHUNK_SIZE: usize = 4096;
const PAIRS_PER_CHUNK: usize = (CHUNK_SIZE - 8) / 16;
const PADDING: usize = CHUNK_SIZE - 8 - PAIRS_PER_CHUNK * 16;

/// A handle to an SST of a database
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub struct SST {
    opened_file: fs::File,
}

#[repr(C)]
#[derive(bincode::Encode, bincode::Decode)]
struct Page {
    /// Number of pairs stored in this page
    length: u64,
    pairs: [(u64, u64); PAIRS_PER_CHUNK],
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
}

const _: () = assert!(size_of::<Page>() == CHUNK_SIZE);

const BINCODE_CONFIG: bincode::config::Configuration<LittleEndian, Fixint, NoLimit> =
    bincode::config::legacy();

impl SST {
    /*
     * Create an SST table to store contents on disk
     * */
    pub fn create(key_values: Vec<(u64, u64)>, path: impl AsRef<Path>) -> Result<SST, DBError> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(path)?;

        let mut buffer = vec![0u8; CHUNK_SIZE];

        let (chunks, remainder) = key_values.as_chunks::<PAIRS_PER_CHUNK>();

        let mut page = Page::empty();

        for chunk in chunks {
            page.pairs.copy_from_slice(chunk);

            page.length = chunk.len() as u64;

            let byte_len = bincode::encode_into_slice(&page, &mut buffer, BINCODE_CONFIG)?;
            debug_assert_eq!(byte_len, CHUNK_SIZE);

            file.write_all(&buffer)?;
        }

        if !remainder.is_empty() {
            let (actual_pairs, empty_pairs) = page.pairs.split_at_mut(remainder.len());
            actual_pairs.copy_from_slice(remainder);
            empty_pairs.fill(Default::default());

            page.length = remainder.len() as u64;

            let byte_len = bincode::encode_into_slice(&page, &mut buffer, BINCODE_CONFIG)?;
            debug_assert_eq!(byte_len, CHUNK_SIZE);

            file.write_all(&buffer)?;
        }

        Ok(SST { opened_file: file })
    }

    /* Open the file and add it to opened files
     * find the file's SST and give it back
     * */
    pub fn open(path: impl AsRef<Path>) -> Result<SST, DBError> {
        let file = fs::OpenOptions::new()
            .write(true)
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(path)?;

        Ok(SST { opened_file: file })
    }

    pub fn get(&self, key: u64) -> Result<Option<u64>, DBError> {
        let mut scanner = self.scan(key..=key)?;

        match scanner.next() {
            None => Ok(None),
            Some(Err(e)) => Err(e),
            Some(Ok((_, v))) => Ok(Some(v)),
        }
    }

    pub fn scan(&self, range: RangeInclusive<u64>) -> Result<SSTIter<'_>, DBError> {
        SSTIter::new(self, range)
    }
}

/* SST iterator
 * Contains a 4KB buffer that keeps the wanted SST page in memory
 *
 *
 * */
pub struct SSTIter<'a> {
    page_number: usize,
    item_number: usize,
    buffered_page: Box<Page>,
    range: RangeInclusive<u64>,
    file: &'a fs::File,
    num_pages: usize,
    ended: bool,
}

impl<'a> Iterator for SSTIter<'a> {
    type Item = Result<(u64, u64), DBError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.go_to_next()
    }
}

impl<'a> SSTIter<'a> {
    fn new(sst: &'a SST, range: RangeInclusive<u64>) -> Result<Self, DBError> {
        if range.start() > range.end() {
            return Err(DBError::InvalidScanRange);
        }

        let mut buffer_bytes = vec![0u8; CHUNK_SIZE];
        let mut buffered_page = Page::empty();
        let mut page_number = 0;
        let mut item_number = 0;
        let mut found = false;

        let file_size = sst.opened_file.metadata()?.len() as usize;
        if !file_size.is_multiple_of(CHUNK_SIZE) {
            return Err(DBError::IOError("SST file size not aligned".to_string()));
        }

        let num_pages = file_size / CHUNK_SIZE;

        // Linear search
        'outer: for page in 0..num_pages {
            let page_offset = page * CHUNK_SIZE;

            sst.opened_file
                .read_exact_at(&mut buffer_bytes, page_offset as u64)?;

            let byte_len;
            (*buffered_page, byte_len) =
                bincode::borrow_decode_from_slice(&buffer_bytes, BINCODE_CONFIG)?;
            debug_assert_eq!(byte_len, CHUNK_SIZE);

            for i in 0..buffered_page.length as usize {
                let (key, _) = buffered_page.pairs[i];

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
            page_number,
            item_number,
            buffered_page,
            range,
            file: &sst.opened_file,
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
    fn go_to_next(&mut self) -> Option<Result<(u64, u64), DBError>> {
        if self.ended {
            return None;
        }

        let item @ (key, _) = self.buffered_page.pairs[self.item_number];

        if &key > self.range.end() {
            self.ended = true;
            return None;
        }

        self.item_number += 1;

        if self.item_number < self.buffered_page.length as usize {
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

        let mut byte_buffer = vec![0u8; CHUNK_SIZE];
        let page_offset = self.page_number * CHUNK_SIZE;

        let res = self
            .file
            .read_exact_at(&mut byte_buffer, page_offset as u64);
        if let Err(e) = res {
            self.ended = true;
            return Some(Err(e.into()));
        }

        let res = bincode::borrow_decode_from_slice(&byte_buffer, BINCODE_CONFIG);
        match res {
            Ok(res) => {
                let byte_len;
                (*self.buffered_page, byte_len) = res;
                debug_assert_eq!(byte_len, CHUNK_SIZE);

                Some(Ok(item))
            }
            Err(e) => {
                self.ended = true;
                Some(Err(e.into()))
            }
        }
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
        SST::create(vec![], path).unwrap_err();

        let path = &TestPath::new("./db/SST_Duplicate");
        _ = SST::create(vec![], path);
        SST::create(vec![], path).unwrap_err();
    }

    /* Create an SST and then open it up to see if sane */
    #[test]
    fn test_create_open_sst() -> Result<()> {
        let file_name = "./db/SST_Test_Create_Open";
        let path = &TestPath::new(file_name);

        SST::create(vec![], path)?;

        SST::open(path)?;

        Ok(())
    }

    /* Write contents to SST and read them afterwards */
    #[test]
    fn test_read_write_to_sst() -> Result<()> {
        let file_name = "./db/SST_Test_Read_Write";
        let path = &TestPath::new(file_name);

        SST::create(
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
        )?;

        let sst = SST::open(path)?;

        let scan = sst.scan(11..=12)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 0);
        assert_eq!(scan.item_number, 5);

        let scan = sst.scan(2..=12)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 0);
        assert_eq!(scan.item_number, 1);

        Ok(())
    }

    #[test]
    fn test_scan_sst() -> Result<()> {
        let file_name = "./db/SST_Test_Scan";
        let path = &TestPath::new(file_name);

        SST::create(
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
        )?;

        let sst = SST::open(path)?;

        let mut scan = sst.scan(2..=12)?;
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
        SST::create(test_vec, path)?;

        let sst = SST::open(path)?;

        let file_size = sst.opened_file.metadata()?.len();
        println!("Current File Size : {}", file_size);

        let range_start = 1;
        let range_end = 4000;

        let mut scan = sst.scan(range_start..=range_end)?;

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
