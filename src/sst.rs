use std::{
    fs,
    io::Write,
    ops::RangeInclusive,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

use bincode::config::{Fixint, LittleEndian, NoLimit};

use crate::DBError;

const CHUNK_SIZE: usize = 4096;
const PAIRS_PER_CHUNK: usize = (CHUNK_SIZE - 8) / 16;
const PADDING: usize = CHUNK_SIZE - PAIRS_PER_CHUNK * 16 - 8;

/// A handle to an SST of a database
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
            length: Default::default(),
            pairs: [(0, 0); _],
            padding: Default::default(),
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
    pub fn create(key_values: Vec<(u64, u64)>, path: &Path) -> Result<SST, DBError> {
        let path: PathBuf = path.to_path_buf();

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

            let byte_len = bincode::encode_into_slice(&page, &mut buffer, BINCODE_CONFIG)
                .map_err(|e| DBError::IOError(e.to_string()))?;
            debug_assert_eq!(byte_len, CHUNK_SIZE);

            file.write_all(&buffer)?;
        }

        if !remainder.is_empty() {
            let mut page = Page::empty();
            page.length = remainder.len() as u64;

            let (actual_pairs, _) = page.pairs.split_at_mut(remainder.len());
            actual_pairs.copy_from_slice(remainder);

            let byte_len = bincode::encode_into_slice(&page, &mut buffer, BINCODE_CONFIG)
                .map_err(|e| DBError::IOError(e.to_string()))?;
            debug_assert_eq!(byte_len, CHUNK_SIZE);

            file.write_all(&buffer)?;
        }

        let sst = SST { opened_file: file };
        Ok(sst)
    }

    /* Open the file and add it to opened files
     * find the file's SST and give it back
     * */
    pub fn open(path: &Path) -> Result<SST, DBError> {
        let path: PathBuf = path.to_path_buf();

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

            let mut byte_len = 0;
            (*buffered_page, byte_len) =
                bincode::borrow_decode_from_slice(&buffer_bytes, BINCODE_CONFIG)
                    .map_err(|e| DBError::IOError(e.to_string()))?;
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

        let item = self.buffered_page.pairs[self.item_number];

        if &item.0 > self.range.end() {
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
            .read_exact_at(&mut byte_buffer, page_offset as u64)
            .map_err(|e| DBError::IOError(e.to_string()));
        if let Err(e) = res {
            self.ended = true;
            return Some(Err(e));
        }

        let mut byte_len = 0;
        let res = bincode::borrow_decode_from_slice(&byte_buffer, BINCODE_CONFIG)
            .map_err(|e| DBError::IOError(e.to_string()));
        if let Err(e) = res {
            self.ended = true;
            return Some(Err(e));
        }

        (*self.buffered_page, byte_len) = res.unwrap();
        debug_assert_eq!(byte_len, CHUNK_SIZE);

        Some(Ok(item))
    }
}

/* Tests for SSTs */
#[cfg(test)]
mod tests {
    use super::*;

    struct TestCleanup {
        path: PathBuf,
    }

    impl Drop for TestCleanup {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    #[test]
    fn test_problematic_ssts() {
        let path = Path::new("/xyz/abc/file");
        let sst = SST::create(vec![], path);
        let _cleanup = TestCleanup {
            path: path.to_path_buf(),
        };

        assert!(sst.is_err());
        let path = Path::new("./db/SSTe");
        let _ = SST::create(vec![], path);
        let sst = SST::create(vec![], path);
        assert!(sst.is_err());
    }

    /* Create an SST and then open it up to see if sane */
    #[test]
    fn test_create_open_sst() {
        let file_name = "./db/SST_Test1";
        let path = Path::new(file_name);
        let _cleanup = TestCleanup {
            path: path.to_path_buf(),
        };
        let sst = SST::create(vec![], path);
        assert!(sst.is_ok());

        let sst = SST::open(path);
        assert!(sst.is_ok());
    }

    /* Write contents to SST and read them afterwards */
    #[test]
    fn test_read_write_to_sst() {
        let file_name = "./db/SST_Test2";
        let path = Path::new(file_name);
        let _cleanup = TestCleanup {
            path: path.to_path_buf(),
        };

        let sst = SST::create(
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
        );
        assert!(sst.is_ok());

        let sst = SST::open(path);
        let sst = sst.unwrap();

        let scan = match sst.scan(11..=12) {
            Ok(scan) => scan,
            Err(_) => {
                panic!();
            }
        };

        println!("{} {}", scan.page_number, scan.item_number);

        assert_eq!(scan.page_number, 0);
        assert_eq!(scan.item_number, 5);

        let scan = match sst.scan(2..=12) {
            Ok(scan) => scan,
            Err(e) => {
                println!("error : {}", e);
                panic!();
            }
        };

        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 0);
        assert_eq!(scan.item_number, 1);
    }

    #[test]
    fn test_scan_sst() {
        let file_name = "./db/SST_Test3";
        let path = Path::new(file_name);
        let _cleanup = TestCleanup {
            path: path.to_path_buf(),
        };

        let sst = SST::create(
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
        );
        assert!(sst.is_ok());

        let sst = SST::open(path);
        let sst = sst.unwrap();

        let mut scan = match sst.scan(2..=12) {
            Ok(scan) => scan,
            Err(e) => {
                println!("error : {}", e);
                panic!();
            }
        };

        assert_eq!(scan.next().unwrap(), Ok((3, 4)));
        assert_eq!(scan.next().unwrap(), Ok((5, 6)));
        assert_eq!(scan.next().unwrap(), Ok((7, 8)));
        assert_eq!(scan.next().unwrap(), Ok((9, 10)));
        assert_eq!(scan.next().unwrap(), Ok((11, 12)));
        assert_eq!(scan.next(), None);
    }

    /*
     * Huge test with writing a vector of 400000 elements to file
     * and then doing scans over it
     *
     * This test should be run with superuser privilages
     * */
    #[test]
    fn test_huge_test() {
        let file_name = "./db/SST_Test4";
        let path = Path::new(file_name);
        let _cleanup = TestCleanup {
            path: path.to_path_buf(),
        };

        let mut test_vec = Vec::<(u64, u64)>::new();
        for i in 1..400_000 {
            test_vec.push((i, i));
        }
        let sst = SST::create(test_vec, path);

        /*
         * Flush the actual buffer cache for benchmarking purposes
         * */
        // Command::new("sync").status().expect("Sync Error");
        // Command::new("sh")
        //     .arg("-c")
        //     .arg("echo 3 > /proc/sys/vm/drop_caches")
        //     .status()
        //     .expect("Clearing Cache Error");

        assert!(sst.is_ok());

        let sst = SST::open(path);
        let sst = sst.unwrap();
        let file_size = sst.opened_file.metadata().expect("err 2").len();

        println!("Current File Size : {}", file_size);

        let range_start = 1;
        let range_end = 4000;

        let mut scan = match sst.scan(range_start..=range_end) {
            Ok(scan) => scan,
            Err(e) => {
                println!("error : {}", e);
                panic!();
            }
        };

        let mut page_number = 0;
        for i in range_start..range_end {
            if scan.page_number != page_number {
                page_number = scan.page_number;
                println!("New page moved to memory : {}", page_number);
            }

            assert_eq!(scan.next().unwrap(), Ok((i, i)));
        }
    }
}
