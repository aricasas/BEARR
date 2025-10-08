use std::{
    fs, io,
    io::{Read, Seek, Write},
    ops::RangeInclusive,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

use crate::DBError;

const CHUNK_SIZE: usize = 4096;

/// A handle to an SST of a database
#[derive(Debug)]
pub struct SST {
    opened_file: fs::File,
}

impl SST {
    /*
     * Create an SST table to store contents on disk
     * */
    pub fn create(key_values: Vec<(u64, u64)>, path: &Path) -> Result<SST, DBError> {
        // dbg!(&key_values);

        let path: PathBuf = path.to_path_buf();

        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(path)?;

        /* An 8 byte overhead to store the size of that page */
        let items_per_page = (CHUNK_SIZE - 8) / 16;
        for chunk in key_values.chunks(items_per_page) {
            /* Serialize the vector in fixed chunk sizes, append the
             * actual size to it and store on disk
             */
            let bytes =
                bincode::serialize(&chunk.to_vec()).map_err(|e| DBError::IOError(e.to_string()))?;
            debug_assert_eq!(bytes.len(), items_per_page * size_of::<(u64, u64)>());

            file.write_all(&(bytes.len() as u64).to_le_bytes())?;
            file.write_all(&bytes)?;
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
    buffer: Vec<(u64, u64)>,
    range: RangeInclusive<u64>,
    reader: &'a fs::File,
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

        let mut buffer = Vec::new();
        let mut page_number = 0;
        let mut item_number = 0;
        let mut reader = &sst.opened_file;
        let mut found = false;
        let mut ended = false;

        /* Set the reader to the start of the file
         * TODO: Discuss this
         * */
        reader.seek(io::SeekFrom::Start(0))?;

        /* Read SST in pages(chuck size = CHUNK_SIZE) to find the start of the range
         * save page number, item_number and buffer the contents of the page
         * */
        for page in 0.. {
            let mut len_bytes = [0u8; 8];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let chunk_len = u64::from_le_bytes(len_bytes) as usize;

                    let mut chunk_data = vec![0u8; chunk_len];
                    match reader.read_exact(&mut chunk_data) {
                        Ok(_) => {
                            match bincode::deserialize::<Vec<(u64, u64)>>(&chunk_data) {
                                Ok(buf) => {
                                    /* TODO: Need to change the implementation to binary search */
                                    for (index, item) in buf.iter().enumerate() {
                                        if item.0 >= *range.start() {
                                            page_number = page;
                                            buffer = buf;
                                            item_number = index;
                                            found = true;
                                            break;
                                        }
                                    }
                                    if found {
                                        break;
                                    }
                                }

                                Err(e) => {
                                    println!("Some error occured while reading the file : {}", e);
                                    return Err(DBError::IOError(e.to_string()));
                                }
                            }
                        }

                        Err(e) => {
                            println!("Some error occured while reading the file : {}", e);
                            return Err(DBError::IOError(e.to_string()));
                        }
                    }
                }
                Err(e) => {
                    break;
                    // dbg!(&e);
                    // println!("Some error occured while reading the file : {}", e);
                    // return Err(DBError::IOError(e.to_string()));
                }
            }
        }

        if !found {
            ended = true;
        }

        let iter = Self {
            page_number,
            item_number,
            buffer,
            range,
            reader,
            ended,
        };

        /* iter.go_to_start(); */
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

        let item = self.buffer[self.item_number];

        /* Check item range */
        if item.0 <= *self.range.end() {
            self.item_number += 1;

            if self.item_number >= self.buffer.len() {
                let mut len_bytes = [0u8; 8];
                match self.reader.read_exact(&mut len_bytes) {
                    Ok(_) => {
                        let chunk_len = u64::from_le_bytes(len_bytes) as usize;
                        let mut chunk_data = vec![0u8; chunk_len];

                        match self.reader.read_exact(&mut chunk_data) {
                            Ok(_) => match bincode::deserialize::<Vec<(u64, u64)>>(&chunk_data) {
                                Ok(buf) => {
                                    self.item_number = 0;
                                    self.page_number += 1;
                                    self.buffer = buf;
                                }
                                Err(e) => {
                                    println!("Deserialization error: {}", e);
                                    self.ended = true;
                                    return None;
                                }
                            },
                            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                                self.ended = true;
                                return None;
                            }
                            Err(e) => {
                                println!("Error Reading File : {}", e);
                                self.ended = true;
                                return None;
                            }
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        self.ended = true;
                        return None;
                    }
                    Err(e) => {
                        println!("Error Reading File : {}", e);
                        self.ended = true;
                        return None;
                    }
                }
            }

            Some(Ok(item))
        } else {
            self.ended = true;
            None
        }
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
