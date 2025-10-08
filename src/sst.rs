#![allow(warnings)]

use std::{
    fs, io,
    io::{BufReader, Read, Seek, Write},
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use crate::DBError;
use serde::{Deserialize, Serialize};

const CHUNK_SIZE: usize = 4096;

/// A handle to an SST of a database
#[derive(Debug)]
pub struct SST {
    filename: PathBuf,
    opened_file: Option<fs::File>,
}

impl SST {
    /*
     * Create an SST table to store contents on disk
     * */
    pub fn create(key_values: Vec<(u64, u64)>, path: &Path) -> Result<SST, DBError> {
        let path: PathBuf = path.to_path_buf();

        /* TODO : A less expensive way to check if file exists??
         * */

        let mut file = match fs::File::create_new(&path) {
            Ok(file) => file,
            Err(e) => {
                println!("failed to create : {}", e);
                return Err(DBError::IOError(e.to_string()));
            }
        };

        /* TODO:: change this maybe - seems useless */
        let sst = SST {
            filename: path.clone(),
            opened_file: None,
        };

        /* Serialize the vector */
        let bytes = match bincode::serialize(&key_values) {
            Ok(bytes) => bytes,
            Err(e) => {
                println!("Serialization Error : {}", e);
                return Err(DBError::IOError(e.to_string()));
            }
        };

        /* Write to file and make sure the write is flushed to disk */
        match file.write_all(&bytes) {
            Ok(n) => {
                file.sync_all();
            }
            Err(e) => {
                println!("failed to write : {}", e);
                return Err(DBError::IOError(e.to_string()));
            }
        }

        Ok(sst)
    }

    /* Open the file and add it to opened files
     * find the file's SST and give it back
     *
     * TODO:: mmap huge files into memory for faster future accesses
     * */
    pub fn open(path: &Path) -> Result<SST, DBError> {
        let path: PathBuf = path.to_path_buf();

        match fs::File::open(&path) {
            Ok(file) => {
                return Ok(SST {
                    filename: path,
                    opened_file: Some(file),
                });
            }
            Err(e) => {
                println!("failed to open : {}", e);
                return Err(DBError::IOError(e.to_string()));
            }
        };
    }

    pub fn get(&self, key: u64) -> Result<Option<u64>, DBError> {
        todo!()
    }

    pub fn scan(&self, range: RangeInclusive<u64>) -> Result<SSTIter, DBError> {
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
    sst: &'a SST,
    reader: BufReader<&'a fs::File>,
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

        if sst.opened_file.is_none() {
            return Err(DBError::IOError("No File Opened".to_string()));
        }

        let mut buffer = Vec::new();
        let mut page_number = 0;
        let mut item_number = 0;
        let mut reader = BufReader::with_capacity(CHUNK_SIZE, sst.opened_file.as_ref().unwrap());
        let mut found = false;
        let mut ended = false;

        /* Set the reader to the start of the file
         * TODO: Discuss this
         * */
        reader.seek(io::SeekFrom::Start(0))?;

        /* Read SST in pages(chuck size = CHUNK_SIZE) to find the start of the range
         * save page number, item_number and buffer the contents of the page
         * */
        for page in 1.. {
            match bincode::deserialize_from::<_, Vec<(u64, u64)>>(&mut reader) {
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
                /* TODO: Handle EOF ?? */
                Err(e) => {
                    println!(
                        "Some error occured while reading the file : {}",
                        e.to_string()
                    );
                    return Err(DBError::IOError(e.to_string()));
                }
            }
        }

        /* TODO: handle not found ?? */
        if !found {
            return Err(DBError::IOError("Start not found".to_string()));
        }

        let mut iter = Self {
            page_number,
            item_number,
            buffer,
            range,
            sst,
            reader,
            ended,
        };

        /* iter.go_to_start(); */
        Ok(iter)
    }

    fn go_to_next(&mut self) -> Option<Result<(u64, u64), DBError>> {
        if self.ended {
            return None;
        }
        let item = self.buffer[self.item_number];
        if (item.0 < *self.range.end()) {
            self.item_number += 1;
            if (self.item_number >= self.buffer.len()) {
                match bincode::deserialize_from::<_, Vec<(u64, u64)>>(&mut self.reader) {
                    Ok(buf) => {
                        self.item_number = 0;
                        self.page_number += 1;
                        self.buffer = buf;
                    }
                    Err(_) => {
                        // println!(
                        //     "Some error occured while reading the file : {}",
                        //     e.to_string()
                        // );
                        // return Some(Err(DBError::IOError(e.to_string())));
                        self.ended = true;
                        return None;
                    }
                }
            }

            return Some(Ok(item));
        } else {
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
        assert!(sst.is_err());

        let path = Path::new("./db/SSTe");
        let sst = SST::create(vec![], path);
        let sst = SST::create(vec![], path);
        assert!(sst.is_err());
        fs::remove_file(path);
    }

    /* Create an SST and then open it up to see if sane */
    #[test]
    fn test_create_open_sst() {
        let file_name = "./db/SST_Test";
        let path = Path::new(file_name);
        let _cleanup = TestCleanup {
            path: path.to_path_buf(),
        };
        let sst = SST::create(vec![], path);
        assert!(!sst.is_err());

        let sst = SST::open(path);
        assert!(!sst.is_err());
    }

    /* Write contents to SST and read them afterwards */
    #[test]
    fn test_read_write_to_sst() {
        let file_name = "./db/SST_Test";
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
        assert!(!sst.is_err());

        let sst = SST::open(path);
        let sst = sst.unwrap();

        let mut scan = match sst.scan(11..=12) {
            Ok(scan) => scan,
            Err(e) => {
                panic!();
            }
        };

        /* TODO: Add some automation to these tests */
        println!("{} {}", scan.page_number, scan.item_number);

        assert_eq!(scan.page_number, 1);
        assert_eq!(scan.item_number, 5);

        let mut scan = match sst.scan(2..=12) {
            Ok(scan) => scan,
            Err(e) => {
                println!("error : {}", e.to_string());
                panic!();
            }
        };

        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 1);
        assert_eq!(scan.item_number, 1);
    }

    #[test]
    fn test_scan_sst() {
        let file_name = "./db/SST_Test";
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
        assert!(!sst.is_err());

        let sst = SST::open(path);
        let sst = sst.unwrap();

        let mut scan = match sst.scan(2..=12) {
            Ok(scan) => scan,
            Err(e) => {
                println!("error : {}", e.to_string());
                panic!();
            }
        };

        assert_eq!(scan.next().unwrap(), Ok((3, 4)));
        assert_eq!(scan.next().unwrap(), Ok((5, 6)));
        assert_eq!(scan.next().unwrap(), Ok((7, 8)));
        assert_eq!(scan.next().unwrap(), Ok((9, 10)));
        assert_eq!(scan.next().unwrap(), Ok((11, 12)));
        assert_eq!(scan.next().unwrap(), Ok((13, 14)));
        assert_eq!(scan.next().unwrap(), Ok((15, 16)));
    }
}
