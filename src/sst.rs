#![allow(warnings)]

use std::{
    fs, io,
    io::Write,
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use crate::DBError;
use serde::{Deserialize, Serialize};

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
        todo!()
    }
}

/* SST iterator */
pub struct SSTIter {}
impl Iterator for SSTIter {
    type Item = Result<(u64, u64), DBError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

/* Tests for SSTs */
#[cfg(test)]
mod tests {
    use super::*;

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
        let sst = SST::create(vec![], path);
        assert!(!sst.is_err());

        let sst = SST::open(path);
        assert!(!sst.is_err());

        fs::remove_file(path);
    }

    /* Write contents to SST and read them afterwards */
    #[test]
    fn test_read_write_to_sst() {
        let path = Path::new("./db/SST_Test");
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
        fs::remove_file(path);
    }

    #[test]
    fn test_scan_sst() {
        todo!()
    }
}
