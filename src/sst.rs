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
}

impl SST {
    /*
     * Create an SST table to store contents on disk
     * */
    pub fn create(key_values: Vec<(u64, u64)>, path: &Path) -> Result<SST, DBError> {
        println!("new file ---> {}", path.display());
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

        let sst = SST {
            filename: path.clone(),
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

    pub fn open(path: &Path) -> Result<SST, DBError> {
        todo!()
    }

    pub fn get(&self, key: u64) -> Result<Option<u64>, DBError> {
        todo!()
    }

    pub fn scan(&self, range: RangeInclusive<u64>) -> Result<SSTIter, DBError> {
        todo!()
    }
}

/* SST itterator */
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
    fn test_problematic_create_sst() {
        let path = Path::new("/xyz/abc/file.txt");
        let sst = SST::create(vec![], path);
        assert!(sst.is_err());

        let path = Path::new("./db/SSTe.txt");
        let sst = SST::create(vec![], path);
        let sst = SST::create(vec![], path);
        assert!(sst.is_err());
    }

    #[test]
    fn test_create_sst() {
        let path = Path::new("./db/SST");
        let sst = SST::create(vec![], path);
        assert!(!sst.is_err());
        if let Ok(value) = sst {
            println!("foile {}", value.filename.display());
        }
    }

    #[test]
    fn test_write_to_sst() {
        todo!()
    }

    #[test]
    fn test_read_from_sst() {
        todo!()
    }

    #[test]
    fn test_scan_sst() {
        todo!()
    }
}
