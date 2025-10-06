use std::{
    fs, io,
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use crate::DBError;

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
        let sst = SST {
            filename: path.clone(),
        };
        match fs::write(&path, "hello world") {
            Ok(_) => {}
            Err(e) => {
                println!("failed to write : {}", e);
                return Err(DBError);
            }
        }

        Ok(sst)
    }

    pub fn open(path: &Path) -> Result<SST, DBError> {
        todo!()
    }

    pub fn get(&self) -> Result<Option<u64>, DBError> {
        todo!()
    }

    pub fn scan(&self, range: RangeInclusive<u64>) -> Result<SSTIter, DBError> {
        todo!()
    }
}

/* SST itterator */
pub struct SSTIter {}
impl Iterator for SSTIter {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

/* Tests for SSTs */
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_sst() {
        let path = Path::new("/home/bigwhomann/file.txt");
        let sst = SST::create(vec![], path);
        if let Ok(value) = sst {
            println!("{}", value.filename.display());
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
