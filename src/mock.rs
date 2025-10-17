use std::{
    fs::{self, File},
    io::Write,
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use crate::error::DBError;

#[allow(clippy::upper_case_acronyms)]
pub struct SST {
    filename: PathBuf,
}

impl SST {
    pub fn create(key_values: Vec<(u64, u64)>, path: impl AsRef<Path>) -> Result<SST, DBError> {
        let path = path.as_ref();
        let mut file = File::create_new(path)?;
        write!(&mut file, "{}", serde_json::to_string(&key_values).unwrap())?;
        Ok(Self {
            filename: path.to_path_buf(),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<SST, DBError> {
        let path = path.as_ref();
        Ok(Self {
            filename: path.to_path_buf(),
        })
    }

    pub fn get(&self, key: u64) -> Result<Option<u64>, DBError> {
        let key_values = fs::read_to_string(&self.filename)?;
        let key_values: Vec<(u64, u64)> = serde_json::from_str(&key_values).unwrap();
        Ok(key_values
            .iter()
            .find_map(|&(k, v)| (k == key).then_some(v)))
    }

    pub fn scan(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<impl Iterator<Item = Result<(u64, u64), DBError>>, DBError> {
        let key_values = fs::read_to_string(&self.filename)?;
        let mut key_values: Vec<(u64, u64)> = serde_json::from_str(&key_values).unwrap();
        key_values.sort();
        Ok(key_values
            .into_iter()
            .filter_map(move |(k, v)| range.contains(&k).then_some(Ok((k, v)))))
    }
}
