use std::{fs::File, io::Write, os::unix::fs::FileExt, path::Path};

use crate::{PAGE_SIZE, error::DbError, file_system::Aligned};

#[derive(Default)]
pub struct FileSystem;
impl FileSystem {
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        Ok(Self)
    }
    pub fn get(&mut self, path: impl AsRef<Path>, page_number: usize) -> Result<&Aligned, DbError> {
        let mut page = Aligned::new();
        File::open(&path)?.read_exact_at(&mut page.0, (page_number * PAGE_SIZE) as u64)?;
        Ok(Box::leak(page))
    }
    pub fn append(&mut self, path: impl AsRef<Path>, page: &Aligned) -> Result<(), DbError> {
        File::options()
            .append(true)
            .create(true)
            .open(path)?
            .write_all(page)?;
        Ok(())
    }
}
