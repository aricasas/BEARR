use std::{fs::OpenOptions, io::Write, os::unix::fs::FileExt, path::Path};

use crate::{PAGE_SIZE, error::DbError, file_system::Aligned};

#[derive(Default)]
pub struct FileSystem;
impl FileSystem {
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        let _ = capacity;
        Ok(Self)
    }
    pub fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Result<&Aligned, DbError> {
        let mut page = Aligned::new();

        let file = OpenOptions::new().read(true).open(&path)?;

        file.read_exact_at(&mut page.0, (page_number * PAGE_SIZE) as u64)?;

        Ok(Box::leak(page))
    }
    pub fn write_file(
        &self,
        path: impl AsRef<Path>,
        mut write_next: impl FnMut(&mut Aligned) -> Result<bool, DbError>,
    ) -> Result<usize, DbError> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .create(true)
            .open(path)?;

        let mut num_pages = 0;

        let mut page_bytes = Aligned::new();
        while write_next(&mut page_bytes)? {
            file.write_all(&page_bytes.0)?;
            num_pages += 1;
            page_bytes.clear();
        }

        Ok(num_pages)
    }
}
