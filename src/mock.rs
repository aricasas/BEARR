use std::{
    cell::RefCell,
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    rc::Rc,
};

use crate::{PAGE_SIZE, error::DbError, file_system::Aligned};

#[derive(Default)]
pub struct FileSystem(RefCell<HashMap<(PathBuf, usize), Rc<Aligned>>>);
impl FileSystem {
    pub fn new(capacity: usize, write_buffering: usize) -> Result<Self, DbError> {
        let _ = capacity;
        let _ = write_buffering;

        Ok(Self(RefCell::new(HashMap::new())))
    }
    pub fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Result<Rc<Aligned>, DbError> {
        let mut buffer_pool = self.0.borrow_mut();

        let key = (path.as_ref().to_owned(), page_number);

        if let Some(page) = buffer_pool.get(&key) {
            Ok(Rc::clone(page))
        } else {
            let mut page: Rc<Aligned> = bytemuck::allocation::zeroed_rc();
            let buffer = Rc::get_mut(&mut page).unwrap();

            let file = OpenOptions::new().read(true).open(&path)?;

            file.read_exact_at(&mut buffer.0, (page_number * PAGE_SIZE) as u64)?;
            buffer_pool.insert(key.clone(), page.clone());
            Ok(page)
        }
    }
    pub fn write_file(
        &mut self,
        path: impl AsRef<Path>,
        mut write_next: impl FnMut(&mut Aligned) -> Result<bool, DbError>,
    ) -> Result<usize, DbError> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .create(true)
            .open(path)?;

        let mut num_pages = 0;

        let mut page_bytes: Rc<Aligned> = bytemuck::allocation::zeroed_rc();
        let buffer = Rc::get_mut(&mut page_bytes).unwrap();
        while write_next(buffer)? {
            file.write_all(&buffer.0)?;
            num_pages += 1;
            buffer.clear();
        }

        Ok(num_pages)
    }
}
