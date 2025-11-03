use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
};

use crate::{
    DbError, PAGE_SIZE,
    eviction::{Eviction, EvictionId},
    hashtable::HashTable,
};

#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
pub struct Aligned(pub [u8; PAGE_SIZE]);
impl Aligned {
    pub fn new() -> Rc<Self> {
        Rc::new(Self([0; _]))
    }
    pub fn clear(&mut self) {
        self.0 = [0; _];
    }
}
impl Default for Aligned {
    fn default() -> Self {
        Self([0; _])
    }
}

pub struct FileSystem {
    inner: RefCell<InnerFs>,
    capacity: usize,
    write_buffering: usize,
}

struct InnerFs {
    buffer_pool: HashTable<BufferPoolEntry>,
    eviction_handler: Eviction<(PathBuf, usize)>,
}

struct BufferPoolEntry {
    eviction_id: EvictionId,
    page: Rc<Aligned>,
}

impl FileSystem {
    pub fn new(capacity: usize, write_buffering: usize) -> Result<Self, DbError> {
        let eviction_handler = Eviction::new(capacity)?;
        let inner = InnerFs {
            buffer_pool: HashTable::new(capacity),
            eviction_handler,
        };

        Ok(Self {
            inner: RefCell::new(inner),
            capacity,
            write_buffering,
        })
    }

    /// Gets the page from `path` at the byte offset `page_number * PAGE_SIZE`
    ///
    /// If it is stored in the buffer pool, it gets it from there, otherwise it will read it from disk
    /// and might evict another page from the buffer pool to make space.
    ///
    /// Returns a reference to the bytes of the page, or an error.
    pub fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Result<Rc<Aligned>, DbError> {
        // If page is in buffer pool return it and mark as touched in eviction handler
        // If not and there is space, allocate new page, put in buffer pool
        // If not and there is no space, call eviction, get page to replaced, find it and overwrite that allocation with new page from disk

        // Remember to register new page in eviction handler
        todo!()
    }

    /// Writes a new file into disk by calling `next_page` repeatedly until it returns Err() or Ok(false).
    ///
    /// Returns an error if `next_page` has an error, or if there is some I/O error (such as the file already existing).
    /// Otherwise, returns the number of pages written.
    pub fn append(
        &mut self,
        path: impl AsRef<Path>,
        // Closure that writes out the next page and returns whether it wrote something (false if done)
        mut next_page: impl FnMut(&mut Aligned) -> Result<bool, DbError>,
    ) -> Result<usize, DbError> {
        // Call write_next several times (configurable amount) and do a big write with several pages
        let mut buffer = vec![Aligned::default(); 50];
        next_page(&mut buffer[3])?;
        let bytes: &[u8] = bytemuck::cast_slice(&buffer[0..20]);
        todo!()
    }

    pub fn write_file(
        &mut self,
        path: impl AsRef<Path>,
        starting_page_number: usize,
        // Closure that writes out the next page and returns whether it wrote something (false if done)
        mut next_page: impl FnMut(&mut Aligned) -> Result<bool, DbError>,
    ) -> Result<usize, DbError> {
        todo!()
    }
}

impl Default for FileSystem {
    fn default() -> Self {
        Self::new(1, 1).unwrap()
    }
}

