use std::path::{Path, PathBuf};

use crate::{
    DbError, PAGE_SIZE,
    eviction::{Eviction, EvictionId},
};

#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
pub struct Aligned(pub [u8; PAGE_SIZE]);
impl Aligned {
    pub fn new() -> Box<Self> {
        Box::new(Self([0; _]))
    }
    pub fn clear(&mut self) {
        self.0 = [0; _];
    }
}

pub struct FileSystem {
    buffer_pool: HashTable<BufferPoolEntry>,
    eviction_handler: Eviction<(PathBuf, usize)>,
}

impl FileSystem {
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        let eviction_handler = Eviction::new(capacity)?;

        Ok(Self {
            buffer_pool: HashTable::new(capacity),
            eviction_handler,
        })
    }

    /// Gets the page from `path` at the byte offset `page_number * PAGE_SIZE`
    ///
    /// If it is stored in the buffer pool, it gets it from there, otherwise it will read it from disk
    /// and might evict another page from the buffer pool to make space.
    ///
    /// Returns a reference to the bytes of the page, or an error.
    pub fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Result<&Aligned, DbError> {
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
    pub fn write_file(
        &self,
        path: impl AsRef<Path>,
        // Closure that writes out the next page and returns whether it wrote something (false if done)
        next_page: impl FnMut(&mut Aligned) -> Result<bool, DbError>,
    ) -> Result<usize, DbError> {
        // Call write_next several times (configurable amount) and do a big write with several pages
        todo!()
    }
}

// I implemented this to use on the tests, but idk if i should have done smth else
impl Default for FileSystem {
    fn default() -> Self {
        Self::new(1).unwrap()
    }
}

struct BufferPoolEntry {
    eviction_id: EvictionId,
    page: Box<Aligned>,
    // TODO
}

struct HashTable<V> {
    inner: Vec<V>,
}

impl<V> HashTable<V> {
    fn new(capacity: usize) -> Self {
        todo!()
    }
    fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Option<&V> {
        todo!()
    }
    fn insert(&mut self, path: PathBuf, page_number: usize) {
        todo!()
    }
    fn remove(&mut self, path: impl AsRef<Path>, page_number: usize) -> V {
        todo!()
    }
}

fn hash_to_index(path: impl AsRef<Path>, page_number: usize, container_length: usize) -> usize {
    todo!()
}
