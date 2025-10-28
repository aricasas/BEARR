use std::{
    cell::RefCell,
    fs,
    io::Write,
    ops::DerefMut,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
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
    fn new() -> Rc<Self> {
        bytemuck::allocation::zeroed_rc()
    }
    fn inner_mut(self: &mut Rc<Self>) -> Option<&mut [u8; PAGE_SIZE]> {
        Rc::get_mut(self).map(|a| &mut a.0)
    }
    pub fn clear(&mut self) {
        self.0.fill(0);
    }
}

pub struct FileSystem {
    inner: RefCell<InnerFs>,
    capacity: usize,
    write_buffering: usize,
}

struct InnerFs {
    buffer_pool: HashTable<BufferPoolEntry>,
    eviction_handler: Eviction,
}

struct BufferPoolEntry {
    eviction_id: EvictionId,
    page: Rc<Aligned>,
}

impl FileSystem {
    pub fn new(capacity: usize, write_buffering: usize) -> Result<Self, DbError> {
        let buffer_pool = HashTable::new(capacity)?;
        let eviction_handler = Eviction::new(capacity)?;
        let inner = InnerFs {
            buffer_pool,
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
        let path = path.as_ref();

        let mut inner = self.inner.borrow_mut();
        let inner = inner.deref_mut();

        if let Some(entry) = inner.buffer_pool.get(path, page_number) {
            inner.eviction_handler.touch(entry.eviction_id);
            Ok(Rc::clone(&entry.page))
        } else {
            if inner.buffer_pool.len() == self.capacity {
                inner.evict_page()?;
            }
            inner.add_new_page(path, page_number)
        }
    }

    /// Writes a new file into disk by calling `next_page` repeatedly until it returns Err() or Ok(false).
    ///
    /// Returns an error if `next_page` has an error, or if there is some I/O error (such as the file already existing).
    /// Otherwise, returns the number of pages written.
    pub fn write_file(
        &mut self,
        path: impl AsRef<Path>,
        // Closure that writes out the next page and returns whether it wrote something (false if done)
        mut next_page: impl FnMut(&mut Aligned) -> Result<bool, DbError>,
    ) -> Result<usize, DbError> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(&path)?;

        let mut buffer: Vec<Aligned> = bytemuck::allocation::zeroed_vec(self.write_buffering);
        let mut num_pages_written = 0;
        loop {
            let end_iteration = 'write: {
                for (i, page) in buffer.iter_mut().enumerate() {
                    page.clear();
                    if next_page(page)? {
                        num_pages_written += 1;
                    } else {
                        break 'write Some(i);
                    }
                }
                None
            };
            let buffer_data_end = end_iteration.unwrap_or(buffer.len());
            if buffer_data_end > 0 {
                let bytes: &[u8] = bytemuck::cast_slice(&buffer[0..buffer_data_end]);
                file.write_all(bytes)?;
            }
            if end_iteration.is_some() {
                return Ok(num_pages_written);
            }
        }
    }
}

impl InnerFs {
    fn evict_page(&mut self) -> Result<(), DbError> {
        let chooser = self.eviction_handler.choose_victim();
        for (victim, (path, page_number)) in chooser {
            let page_number = *page_number;
            let page = &self.buffer_pool.get(path, page_number).unwrap().page;
            if Rc::strong_count(page) == 1 {
                self.buffer_pool.remove(path, page_number);
                self.eviction_handler.evict(victim);
                return Ok(());
            }
        }
        Err(DbError::Oom)
    }

    fn add_new_page(&mut self, path: &Path, page_number: usize) -> Result<Rc<Aligned>, DbError> {
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(path)?;
        let mut page = Aligned::new();
        let page_offset = page_number * PAGE_SIZE;
        file.read_exact_at(page.inner_mut().unwrap(), page_offset as u64)?;

        let eviction_id = self
            .eviction_handler
            .insert_new(path.to_path_buf(), page_number);

        let entry = BufferPoolEntry {
            eviction_id,
            page: Rc::clone(&page),
        };
        self.buffer_pool
            .insert(path.to_path_buf(), page_number, entry);

        Ok(page)
    }
}

impl Default for FileSystem {
    fn default() -> Self {
        Self::new(1, 1).unwrap()
    }
}
