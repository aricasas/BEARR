use std::{
    cell::RefCell,
    fs,
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
/// An aligned 4096-byte page, suitable for various transmutations.
pub struct Aligned(pub [u8; PAGE_SIZE]);
impl Aligned {
    fn new() -> Rc<Self> {
        bytemuck::allocation::zeroed_rc()
    }
    fn inner_mut(self: &mut Rc<Self>) -> Option<&mut [u8; PAGE_SIZE]> {
        Rc::get_mut(self).map(|a| &mut a.0)
    }
    fn clear(&mut self) {
        self.0.fill(0);
    }
}

/// An abstraction over a buffer pool
/// that exposes functions for reading and writing pages to and from the file system.
///
/// NOTE: currently doesn't have any support for dirty pages.
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
    /// Creates and returns a new file system with an empty buffer pool.
    ///
    /// The buffer pool will have the given capacity,
    /// and writes to the file system will be buffered until the given number of pages have been accumulated.
    ///
    /// Returns an error if creation of the buffer pool or eviction handler fails.
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
            if cfg!(test) {
                // println!("get page {page_number} of {path:?}");
            }
            if inner.buffer_pool.len() == self.capacity {
                // println!("evict page");
                inner.evict_page()?;
            }
            inner.add_new_page(path, page_number)
        }
    }

    /// Writes pages to `path` starting from the byte offset `starting_page_number * PAGE_SIZE`
    ///
    /// Creates the file if it doesn't already exist,
    /// and fills the bytes before the offset with `0x00` if the file isn't long enough.
    ///
    /// Repeatedly calls `next_page` with an out argument.
    /// If `next_page` returns true, the modified out argument is written to the file.
    /// When `next_page` returns false, stops writing.
    /// Writes are buffered for efficiency.
    ///
    /// Returns an error if `next_page` has an error, or if there is some I/O error.
    /// Otherwise, returns the number of pages written by `next_page`.
    pub fn write_file(
        &mut self,
        path: impl AsRef<Path>,
        starting_page_number: usize,
        // Closure that writes out the next page and returns whether it wrote something (false if done)
        mut next_page: impl FnMut(&mut Aligned) -> Result<bool, DbError>,
    ) -> Result<usize, DbError> {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(&path)?;

        let mut buffer: Vec<Aligned> = bytemuck::allocation::zeroed_vec(self.write_buffering);
        let mut page_number_unwritten = starting_page_number;
        let mut page_number_written = page_number_unwritten;
        let mut end = false;
        loop {
            for page in &mut buffer {
                page.clear();
                if next_page(page)? {
                    page_number_unwritten += 1;
                } else {
                    end = true;
                    break;
                }
            }

            let buffer_data_end = page_number_unwritten - page_number_written;
            if buffer_data_end > 0 {
                if cfg!(test) {
                    // println!("write {buffer_data_end} page(s)");
                }
                let bytes: &[u8] = bytemuck::cast_slice(&buffer[0..buffer_data_end]);
                let offset = page_number_written * PAGE_SIZE;
                file.write_all_at(bytes, offset as u64)?;
                page_number_written = page_number_unwritten;
            }

            if end {
                return Ok(page_number_written - starting_page_number);
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
        Self::new(16, 1).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::test_path::TestPath;

    use super::*;

    fn test_path(name: &str) -> TestPath {
        TestPath::new("file_system", name)
    }

    fn write_string(
        fs: &mut FileSystem,
        path: &TestPath,
        starting_page_number: usize,
        s: &str,
    ) -> Result<()> {
        let mut bytes = s.bytes();
        assert_eq!(
            fs.write_file(path, starting_page_number, |out| {
                Ok(bytes.next().map(|c| out.0.fill(c)).is_some())
            })?,
            s.len()
        );
        Ok(())
    }

    fn assert_page_contents(
        fs: &FileSystem,
        path: &TestPath,
        starting_page_number: usize,
        s: &str,
    ) -> Result<()> {
        let bytes = s.bytes();
        for (page_number, a) in (starting_page_number..).zip(bytes) {
            let page = fs.get(path, page_number)?;
            for &b in &page.0 {
                assert_eq!(a, b);
            }
        }
        Ok(())
    }

    #[test]
    fn test_basic() -> Result<()> {
        let path = &test_path("monad");
        let mut fs = FileSystem::new(8, 4)?;

        write_string(
            &mut fs,
            path,
            "a monad ".len(),
            "is a ?????? in the category of ",
        )?;
        write_string(&mut fs, path, "".len(), "a monad ")?;
        write_string(&mut fs, path, "a monad is a ".len(), "monoid")?;

        for _ in 0..3 {
            assert_page_contents(&fs, path, "a ".len(), "monad")?;
        }
        for _ in 0..3 {
            assert_page_contents(&fs, path, "a monad is a ".len(), "monoid")?;
        }

        write_string(
            &mut fs,
            path,
            "a monad is a monoid in the category of ".len(),
            "endofunctors",
        )?;

        for _ in 0..3 {
            assert_page_contents(
                &fs,
                path,
                "a monad is a monoid in the category of endo".len(),
                "functor",
            )?;
        }

        Ok(())
    }

    #[test]
    fn test_multiple_files() -> Result<()> {
        let mut fs = FileSystem::new(2, 1)?;

        let a = &test_path("multi-a");
        let b = &test_path("multi-b");
        let c = &test_path("multi-c");

        write_string(&mut fs, a, 0, "a")?;
        write_string(&mut fs, b, 0, "b")?;
        write_string(&mut fs, c, 0, "c")?;

        assert_page_contents(&fs, a, 0, "a")?;
        assert_page_contents(&fs, b, 0, "b")?;
        assert_page_contents(&fs, c, 0, "c")?;
        assert_page_contents(&fs, b, 0, "b")?;
        assert_page_contents(&fs, a, 0, "a")?;
        assert_page_contents(&fs, c, 0, "c")?;
        assert_page_contents(&fs, a, 0, "a")?;
        assert_page_contents(&fs, b, 0, "b")?;

        Ok(())
    }
}
