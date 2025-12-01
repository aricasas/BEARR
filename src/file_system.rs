use std::{
    fs,
    num::NonZeroUsize,
    ops::{DerefMut, Range},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{
    DbError, PAGE_SIZE,
    eviction::{Eviction, EvictionId},
    hashtable::HashTable,
};

/// An aligned 4096-byte page, suitable for various transmutations.
#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
pub struct Aligned(pub [u8; PAGE_SIZE]);
impl Aligned {
    /// Returns a new zeroed page wrapped inside `Arc`.
    fn new() -> Arc<Self> {
        bytemuck::allocation::zeroed_arc()
    }

    /// Returns a mutable reference to the inner array,
    /// or None if there are other `Arc` pointers to the same array.
    fn inner_mut(self: &mut Arc<Self>) -> Option<&mut [u8; PAGE_SIZE]> {
        Arc::get_mut(self).map(|a| &mut a.0)
    }

    /// Fills the page with zeroes.
    fn clear(&mut self) {
        self.0.fill(0);
    }
}

/// Some information that identifies a data file in the database.
#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileId {
    pub lsm_level: usize,
    pub sst_number: usize,
}

/// Some information that identifies a page of a data file in the database.
/// The page will be at the byte offset `page_number * PAGE_SIZE`.
#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageId {
    pub file_id: FileId,
    pub page_number: usize,
}

/// An identifier for a version of a file in the buffer pool.
/// These need to be separate from regular file IDs
/// because writing to / deleting a file will invalidate the corresponding entries in the buffer pool.
#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferFileId(pub usize);

/// Information that identifies a page in the buffer pool.
#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferPageId {
    pub file_id: BufferFileId,
    pub page_number: usize,
}

impl FileId {
    /// Returns the filename for the corresponding file of this ID.
    pub fn name(self) -> String {
        let Self {
            lsm_level,
            sst_number,
        } = self;
        format!("data-lsm{lsm_level}-sst{sst_number}")
    }

    /// Returns the page ID for the page of this file with the given page number.
    pub fn page(self, page_number: usize) -> PageId {
        PageId {
            file_id: self,
            page_number,
        }
    }
}

impl BufferFileId {
    /// Returns the page ID in the buffer pool for the page of this file with the given page number.
    pub fn page(self, page_number: usize) -> BufferPageId {
        BufferPageId {
            file_id: self,
            page_number,
        }
    }
}

/// An abstraction over a buffer pool
/// that exposes functions for reading and writing pages to and from the file system.
///
/// CONCURRENCY CORRECTNESS:
/// It is a logic error to modify a file in one thread
/// while another thread is reading or modifying the same file.
/// Gets and scans only read files,
/// and puts, deletes, and flushes require a mutable (exclusive) reference to the database struct,
/// so this should not happen.
pub struct FileSystem {
    inner: Mutex<InnerFs>,
    prefix: PathBuf,
    capacity: usize,
    write_buffering: usize,
    readahead_buffering: usize,
}

/// The parts of the file system that are collectively kept behind a lock.
struct InnerFs {
    buffer_pool: HashTable<BufferPageId, BufferPoolEntry>,
    eviction_handler: Eviction,
    file_map: FileMap,
}

/// A lookup table for translating regular file IDs to buffer pool file IDs.
struct FileMap {
    /// `map[i][j]` gives the buffer ID for LSM level `i`, SST number j`,
    /// or None if there is no buffer ID currently assigned.
    ///
    /// Out-of-bounds indices are conceptually treated as storing None.
    ///
    /// Uses `NonZeroUsize` to save some space.
    map: Vec<Vec<Option<NonZeroUsize>>>,
    /// Starts at 1 and increments whenever a new buffer ID is needed.
    counter: NonZeroUsize,
}

/// The data stored for each page in the buffer pool.
struct BufferPoolEntry {
    eviction_id: EvictionId,
    page: Arc<Aligned>,
}

impl FileSystem {
    /// Creates and returns a new file system with an empty buffer pool.
    ///
    /// The buffer pool will have the given capacity,
    /// and writes to the file system will be buffered until the given number of pages have been accumulated.
    ///
    /// Returns an error if creation of the buffer pool or eviction handler fails.
    pub fn new(
        prefix: impl AsRef<Path>,
        capacity: usize,
        write_buffering: usize,
        readahead_buffering: usize,
    ) -> Result<Self, DbError> {
        let buffer_pool = HashTable::new(capacity)?;
        let eviction_handler = Eviction::new(capacity)?;
        let inner = InnerFs {
            buffer_pool,
            eviction_handler,
            file_map: FileMap::new(),
        };

        Ok(Self {
            inner: Mutex::new(inner),
            prefix: prefix.as_ref().to_path_buf(),
            capacity,
            write_buffering,
            readahead_buffering,
        })
    }

    /// Translates a file ID to the corresponding path for this file system.
    fn path(&self, file_id: FileId) -> PathBuf {
        self.prefix.join(file_id.name())
    }

    /// Gets the page with the given ID.
    ///
    /// If it is stored in the buffer pool, it gets it from there, otherwise it will read it from disk
    /// and might evict another page from the buffer pool to make space.
    ///
    /// Returns a reference to the bytes of the page, or an error.
    pub fn get(&self, page_id: PageId) -> Result<Arc<Aligned>, DbError> {
        let PageId {
            file_id,
            page_number,
        } = page_id;
        self.get_range(file_id, page_number..page_number + 1)
    }

    /// Gets the page with the given ID.
    ///
    /// If it is stored in the buffer pool, it gets it from there, otherwise it will read it from disk
    /// and might evict another page from the buffer pool to make space.
    ///
    /// While performing the read, it will also read pages ahead of the one that is returned and load them
    /// to the buffer pool. This is so subsequent sequential accesses will not have to perform I/O again.
    ///
    /// The total number of pages read is determined by the `readahead_buffering` configuration option.
    /// It is also limited by the file size, which should be provided in pages via the `file_size` argument.
    ///
    /// Returns a reference to the bytes of the page, or an error.
    pub fn get_sequential(
        &self,
        page_id: PageId,
        file_size: usize,
    ) -> Result<Arc<Aligned>, DbError> {
        let PageId {
            file_id,
            page_number: page_start,
        } = page_id;
        let page_end = (page_start + self.readahead_buffering).min(file_size);
        self.get_range(file_id, page_start..page_end)
    }

    /// Buffers the given range of pages for the file with the given ID,
    /// if the first page is not found in the buffer pool.
    ///
    /// Panics if the given range is empty.
    ///
    /// Returns a reference to the bytes of the first page, or an error.
    fn get_range(
        &self,
        file_id: FileId,
        page_range: Range<usize>,
    ) -> Result<Arc<Aligned>, DbError> {
        assert!(
            !page_range.is_empty(),
            "cannot read empty range of pages: {page_range:?}"
        );

        let page_start = page_range.start;
        let num_pages_to_read = page_range.len();

        // Hold lock to check if page is in buffer pool
        {
            let mut inner_lock = self.inner.lock().unwrap(); // If lock is poisoned, this is unrecoverable
            let inner = inner_lock.deref_mut();

            let buffer_page_id = inner.file_map.get_or_assign_file(file_id).page(page_start);
            if let Some(entry) = inner.buffer_pool.get(buffer_page_id) {
                inner.eviction_handler.touch(entry.eviction_id);
                return Ok(Arc::clone(&entry.page));
            }
        }

        // If page is not in buffer pool, fetch it and following pages from disk.
        // We don't hold the lock here so other threads can do work while we wait for I/O to complete.
        // This is fine as long as other threads are only reading the same file.

        let path = self.path(file_id);
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(path)?;

        let offset = page_start * PAGE_SIZE;

        let mut buffer: Vec<Aligned> = bytemuck::allocation::zeroed_vec(num_pages_to_read);

        file.read_exact_at(bytemuck::cast_slice_mut(&mut buffer), offset as u64)
            .map_err(|e| {
                DbError::IoError(format!("failed exact read {file_id:?} {page_range:?}: {e}"))
            })?;

        // Obtain lock again to put page in buffer pool
        {
            let mut inner_lock = self.inner.lock().unwrap();
            let inner = inner_lock.deref_mut();

            let buffer_file_id = inner.file_map.get_file(file_id).unwrap();

            // Add readahead pages to buffer, but don't mark them as touched in the eviction handler
            // if they happen to already be there since the application hasn't logically touched them yet
            for i in (1..num_pages_to_read).rev() {
                let buffer_page_id = buffer_file_id.page(page_start + i);
                if inner.buffer_pool.get(buffer_page_id).is_none() {
                    if inner.buffer_pool.len() == self.capacity {
                        inner.evict_page()?;
                    }

                    let mut page = Aligned::new();
                    page.inner_mut().unwrap().copy_from_slice(&buffer[i].0);

                    inner.add_new_page(Arc::clone(&page), buffer_page_id);
                }
            }

            // Add the requested page to buffer pool and mark it as touched if it happens to be there already
            // (another thread could have inserted this page while we weren't holding the lock)
            let buffer_page_id = buffer_file_id.page(page_start);
            if let Some(page_entry) = inner.buffer_pool.get(buffer_page_id) {
                inner.eviction_handler.touch(page_entry.eviction_id);
                Ok(Arc::clone(&page_entry.page))
            } else {
                if inner.buffer_pool.len() == self.capacity {
                    inner.evict_page()?;
                }

                let mut page = Aligned::new();
                page.inner_mut().unwrap().copy_from_slice(&buffer[0].0);

                inner.add_new_page(Arc::clone(&page), buffer_page_id);
                Ok(page)
            }
        }
    }

    /// Writes pages to a file starting at an offset, indicated by the page ID.
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
        &self,
        starting_page_id: PageId,
        // Closure that writes out the next page and returns whether it wrote something (false if done)
        mut next_page: impl FnMut(&mut Aligned) -> Result<bool, DbError>,
    ) -> Result<usize, DbError> {
        let PageId {
            file_id,
            page_number: starting_page_number,
        } = starting_page_id;
        let path = self.path(file_id);

        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(&path)?;

        self.inner.lock().unwrap().file_map.unassign_file(file_id);

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

    /// Deletes the file with the given ID.
    ///
    /// Panics if the file doesn't exist.
    ///
    /// Returns `DbError::IoError` if there is an I/O error.
    pub fn delete_file(&self, file_id: FileId) -> Result<(), DbError> {
        let path = self.path(file_id);

        if !path.try_exists()? {
            panic!("Cannot delete non-existent file: {file_id:?}");
        }

        self.inner.lock().unwrap().file_map.unassign_file(file_id);

        fs::remove_file(path)?;

        Ok(())
    }

    /// Changes the file with the given old ID to have the given new ID instead.
    ///
    /// Panics if a file with the old ID doesn't exist,
    /// or if a file with the new ID does exist.
    ///
    /// Returns `DbError::IoError` if there is an I/O error.
    pub fn rename_file(&self, old_file_id: FileId, new_file_id: FileId) -> Result<(), DbError> {
        let old_path = self.path(old_file_id);
        let new_path = self.path(new_file_id);

        if !old_path.try_exists()? {
            panic!("Cannot rename non-existent file: {old_file_id:?}");
        }
        if new_path.try_exists()? {
            panic!("Cannot rename to existing file: {new_file_id:?}");
        }

        fs::rename(old_path, new_path)?;

        let file_map = &mut self.inner.lock().unwrap().file_map;
        file_map.unassign_file(old_file_id);
        file_map.unassign_file(new_file_id);

        Ok(())
    }
}

impl InnerFs {
    /// Makes space in the buffer pool by evicting a page.
    ///
    /// Will only evict pages that are not referenced by another `Arc` elsewhere.
    ///
    /// Returns `DbError::Oom` if no page can be evicted
    /// due to every page in the buffer pool being referenced by another `Arc`.
    pub fn evict_page(&mut self) -> Result<(), DbError> {
        let chooser = self.eviction_handler.choose_victim();
        for (victim, page_id) in chooser {
            let page = &self.buffer_pool.get(page_id).unwrap().page;
            if Arc::strong_count(page) == 1 {
                self.buffer_pool.remove(page_id);
                self.eviction_handler.evict(victim);
                return Ok(());
            }
        }
        Err(DbError::Oom)
    }

    /// Adds the given page to the buffer pool with the given page ID key,
    /// updating the eviction handler appropriately.
    pub fn add_new_page(&mut self, page: Arc<Aligned>, page_id: BufferPageId) {
        let eviction_id = self.eviction_handler.insert_new(page_id);
        let entry = BufferPoolEntry { eviction_id, page };
        self.buffer_pool.insert(page_id, entry);
    }
}

impl FileMap {
    /// Returns a new file map with no file ID assignments made.
    pub fn new() -> Self {
        Self {
            map: Vec::new(),
            counter: NonZeroUsize::MIN,
        }
    }

    /// Returns a mutable reference to the buffer ID slot for the given regular ID,
    /// alongside a function for getting a new buffer ID.
    ///
    /// Expands `self.map` so that the relevant indices are in bounds if necessary.
    ///
    /// The two return values are grouped together for borrow checker reasons.
    fn access(
        &mut self,
        file_id: FileId,
    ) -> (&mut Option<NonZeroUsize>, impl FnOnce() -> NonZeroUsize) {
        let Self { map, counter } = self;
        let FileId {
            lsm_level,
            sst_number,
        } = file_id;
        while !(0..map.len()).contains(&lsm_level) {
            map.push(Vec::new());
        }
        let level = map.get_mut(lsm_level).unwrap();
        while !(0..level.len()).contains(&sst_number) {
            level.push(None);
        }
        let id = level.get_mut(sst_number).unwrap();
        let next = || {
            *counter = counter.checked_add(1).unwrap();
            *counter
        };
        (id, next)
    }

    /// Returns the buffer ID for the given regular ID,
    /// assigning a new buffer ID if there isn't already one.
    fn get_or_assign_file(&mut self, file_id: FileId) -> BufferFileId {
        let (id, next) = self.access(file_id);
        let id = id.get_or_insert_with(next);
        BufferFileId(id.get())
    }

    /// Returns the buffer ID for the given regular ID, if one has been assigned.
    fn get_file(&mut self, file_id: FileId) -> Option<BufferFileId> {
        let (id, _) = self.access(file_id);
        id.map(|id| BufferFileId(id.get()))
    }

    /// Removes any assigned buffer ID for the given regular ID.
    /// Should be called to invalidate entries associated with a buffer ID
    /// whenever a file with the corresponding regular ID is modified.
    pub fn unassign_file(&mut self, file_id: FileId) {
        let (id, _) = self.access(file_id);
        *id = None;
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::test_util::{TestPath, assert_panics};

    use super::*;

    fn test_path(name: &str) -> Result<TestPath> {
        let path = TestPath::create("file_system", name);
        std::fs::create_dir(&path)?;
        Ok(path)
    }

    fn write_string(fs: &FileSystem, starting_page_id: PageId, s: &str) -> Result<()> {
        let mut bytes = s.bytes();
        assert_eq!(
            fs.write_file(starting_page_id, |out| {
                Ok(bytes.next().map(|c| out.0.fill(c)).is_some())
            })?,
            s.len()
        );
        Ok(())
    }

    fn assert_page_contents(fs: &FileSystem, starting_page_id: PageId, s: &str) -> Result<()> {
        let PageId {
            file_id,
            page_number: starting_page_number,
        } = starting_page_id;
        let bytes = s.bytes();
        for (page_number, a) in (starting_page_number..).zip(bytes) {
            let page = fs.get(file_id.page(page_number))?;
            for &b in &page.0 {
                assert_eq!(a, b);
            }
        }
        Ok(())
    }

    fn assert_page_contents_sequential(
        fs: &FileSystem,
        starting_page_id: PageId,
        s: &str,
        file_size: usize,
    ) -> Result<()> {
        let PageId {
            file_id,
            page_number: starting_page_number,
        } = starting_page_id;
        let bytes = s.bytes();
        for (page_number, a) in (starting_page_number..).zip(bytes) {
            let page = fs.get_sequential(file_id.page(page_number), file_size)?;
            for &b in &page.0 {
                assert_eq!(a, b);
            }
        }
        Ok(())
    }

    fn assert_not_exists(fs: &FileSystem, page_id: PageId) {
        assert!(fs.get(page_id).is_err());
    }

    #[test]
    fn test_basic() -> Result<()> {
        let path = &test_path("basic")?;
        let fs = &FileSystem::new(path, 16, 8, 4)?;

        let file_id_a = FileId {
            lsm_level: 3,
            sst_number: 14,
        };
        let file_id_b = FileId {
            lsm_level: 1,
            sst_number: 59,
        };
        let file_id_c = FileId {
            lsm_level: 2,
            sst_number: 65,
        };

        write_string(fs, file_id_a.page("a monad ".len()), "is a monoid")?;
        write_string(fs, file_id_b.page(0), "in the ????????")?;

        for _ in 0..3 {
            assert_page_contents(fs, file_id_a.page(0), "\0\0\0\0\0\0\0\0")?;
        }
        for _ in 0..3 {
            assert_page_contents(fs, file_id_b.page("in the ".len()), "????????")?;
        }
        assert_not_exists(fs, file_id_c.page(0));

        write_string(fs, file_id_a.page(0), "a monad ")?;
        write_string(
            fs,
            file_id_b.page("in the ".len()),
            "category of endofunctors",
        )?;

        for _ in 0..3 {
            assert_page_contents_sequential(
                fs,
                file_id_a.page("a ".len()),
                "monad",
                "a monad is a monoid".len(),
            )?;
        }
        for _ in 0..3 {
            assert_page_contents_sequential(
                fs,
                file_id_a.page("a monad is a ".len()),
                "monoid",
                "a monad is a monoid".len(),
            )?;
        }
        assert_page_contents_sequential(
            fs,
            file_id_b.page(0),
            "in the category of endofunctors",
            "in the category of endofunctors".len(),
        )?;

        assert_panics(|| _ = fs.rename_file(file_id_a, file_id_b));
        assert_panics(|| _ = fs.delete_file(file_id_c));
        fs.delete_file(file_id_b)?;
        assert_panics(|| _ = fs.delete_file(file_id_b));
        assert_panics(|| _ = fs.rename_file(file_id_c, file_id_a));
        fs.rename_file(file_id_a, file_id_c)?;

        assert_not_exists(fs, file_id_a.page(1));
        assert_not_exists(fs, file_id_b.page(2));
        assert_page_contents(fs, file_id_c.page(0), "a monad is a monoid")?;

        Ok(())
    }
}
