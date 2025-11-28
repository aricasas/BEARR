use std::{
    fs,
    num::NonZeroUsize,
    ops::DerefMut,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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
    fn new() -> Arc<Self> {
        bytemuck::allocation::zeroed_arc()
    }
    fn inner_mut(self: &mut Arc<Self>) -> Option<&mut [u8; PAGE_SIZE]> {
        Arc::get_mut(self).map(|a| &mut a.0)
    }
    fn clear(&mut self) {
        self.0.fill(0);
    }
}

#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
/// Some information that identifies a data file in the database.
pub struct FileId {
    pub lsm_level: usize,
    pub sst_number: usize,
}

#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
/// Some information that identifies a page of a data file in the database.
/// The page will be at the byte offset `page_number * PAGE_SIZE`.
pub struct PageId {
    pub file_id: FileId,
    pub page_number: usize,
}

#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
/// An identifier for a version of a file in the buffer pool.
pub struct BufferFileId(pub usize);

#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
/// Information that identifies a page in the buffer pool.
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

/// An abstraction over a buffer pool
/// that exposes functions for reading and writing pages to and from the file system.
///
/// TODO: properly support dirty pages for writes and deletes.
pub struct FileSystem {
    inner: Mutex<InnerFs>,
    prefix: PathBuf,
    capacity: usize,
    write_buffering: usize,
    readahead_buffering: usize,
}

struct InnerFs {
    buffer_pool: HashTable<BufferPageId, BufferPoolEntry>,
    eviction_handler: Eviction,
    file_map: FileMap,
}

struct FileMap {
    map: Vec<Vec<Option<NonZeroUsize>>>,
    counter: NonZeroUsize,
}

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
        let buffer_page_id;

        // Hold lock to check if page is in buffer pool
        {
            let mut inner_lock = self.inner.lock().unwrap(); // If lock is poisoned, this is unrecoverable
            let inner = inner_lock.deref_mut();

            buffer_page_id = inner.file_map.get_or_assign_page(page_id);

            if let Some(entry) = inner.buffer_pool.get(buffer_page_id) {
                inner.eviction_handler.touch(entry.eviction_id);
                return Ok(Arc::clone(&entry.page));
            }
        }

        // If page is not in buffer pool, fetch it from disk
        // We don't hold the lock here so other threads can do work while we wait for I/O to complete.
        // It's okay to relinquish exclusive access, because higher levels of the database ensure that
        // no other thread is modifying files while our thread is reading files.

        let PageId {
            file_id,
            page_number,
        } = page_id;
        let path = self.path(file_id);
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(path)?;
        let mut page = Aligned::new();
        let page_offset = page_number * PAGE_SIZE;
        file.read_exact_at(page.inner_mut().unwrap(), page_offset as u64)?;

        // Obtain lock again to put page in buffer pool
        {
            let mut inner_lock = self.inner.lock().unwrap();
            let inner = inner_lock.deref_mut();

            // Another thread could have inserted this page while we weren't holding the lock
            // but our implementation assumes we never try to insert a page twice.
            // So we check again whether the page is in the buffer pool.
            if let Some(entry) = inner.buffer_pool.get(buffer_page_id) {
                inner.eviction_handler.touch(entry.eviction_id);
                return Ok(Arc::clone(&entry.page));
            }

            // Our implementation never overfills the buffer pool
            assert!(inner.buffer_pool.len() <= self.capacity);

            // Evict to make room the new page
            if inner.buffer_pool.len() == self.capacity {
                inner.evict_page()?;
            }

            inner.add_new_page(Arc::clone(&page), page_id);

            Ok(page)
        }
    }

    /// Gets the page with the given ID.
    ///
    /// If it is stored in the buffer pool, it gets it from there, otherwise it will read it from disk
    /// and might evict another page from the buffer pool to make space.
    ///
    /// While performing the read, it will also read pages ahead of the one that is returned and load them
    /// to the buffer pool. This is so subsequent sequential accesses will not have to perform I/O again.
    ///
    /// The amount of readahead done is configured by the user in the `readahead_buffering` config option.
    ///
    /// Returns a reference to the bytes of the page, or an error.
    pub fn get_sequential(
        &self,
        page_id: PageId,
        // File size in pages
        file_size: usize,
    ) -> Result<Arc<Aligned>, DbError> {
        // Hold lock while checking buffer pool
        {
            let mut inner_lock = self.inner.lock().unwrap(); // If lock is poisoned, this is unrecoverable
            let inner = inner_lock.deref_mut();

            if let Some(entry) = inner
                .buffer_pool
                .get(inner.file_map.get_or_assign_page(page_id))
            {
                inner.eviction_handler.touch(entry.eviction_id);
                return Ok(Arc::clone(&entry.page));
            }
        }

        // If page not in buffer pool, release lock and performing file read
        // See reasons in FileSystem::get

        let PageId {
            file_id,
            page_number,
        } = page_id;
        let path = self.path(file_id);
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(path)?;

        // Read several pages ahead of what was asked
        let page_start = page_number;
        let page_end = (page_number + self.readahead_buffering).min(file_size);

        assert!(page_start < page_end);

        let num_pages_to_read = page_end - page_start;

        let pages_offset = page_number * PAGE_SIZE;

        let mut buffer: Vec<Aligned> = bytemuck::allocation::zeroed_vec(num_pages_to_read);

        file.read_exact_at(bytemuck::cast_slice_mut(&mut buffer), pages_offset as u64)?;

        // Obtain lock again to modify buffer pool
        {
            let mut inner_lock = self.inner.lock().unwrap();
            let inner = inner_lock.deref_mut();

            // Add pages we just read to buffer pool from farthest to closest to wanted page
            // In reverse so we don't accidentally evict the pages that we'll need sooner
            for i in (0..num_pages_to_read).rev() {
                let page_id = PageId {
                    file_id,
                    page_number: page_number + i,
                };

                if inner
                    .buffer_pool
                    .get(inner.file_map.get_or_assign_page(page_id))
                    .is_none()
                {
                    // Only add to buffer pool if not already there
                    if inner.buffer_pool.len() == self.capacity {
                        inner.evict_page()?;
                    }

                    let mut page = Aligned::new();
                    page.inner_mut().unwrap().copy_from_slice(&buffer[i].0);

                    inner.add_new_page(Arc::clone(&page), page_id);
                }
            }

            // Get wanted page from buffer pool
            // We know it's there since it's the latest one we just added
            let page_entry = inner
                .buffer_pool
                .get(inner.file_map.get_or_assign_page(page_id))
                .unwrap();

            Ok(Arc::clone(&page_entry.page))
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

        self.inner.lock().unwrap().file_map.reassign_file(file_id);

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
                    // eprintln!("write {buffer_data_end} page(s)");
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
        file_map.reassign_file(new_file_id);

        Ok(())
    }
}

impl InnerFs {
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

    pub fn add_new_page(&mut self, page: Arc<Aligned>, page_id: PageId) {
        let page_id = self.file_map.get_or_assign_page(page_id);
        let eviction_id = self.eviction_handler.insert_new(page_id);
        let entry = BufferPoolEntry { eviction_id, page };
        self.buffer_pool.insert(page_id, entry);
    }
}

impl FileMap {
    pub fn new() -> Self {
        Self {
            map: Vec::new(),
            counter: NonZeroUsize::MIN,
        }
    }

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

    fn get_or_assign_file(&mut self, file_id: FileId) -> BufferFileId {
        let (id, next) = self.access(file_id);
        let id = id.get_or_insert_with(next);
        BufferFileId(id.get())
    }

    pub fn get_or_assign_page(&mut self, page_id: PageId) -> BufferPageId {
        let PageId {
            file_id,
            page_number,
        } = page_id;
        BufferPageId {
            file_id: self.get_or_assign_file(file_id),
            page_number,
        }
    }

    pub fn reassign_file(&mut self, file_id: FileId) {
        let (id, next) = self.access(file_id);
        *id = Some(next());
    }

    pub fn unassign_file(&mut self, file_id: FileId) {
        let (id, _) = self.access(file_id);
        *id = None;
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::test_util::TestPath;

    use super::*;

    fn test_path(name: &str) -> TestPath {
        TestPath::create("file_system", name)
    }

    fn page_id(lsm_level: usize, sst_number: usize, page_number: usize) -> PageId {
        FileId {
            lsm_level,
            sst_number,
        }
        .page(page_number)
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

    #[test]
    fn test_basic() -> Result<()> {
        let path = &test_path("monad");
        let mut fs = FileSystem::new(path, 8, 4, 4)?;

        // TODO

        Ok(())
    }
}
