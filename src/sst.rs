use std::ops::RangeInclusive;

use crate::{
    DbError,
    bloom_filter::BloomFilter,
    btree::{BTree, BTreeIter, BTreeMetadata},
    file_system::FileId,
};

#[cfg(not(feature = "mock"))]
use crate::file_system::FileSystem;

#[cfg(feature = "mock")]
use crate::mock::FileSystem;

/// A handle to an SST (Sorted String Table) file.
///
/// An SST is an immutable, on-disk data structure that stores sorted key-value pairs.
/// It consists of:
/// - A B-tree index structure for efficient lookups
/// - A bloom filter for quick negative lookups (checking if a key is definitely not present)
/// - Metadata describing the file layout and structure
///
/// # Structure
///
/// SST Components:
/// ┌─────────────────────────────────────────────────────┐
/// │ BTreeMetadata                                       │
/// │  - File offsets (metadata, leafs, nodes, bloom)     │
/// │  - Tree depth and size information                  │
/// │  - Entry count and bloom filter parameters          │
/// ├─────────────────────────────────────────────────────┤
/// │ BloomFilter (in memory)                             │
/// │  - checks if something exists in an sst             │
/// │  - False positives possible, no false negatives     │
/// ├─────────────────────────────────────────────────────┤
/// │ FileId                                              │
/// │  - LSM level and SST number                         │
/// │  - Used to locate the file on disk                  │
/// └─────────────────────────────────────────────────────┘
///
///
/// # Usage
/// SSTs are immutable once created. They support:
/// - Point lookups via `get()`
/// - Range scans via `scan()`
/// - Bloom filter checks to avoid unnecessary disk I/O
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub struct Sst {
    /// Metadata describing the B-tree structure (offsets, depth, sizes)
    pub btree_metadata: BTreeMetadata,
    /// Identifier for locating the SST file on disk
    pub file_id: FileId,
    /// In-memory bloom filter for quick negative lookups
    pub filter: BloomFilter,
}

impl Sst {
    /// Creates a new SST file from an iterator of key-value pairs.
    ///
    /// # Process
    /// 1. Writes sorted key-value pairs to leaf pages
    /// 2. Builds a B-tree index structure over the leaves
    /// 3. Creates a bloom filter for all keys
    /// 4. Writes metadata describing the complete structure
    ///
    /// # Arguments
    /// * `key_values` - Iterator of (key, value) pairs. **Must be sorted by key.**
    /// * `n_entries_hint` - Upper bound estimate of the number of entries (for bloom filter sizing)
    /// * `bits_per_entry` - Bits per entry in bloom filter (higher = fewer false positives)
    /// * `file_id` - Identifier for the SST file (determines LSM level and file number)
    /// * `file_system` - File system to write the SST to
    ///
    /// # Returns
    /// A new `Sst` handle with metadata and bloom filter loaded in memory
    ///
    /// # Errors
    /// * `DbError` - If writing fails or if the key-value iterator returns an error
    ///
    /// # Example
    /// ```ignore
    /// let sst = Sst::create(
    ///     vec![(1, 100), (2, 200), (3, 300)].into_iter().map(Ok),
    ///     3,           // hint: 3 entries
    ///     8,           // 8 bits per entry
    ///     file_id,
    ///     &mut fs,
    /// )?;
    /// ```
    pub fn create(
        key_values: impl IntoIterator<Item = Result<(u64, u64), DbError>>,
        n_entries_hint: usize,
        bits_per_entry: usize,
        file_id: FileId,
        file_system: &mut FileSystem,
    ) -> Result<Sst, DbError> {
        let key_values = key_values.into_iter();

        let (btree_metadata, filter) = BTree::write(
            file_id,
            key_values,
            n_entries_hint,
            bits_per_entry,
            file_system,
        )?;

        Ok(Sst {
            file_id,
            btree_metadata,
            filter,
        })
    }

    /// Opens an existing SST file and loads its metadata and bloom filter into memory.
    ///
    /// # Process
    /// 1. Reads and validates metadata from page 0
    /// 2. Loads the bloom filter from disk into memory
    /// 3. Creates an SST handle for subsequent operations
    ///
    /// The actual data pages (leaves and internal nodes) remain on disk and are
    /// read on-demand during get() and scan() operations.
    ///
    /// # Arguments
    /// * `file_id` - Identifier for the SST file to open
    /// * `file_system` - File system containing the SST
    ///
    /// # Returns
    /// An `Sst` handle with metadata and bloom filter loaded in memory
    ///
    /// # Errors
    /// * `DbError::CorruptSst` - If the file has an invalid magic number or corrupted metadata
    pub fn open(file_id: FileId, file_system: &FileSystem) -> Result<Sst, DbError> {
        let (btree_metadata, filter) = BTree::open(file_id, file_system)?;

        Ok(Sst {
            file_id,
            btree_metadata,
            filter,
        })
    }

    /// Retrieves the value associated with a key.
    ///
    /// # Process
    /// 1. First checks the bloom filter - if it returns false, the key is definitely not present
    /// 2. If bloom filter returns true, performs a B-tree search
    /// 3. Returns the value if found, None otherwise
    ///
    /// # Performance
    /// The bloom filter allows us to avoid expensive disk I/O for keys that don't exist
    /// in this SST, making negative lookups very fast.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    /// * `file_system` - File system containing the SST pages
    ///
    /// # Returns
    /// * `Some(value)` - If the key exists in this SST
    /// * `None` - If the key doesn't exist (either bloom filter rejected it or tree search failed)
    ///
    /// # Errors
    /// * `DbError` - If reading pages from disk fails
    pub fn get(&self, key: u64, file_system: &FileSystem) -> Result<Option<u64>, DbError> {
        // Bloom filter check: fast negative lookup
        if !self.filter.query(key) {
            return Ok(None);
        }

        // Bloom filter says "maybe present" - do actual tree search
        BTree::get(self, key, file_system)
    }

    /// Creates an iterator for scanning a range of keys.
    ///
    /// # Process
    /// 1. Uses B-tree search to locate the starting position
    /// 2. Returns an iterator that will sequentially read entries in the range
    /// 3. Iterator lazily loads pages as needed during iteration
    ///
    /// # Arguments
    /// * `range` - Inclusive range of keys to scan (e.g., `10..=20`)
    /// * `file_system` - File system containing the SST pages
    ///
    /// # Returns
    /// A `BTreeIter` that yields `(key, value)` pairs in sorted order
    ///
    /// # Errors
    /// * `DbError` - If the initial search fails or pages cannot be read
    ///
    /// # Example
    /// ```ignore
    /// let mut iter = sst.scan(5..=15, &fs)?;
    /// while let Some((k, v)) = iter.next() {
    ///     println!("key: {}, value: {}", k?, v?);
    /// }
    /// ```
    pub fn scan<'a, 'b>(
        &'a self,
        range: RangeInclusive<u64>,
        file_system: &'b FileSystem,
    ) -> Result<BTreeIter<'a, 'b>, DbError> {
        BTreeIter::new(self, range, file_system)
    }
}

/* Tests for SSTs */
#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::test_util::TestFs;

    use super::*;

    fn test_file_id(num: usize) -> FileId {
        FileId {
            lsm_level: 0,
            sst_number: num,
        }
    }

    /// Tests creating an empty SST and verifying it's detected as corrupt.
    ///
    /// An SST with no entries should be considered corrupt since it has no valid data.
    #[test]
    fn test_create_open_sst() -> Result<()> {
        let mut fs = TestFs::new("./db/2/");

        let path = test_file_id(2);

        Sst::create(vec![], 1, 1, path, &mut fs)?;

        assert!(matches!(Sst::open(path, &fs), Err(DbError::CorruptSst)));

        Ok(())
    }

    /// Tests basic write and scan functionality with a small dataset.
    ///
    /// Verifies that:
    /// - SST creation works with sorted pairs
    /// - Scans correctly identify starting positions for different ranges
    /// - The scan iterator is positioned at the right page and item offset
    #[test]
    fn test_read_write_to_sst() -> Result<()> {
        let mut fs = TestFs::new("./db/3/");

        let path = test_file_id(3);

        Sst::create(
            [
                (1, 2),
                (3, 4),
                (5, 6),
                (7, 8),
                (9, 10),
                (11, 12),
                (13, 14),
                (15, 16),
            ]
            .into_iter()
            .map(Ok),
            8,
            8,
            path,
            &mut fs,
        )?;

        let sst = Sst::open(path, &fs)?;

        // Scan starting at 11 should begin at page 1, item 5
        let scan = sst.scan(11..=12, &fs)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 1);
        assert_eq!(scan.item_number, 5);

        // Scan starting at 2 should begin at page 1, item 1 (first item >= 2 is key 3)
        let scan = sst.scan(2..=12, &fs)?;
        println!("{} {}", scan.page_number, scan.item_number);
        assert_eq!(scan.page_number, 1);
        assert_eq!(scan.item_number, 1);

        Ok(())
    }

    /// Tests range scanning functionality.
    ///
    /// Verifies that:
    /// - The iterator returns the correct key-value pairs in order
    /// - The range is respected (only returns keys in 2..=12)
    /// - Iteration stops at the correct point
    #[test]
    fn test_scan_sst() -> Result<()> {
        let mut fs = TestFs::new("./db/4/");

        let path = test_file_id(4);

        Sst::create(
            [
                (1, 2),
                (3, 4),
                (5, 6),
                (7, 8),
                (9, 10),
                (11, 12),
                (13, 14),
                (15, 16),
            ]
            .into_iter()
            .map(Ok),
            8,
            8,
            path,
            &mut fs,
        )?;

        let sst = Sst::open(path, &fs)?;

        let mut scan = sst.scan(2..=12, &fs)?;
        assert_eq!(scan.next().unwrap()?, (3, 4));
        assert_eq!(scan.next().unwrap()?, (5, 6));
        assert_eq!(scan.next().unwrap()?, (7, 8));
        assert_eq!(scan.next().unwrap()?, (9, 10));
        assert_eq!(scan.next().unwrap()?, (11, 12));
        assert_eq!(scan.next(), None);

        Ok(())
    }

    /// Large-scale stress test with 400,000 entries.
    ///
    /// This test verifies:
    /// - SST can handle large datasets
    /// - Scanning across many pages works correctly
    /// - Page transitions during iteration are correct
    /// - All values can be retrieved accurately
    ///
    /// The test also prints when new pages are loaded, demonstrating
    /// the lazy page loading behavior of the iterator.
    #[test]
    fn test_huge_test() -> Result<()> {
        let mut fs = TestFs::new("./db/5/");

        let path = test_file_id(5);

        let mut test_vec = Vec::<(u64, u64)>::new();
        for i in 1..400_000 {
            test_vec.push((i, i));
        }

        Sst::create(test_vec.into_iter().map(Ok), 400_000, 8, path, &mut fs)?;

        let sst = Sst::open(path, &fs)?;

        let range_start = 1;
        let range_end = 4000;

        let mut scan = sst.scan(range_start..=range_end, &fs)?;

        let mut page_number = 0;
        // Verify each entry in the range is correct
        for i in range_start..range_end {
            if scan.page_number != page_number {
                page_number = scan.page_number;
                println!("New page moved to memory : {}", page_number);
            }

            assert_eq!(scan.next().unwrap()?, (i, i));
        }

        Ok(())
    }
}

