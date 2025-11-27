use std::{ops::RangeInclusive, sync::Arc};

use crate::{
    DbError, PAGE_SIZE,
    bloom_filter::BloomFilter,
    file_system::{Aligned, FileId},
    sst::Sst,
};

const PAIRS_PER_CHUNK: usize = (PAGE_SIZE - 8) / 16;
const PADDING: usize = PAGE_SIZE - 8 - PAIRS_PER_CHUNK * 16;

#[cfg(not(feature = "mock"))]
use crate::file_system::FileSystem;

#[cfg(feature = "mock")]
use crate::mock::FileSystem;

#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
/// The smallest granuallity of storage we use in our system
/// It has a length, it has a bunch of pairs and since it is aligned to PAGE_SIZE,
/// the rest is a padding
struct Page {
    /// Number of pairs stored in this page
    length: u64,
    pairs: [[u64; 2]; PAIRS_PER_CHUNK],
    padding: [u8; PADDING],
}

impl Default for Page {
    fn default() -> Self {
        Self {
            length: 0,
            pairs: [Default::default(); _],
            padding: [Default::default(); _],
        }
    }
}

/// Making sure our page is the size of PAGE_SIZE
const _: () = assert!(size_of::<Page>() == PAGE_SIZE);

/// A magic number that is used to check the validity of an SST
pub const BEAR_MAGIC: u64 = 0xBEA22;

#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug)]
/// Metadata struct for each sst
pub struct BTreeMetadata {
    pub magic: u64,        // This is used to check the validity of the metadata
    pub leafs_offset: u64, // Where the leafs start from
    pub nodes_offset: u64, // Where the nodes start from
    pub bloom_offset: u64, // Where the bloom filter starts from
    pub tree_depth: u64,   // Number of layers in the internal nodes
    pub size: u64,         // Entire file size in pages
    pub bloom_size: u64,   // Bloom filter(including hash functions and bitmap) size in bytes
    pub num_hashes: u64,   // Number of hash functions for the bloom filter
    pub n_entries: u64,    // Number of entries in the SST
}

#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug)]
/// The struct that points to the actuall metadata
struct MetadataPage {
    metadata: BTreeMetadata,
    padding: [u8; PAGE_SIZE - size_of::<BTreeMetadata>()],
}

impl Default for MetadataPage {
    fn default() -> Self {
        Self {
            metadata: BTreeMetadata {
                magic: BEAR_MAGIC,
                leafs_offset: LEAF_OFFSET,
                nodes_offset: 0x1000,
                bloom_offset: 0x1000,
                tree_depth: 0,
                size: 0,
                bloom_size: 0,
                num_hashes: 0,
                n_entries: 0,
            },
            padding: [Default::default(); _],
        }
    }
}

/// Making sure the Metadata page is of size PAGE_SIZE
const _: () = assert!(size_of::<MetadataPage>() == PAGE_SIZE);

const LEAF_OFFSET: u64 = 1;
pub const METADATA_OFFSET: u64 = 0;
const KEYS_PER_NODE: usize = (PAGE_SIZE - 8) / 16;
/*
 * Structure of a Btree Node (page aligned) :
 *      Number of Keys in Node
 *      | key1  :  Offset1  |
 *      | key2  :  Offset2  |
 *      | key3  :  Offset3  |
 *             ...
 *      00000000000000000000...
 * */
type Node = Page;
type Leaf = Page;

/// Btree iterator used to iterate pages of the SST
pub struct BTreeIter<'a, 'b> {
    sst: &'a Sst,
    file_system: &'b FileSystem,
    buffered_page: Option<Arc<Page>>,
    pub page_number: usize,
    pub item_number: usize,
    range: RangeInclusive<u64>,
    ended: bool,
}

impl<'a, 'b> Iterator for BTreeIter<'a, 'b> {
    type Item = Result<(u64, u64), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.go_to_next()
    }
}

// BTree iterator functions
impl<'a, 'b> BTreeIter<'a, 'b> {
    /// Check the validity of our interator range, if there exsits
    /// any tree in that range, return the page number and the item number inside it
    /// that corresponds to the smallest element bigger that the start of the range
    pub fn new(
        sst: &'a Sst,
        range: RangeInclusive<u64>,
        file_system: &'b FileSystem,
    ) -> Result<Self, DbError> {
        if range.start() > range.end() {
            return Err(DbError::InvalidScanRange);
        }

        let res = BTree::search(sst, *range.start(), file_system)?;

        if let Some(Ok((page_number, item_number)) | Err((page_number, item_number))) = res {
            Ok(Self {
                sst,
                file_system,
                buffered_page: None,
                page_number,
                item_number,
                range,
                ended: false,
            })
        } else {
            Ok(Self {
                sst,
                file_system,
                buffered_page: None,
                page_number: 0,
                item_number: 0,
                range,
                ended: true,
            })
        }
    }

    /// Get the next element and if needed go to the next page
    fn go_to_next(&mut self) -> Option<Result<(u64, u64), DbError>> {
        if self.ended {
            return None;
        }

        if self.buffered_page.is_none() {
            let page_bytes = self
                .file_system
                .get(self.sst.file_id.page(self.page_number));

            let buffered_page: Arc<Page> = match page_bytes {
                Ok(bytes) => bytemuck::cast_arc(bytes),
                Err(e) => return Some(Err(e)),
            };
            self.buffered_page = Some(buffered_page)
        }

        let buffered_page = self.buffered_page.as_ref().unwrap();

        let [key, value] = buffered_page.pairs[self.item_number];
        let item = (key, value);

        if &key > self.range.end() {
            self.ended = true;
            return None;
        }

        self.item_number += 1;

        if self.item_number < buffered_page.length as usize {
            return Some(Ok(item));
        }

        // Have to buffer a new page
        self.page_number += 1;
        self.item_number = 0;
        self.buffered_page = None;

        if self.page_number >= self.sst.btree_metadata.nodes_offset as usize {
            // EOF
            self.ended = true;
        }

        Some(Ok(item))
    }
}

type SearchResult = Result<(usize, usize), (usize, usize)>;

pub struct BTree {}

impl BTree {
    /// Creates a static B-tree SST (Sorted String Table) file with the following layout:
    ///
    /// ```text
    /// SST File Layout:
    /// ┌─────────────────────────────────────────────────────────────┐
    /// │ Page 0: METADATA                                            │
    /// │  - Magic number, offsets, tree depth, sizes                 │
    /// ├─────────────────────────────────────────────────────────────┤
    /// │ Pages 1..nodes_offset: LEAF NODES                           │
    /// │  - Sorted key-value pairs (actual data)                     │
    /// │  - Each leaf contains up to KEYS_PER_NODE pairs             │
    /// ├─────────────────────────────────────────────────────────────┤
    /// │ Pages nodes_offset..bloom_offset: INTERNAL NODES            │
    /// │  - Tree structure for navigation                            │
    /// │  - Each node contains (largest_key, page_number) pairs      │
    /// │  - Bottom internal nodes point to leaf pages                │
    /// │  - Upper nodes point to lower internal nodes                │
    /// ├─────────────────────────────────────────────────────────────┤
    /// │ Pages bloom_offset..end: BLOOM FILTER                       │
    /// │  - Probabilistic membership test for keys                   │
    /// │  - Multiple hash functions for low false positive rate      │
    /// └─────────────────────────────────────────────────────────────┘
    /// ```
    ///
    /// # Arguments
    /// * `file_id` - Identifier for the SST file
    /// * `pairs` - Iterator of (key, value) pairs (must be sorted by key)
    /// * `n_entries_hint` - Estimated number of entries for bloom filter sizing
    /// * `bits_per_entry` - Bloom filter bits per entry (affects false positive rate)
    /// * `file_system` - File system to write pages to
    ///
    /// # Returns
    /// * `BTreeMetadata` - Metadata describing the tree structure and offsets
    /// * `BloomFilter` - The constructed bloom filter for quick negative lookups
    pub fn write(
        file_id: FileId,
        mut pairs: impl Iterator<Item = Result<(u64, u64), DbError>>,
        n_entries_hint: usize,
        bits_per_entry: usize,
        file_system: &mut FileSystem,
    ) -> Result<(BTreeMetadata, BloomFilter), DbError> {
        let mut nodes_offset: u64;
        let mut largest_keys: Vec<u64> = Vec::new();
        let mut largest_pages: Vec<u64> = Vec::new();

        let mut leaf_count: u64 = 0;

        let mut filter = BloomFilter::empty(n_entries_hint, bits_per_entry);
        let mut n_entries = 0;

        // Closure to write leaf pages containing actual key-value pairs.
        // Each leaf is filled with pairs from the iterator until full.
        // Tracks the largest key in each leaf for building the index structure.
        let write_next_leaf = |page_bytes: &mut Aligned| {
            let leaf: &mut Leaf = bytemuck::cast_mut(page_bytes);
            leaf.length = 0;
            for (pair, k_v) in leaf.pairs.iter_mut().zip(&mut pairs) {
                match k_v {
                    Ok((k, v)) => {
                        filter.insert(k);
                        n_entries += 1;

                        *pair = [k, v];
                    }
                    Err(e) => return Err(e),
                }
                leaf.length += 1;
            }

            // Push the largest key in a page to the largest keys vector
            if leaf.length > 0 {
                largest_keys.push(leaf.pairs[(leaf.length - 1) as usize][0]);
                largest_pages.push(leaf_count);
                leaf_count += 1;
            }
            Ok(leaf.length > 0)
        };

        // Write all leaf pages starting at LEAF_OFFSET
        nodes_offset =
            file_system.write_file(file_id.page(LEAF_OFFSET as usize), write_next_leaf)? as u64;
        nodes_offset += LEAF_OFFSET;

        // Construct the B-tree index structure in memory from the largest keys.
        // This creates a hierarchical index where each level helps navigate to the correct page.
        let btree = create_tree(largest_keys, largest_pages, KEYS_PER_NODE);
        let tree_depth = btree.len() as u64;

        let mut btree_itter = btree.into_iter().flatten();

        // Closure to write internal node pages.
        // Each node contains (key, page_number) pairs for navigation.
        let write_next_btree_page = |page_bytes: &mut Aligned| {
            let node: &mut Node = bytemuck::cast_mut(page_bytes);
            node.length = 0;
            let Some(page_iter) = btree_itter.next() else {
                return Ok(false);
            };
            for (pair, k_v) in node.pairs.iter_mut().zip(page_iter.into_iter()) {
                *pair = k_v.into();
                node.length += 1;
            }

            Ok(node.length > 0)
        };

        // Write internal nodes after the leaf pages
        let nodes_written = file_system
            .write_file(file_id.page(nodes_offset as usize), write_next_btree_page)?
            as u64;

        let num_hashes = filter.hash_functions.len() as u64;
        let mut bloom_bytes_iter = filter.turn_to_bytes().into_iter();

        let bloom_offset = nodes_written + nodes_offset;
        let mut bloom_size = 0;

        // Closure to write bloom filter pages.
        // The bloom filter allows quick negative lookups (if a key is definitely not present).
        let write_next_bloom_page = |page_bytes: &mut Aligned| {
            let mut page_length: u64 = 0;
            for (dest, src) in page_bytes.0.iter_mut().zip(&mut bloom_bytes_iter) {
                *dest = src;
                page_length += 1;
            }
            bloom_size += page_length;
            Ok(page_length > 0)
        };

        // Write bloom filter after the internal nodes
        file_system.write_file(file_id.page(bloom_offset as usize), write_next_bloom_page)?;
        let file_size = bloom_offset + bloom_size;

        // Create metadata structure with all offsets and sizes
        let btree_metadata = BTreeMetadata {
            magic: BEAR_MAGIC,
            leafs_offset: LEAF_OFFSET,
            nodes_offset,
            bloom_offset,
            tree_depth,
            size: file_size,
            bloom_size,
            num_hashes,
            n_entries,
        };

        let mut write_metadata = 0;
        let write_metadata = |page_bytes: &mut Aligned| {
            write_metadata += 1;
            let metadata_page: &mut MetadataPage = bytemuck::cast_mut(page_bytes);
            metadata_page.metadata = btree_metadata;

            Ok(write_metadata == 1)
        };

        // Write metadata at page 0 (METADATA_OFFSET)
        file_system.write_file(file_id.page(METADATA_OFFSET as usize), write_metadata)?;

        Ok((btree_metadata, filter))
    }

    /// Opens an existing SST file and loads its metadata and bloom filter.
    ///
    /// # Process
    /// 1. Reads and validates metadata from page 0
    /// 2. Validates magic number and basic sanity checks
    /// 3. Loads bloom filter from the pages specified in metadata
    /// 4. Reconstructs the BloomFilter object from raw bytes
    ///
    /// # Arguments
    /// * `file_id` - Identifier for the SST file to open
    /// * `file_system` - File system to read pages from
    ///
    /// # Returns
    /// * `BTreeMetadata` - The tree's metadata (offsets, sizes, depth)
    /// * `BloomFilter` - The reconstructed bloom filter
    ///
    /// # Errors
    /// * `DbError::CorruptSst` - If magic number is wrong or offsets are invalid
    pub fn open(
        file_id: FileId,
        file_system: &FileSystem,
    ) -> Result<(BTreeMetadata, BloomFilter), DbError> {
        // Read metadata from page 0
        let metadata_page = file_system.get(file_id.page(METADATA_OFFSET as usize))?;
        let metadata_page: Arc<MetadataPage> = bytemuck::cast_arc(metadata_page);
        let metadata = metadata_page.metadata;

        // Validate magic number
        if metadata.magic != BEAR_MAGIC {
            return Err(DbError::CorruptSst);
        }
        // Sanity check: nodes must come after leafs
        if metadata.nodes_offset <= metadata.leafs_offset {
            return Err(DbError::CorruptSst);
        }

        let bloom_offset = metadata.bloom_offset;
        let bloom_size = metadata.bloom_size;
        let num_hashes = metadata.num_hashes;

        let bloom_pages_num = bloom_size.div_ceil(PAGE_SIZE as u64);

        // Read bloom filter bytes from all its pages
        let mut bloom_vec: Vec<u8> = vec![];
        for page in 0..bloom_pages_num {
            let bloom_page = file_system.get(file_id.page((bloom_offset + page) as usize))?;
            // Handle partial last page
            let end = if page == bloom_pages_num - 1 {
                (bloom_size % (PAGE_SIZE as u64)) as usize
            } else {
                PAGE_SIZE
            };

            bloom_vec.extend_from_slice(&bloom_page.0[0..end]);
        }

        // Reconstruct bloom filter from bytes
        let filter = BloomFilter::from_bytes(&bloom_vec, num_hashes as usize);

        Ok((metadata, filter))
    }

    /// Retrieves the value associated with a key from the SST.
    ///
    /// # Process
    /// 1. Uses `search()` to locate the key in the B-tree structure
    /// 2. If found, reads the leaf page and extracts the value
    /// 3. Returns None if key doesn't exist
    ///
    /// # Arguments
    /// * `sst` - The SST metadata and identifiers
    /// * `key` - The key to look up
    /// * `file_system` - File system to read pages from
    ///
    /// # Returns
    /// * `Some(value)` if the key exists
    /// * `None` if the key doesn't exist
    pub fn get(sst: &Sst, key: u64, file_system: &FileSystem) -> Result<Option<u64>, DbError> {
        let res = BTree::search(sst, key, file_system)?;
        let Some(res) = res else { return Ok(None) };

        let Ok((page_number, item_number)) = res else {
            return Ok(None);
        };

        // Read the leaf page and extract the value
        let leaf_page = file_system.get(sst.file_id.page(page_number))?;
        let leaf_node: Arc<Leaf> = bytemuck::cast_arc(leaf_page);

        Ok(Some(leaf_node.pairs[item_number][1]))
    }

    /// Searches for a key in the B-tree using tree navigation (non-binary search version).
    ///
    /// # Algorithm
    /// 1. Start at root node
    /// 2. Binary search within the node to find which child to follow
    /// 3. Repeat until reaching a leaf page
    /// 4. Binary search within the leaf to find the exact key
    ///
    /// # Arguments
    /// * `sst` - The SST metadata and identifiers
    /// * `key` - The key to search for
    /// * `file_system` - File system to read pages from
    ///
    /// # Returns
    /// * `None` - Key is outside the range of this SST
    /// * `Some(Ok((page, index)))` - Exact match found at this position
    /// * `Some(Err((page, index)))` - Key not found, but this is where it would be inserted
    #[cfg(not(feature = "binary_search"))]
    fn search(
        sst: &Sst,
        key: u64,
        file_system: &FileSystem,
    ) -> Result<Option<SearchResult>, DbError> {
        let nodes_offset = sst.btree_metadata.nodes_offset;
        let leafs_offset = sst.btree_metadata.leafs_offset;
        let tree_depth = sst.btree_metadata.tree_depth;

        // Check if key is beyond the maximum key in the tree
        let root_page = file_system.get(sst.file_id.page(nodes_offset as usize))?;
        let root_node: Arc<Node> = bytemuck::cast_arc(root_page);
        if root_node.pairs[(root_node.length - 1) as usize][0] < key {
            return Ok(None);
        }

        let mut current_node = root_node;

        let mut node_number: u64 = 0;
        let mut page_number: u64;
        let mut idx: usize;

        // Navigate through internal nodes to find the correct leaf
        for level in 0..tree_depth {
            let sub_vec: &[[u64; 2]] =
                &current_node.as_ref().pairs[0..current_node.length as usize];

            // Binary search finds the first key >= search key
            let (Ok(i) | Err(i)) = sub_vec.binary_search_by_key(&key, |x| x[0]);
            idx = i;
            node_number = current_node.pairs[idx][1];

            if level == tree_depth - 1 {
                break;
            }

            // Load next level node
            page_number = node_number + nodes_offset;
            let current_page = file_system.get(sst.file_id.page(page_number as usize))?;
            current_node = bytemuck::cast_arc(current_page);
        }

        // Search within the target leaf page
        page_number = leafs_offset + node_number;
        let leaf_page = file_system.get(sst.file_id.page(page_number as usize))?;
        let leaf: Arc<Leaf> = bytemuck::cast_arc(leaf_page);
        let sub_vec: &[[u64; 2]] = &leaf.as_ref().pairs[0..leaf.length as usize];

        let found_exact;

        idx = match sub_vec.binary_search_by_key(&key, |x| x[0]) {
            Ok(i) => {
                found_exact = true;
                i
            }
            Err(i) => {
                found_exact = false;
                i
            }
        };

        let page_number = page_number as usize;

        if found_exact {
            Ok(Some(Ok((page_number, idx))))
        } else {
            Ok(Some(Err((page_number, idx))))
        }
    }

    /// Searches for a key using binary search over leaf pages (alternative implementation).
    ///
    /// # Algorithm
    /// Instead of traversing the tree structure, this performs a binary search
    /// directly over the leaf pages, which can be more efficient when the tree
    /// is shallow or when doing random access patterns.
    ///
    /// 1. Binary search over leaf page numbers to find the page that could contain the key
    /// 2. Binary search within that leaf page to find the exact key
    ///
    /// # Arguments
    /// * `sst` - The SST metadata and identifiers
    /// * `key` - The key to search for
    /// * `file_system` - File system to read pages from
    ///
    /// # Returns
    /// * `None` - Key is outside the range of this SST
    /// * `Some(Ok((page, index)))` - Exact match found at this position
    /// * `Some(Err((page, index)))` - Key not found, but this is where it would be inserted
    #[cfg(feature = "binary_search")]
    fn search(
        sst: &Sst,
        key: u64,
        file_system: &FileSystem,
    ) -> Result<Option<SearchResult>, DbError> {
        let nodes_offset = sst.btree_metadata.nodes_offset;
        let leafs_offset = sst.btree_metadata.leafs_offset;

        // Check if key is beyond the maximum key in the tree
        let root_page = file_system.get(sst.file_id.page(nodes_offset as usize))?;
        let root_node: Arc<Node> = bytemuck::cast_arc(root_page);
        if root_node.pairs[(root_node.length - 1) as usize][0] < key {
            return Ok(None);
        }

        let mut start_page_num = leafs_offset;
        let mut end_page_num = nodes_offset - 1;
        let mut page_number: usize;

        // Binary search over leaf pages to find the right page
        loop {
            page_number = (start_page_num + end_page_num) as usize / 2;
            if page_number == start_page_num as usize {
                break;
            }
            let middle_page = file_system.get(sst.file_id.page(page_number))?;
            let leaf: Arc<Leaf> = bytemuck::cast_arc(middle_page);

            // Check if key is before this page's range
            if key < leaf.pairs[0][0] {
                end_page_num = page_number as u64;
            }
            // Check if key is after this page's range
            else if key > leaf.pairs[(leaf.length - 1) as usize][0] {
                start_page_num = page_number as u64;
            }
            // Key is within this page's range
            else {
                break;
            }
        }

        // Search within the target leaf page
        let leaf_page = file_system.get(sst.file_id.page(page_number))?;
        let leaf: Arc<Leaf> = bytemuck::cast_arc(leaf_page);
        let found_exact;

        let sub_vec: &[[u64; 2]] = &leaf.as_ref().pairs[0..leaf.length as usize];
        let idx = match sub_vec.binary_search_by_key(&key, |x| x[0]) {
            Ok(i) => {
                found_exact = true;
                i
            }
            Err(i) => {
                found_exact = false;
                i
            }
        };

        if found_exact {
            Ok(Some(Ok((page_number, idx))))
        } else {
            Ok(Some(Err((page_number, idx))))
        }
    }
}

/// Helper function that constructs an in-memory B-tree index structure.
///
/// # Algorithm
/// Given the largest keys from each leaf page, builds a hierarchical index:
///
/// ```text
/// Example with keys [3, 6, 8, 10, 13, 15] and KEYS_PER_NODE=3:
///
/// Level 0 (Root):     [8:1, 15:2]
///                       /        \
/// Level 1:        [3:0, 6:1, 8:2]  [10:3, 13:4, 15:5]
///                    |   |   |       |    |    |
/// Leafs:          [1,2,3][4,5,6][7,8] [9,10][11,12,13][14,15]
/// ```
///
/// Each entry is (largest_key_in_subtree, page_number).
/// The tree is built bottom-up by repeatedly grouping nodes into parents.
///
/// # Arguments
/// * `btree_keys` - Largest key from each leaf page (sorted)
/// * `leaf_pages` - Page number for each leaf (corresponds to btree_keys)
/// * `n` - Maximum entries per node (KEYS_PER_NODE)
///
/// # Returns
/// A Vec of levels, where each level is a Vec of pages, and each page
/// contains (key, page_number) pairs. Ordered from root to leaves.
fn create_tree(btree_keys: Vec<u64>, leaf_pages: Vec<u64>, n: usize) -> Vec<Vec<Vec<(u64, u64)>>> {
    assert_eq!(btree_keys.len(), leaf_pages.len());

    // Build forward pyramid: group keys into chunks, taking largest from each chunk
    let mut forward = vec![];
    let mut current = btree_keys.clone();
    loop {
        let chunks: Vec<Vec<u64>> = current.chunks(n).map(|chunk| chunk.to_vec()).collect();

        if chunks.len() <= 1 {
            forward.push(chunks);
            break;
        }

        forward.push(chunks.clone());
        // Next level uses the largest key from each chunk
        current = chunks.iter().map(|chunk| *chunk.last().unwrap()).collect();
    }

    // Reverse to go from root to leaves
    forward.reverse();

    let mut result = vec![];
    let mut next_id = 1;

    for (level_idx, level) in forward.iter().enumerate() {
        if level_idx == forward.len() - 1 {
            // Bottom level: map keys to actual leaf page numbers
            let leaf_chunks: Vec<Vec<u64>> =
                leaf_pages.chunks(n).map(|chunk| chunk.to_vec()).collect();
            let bottom: Vec<Vec<(u64, u64)>> = level
                .iter()
                .enumerate()
                .map(|(i, chunk)| {
                    chunk
                        .iter()
                        .enumerate()
                        .map(|(j, &v)| (v, leaf_chunks[i][j]))
                        .collect()
                })
                .collect();
            result.push(bottom);
        } else {
            // Internal levels: assign sequential page IDs
            let with_ids: Vec<Vec<(u64, u64)>> = level
                .iter()
                .map(|chunk| {
                    chunk
                        .iter()
                        .map(|&v| {
                            let id = next_id;
                            next_id += 1;
                            (v, id)
                        })
                        .collect()
                })
                .collect();
            result.push(with_ids);
        }
    }
    result
}
