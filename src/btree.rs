use std::{ops::RangeInclusive, sync::Arc};

use crate::{
    DbError, PAGE_SIZE,
    bloom_filter::BloomFilter,
    file_system::FileSystem,
    file_system::{Aligned, FileId},
    sst::Sst,
};

const PAIRS_PER_CHUNK: usize = (PAGE_SIZE - 8) / 16;
const PADDING: usize = PAGE_SIZE - 8 - PAIRS_PER_CHUNK * 16;

#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
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

const _: () = assert!(size_of::<Page>() == PAGE_SIZE);

pub const BEAR_MAGIC: u64 = 0xBEA22;

#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug)]
pub struct BTreeMetadata {
    pub magic: u64, // This is used to check the validity of the metadata
    pub leafs_offset: u64,
    pub nodes_offset: u64,
    pub bloom_offset: u64,
    pub tree_depth: u64, // Number of layers in the internal nodes
    pub size: u64,       // Entire file size in pages
    pub bloom_size: u64, // Bloom filter(including hash functions and bitmap) size in bytes
    pub num_hashes: u64,
    pub n_entries: u64,
}

#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug)]
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

// BTree iterator
impl<'a, 'b> BTreeIter<'a, 'b> {
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
    /// Creates a static B tree in a
    /// Returns a Vec containing the paths to all the levels
    pub fn write(
        file_id: FileId,
        mut pairs: impl Iterator<Item = Result<(u64, u64), DbError>>,
        n_entries_hint: usize,
        bits_per_entry: usize,
        file_system: &FileSystem,
    ) -> Result<(BTreeMetadata, BloomFilter), DbError> {
        let mut nodes_offset: u64;
        let mut largest_keys: Vec<u64> = Vec::new();
        let mut largest_pages: Vec<u64> = Vec::new();

        let mut leaf_count: u64 = 0;

        let mut filter = BloomFilter::empty(n_entries_hint, bits_per_entry);
        let mut n_entries = 0;

        // leaf write closure trait
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

        nodes_offset =
            file_system.write_file(file_id.page(LEAF_OFFSET as usize), write_next_leaf)? as u64;
        nodes_offset += LEAF_OFFSET;

        // Construct the Btree in Memory
        let btree = create_tree(largest_keys, largest_pages, KEYS_PER_NODE);
        let tree_depth = btree.len() as u64;

        /*
         *
         * iterator([3:1 6:2 8:3] [1:1 2:2 3:8] [4:3 5:6 6:9] [7:10 8:11])
         * */

        let mut btree_itter = btree.into_iter().flatten();

        // btree write closure trait
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

        /* leafs --> nodes --> filter --> metadata */

        let nodes_written = file_system
            .write_file(file_id.page(nodes_offset as usize), write_next_btree_page)?
            as u64;

        let num_hashes = filter.hash_functions.len() as u64;
        let mut bloom_bytes_iter = filter.turn_to_bytes().into_iter();

        let bloom_offset = nodes_written + nodes_offset;
        let mut bloom_size = 0;
        // Bloom filter write closure trait
        let write_next_bloom_page = |page_bytes: &mut Aligned| {
            let mut page_length: u64 = 0;
            for (dest, src) in page_bytes.0.iter_mut().zip(&mut bloom_bytes_iter) {
                *dest = src;
                page_length += 1;
            }
            bloom_size += page_length;
            Ok(page_length > 0)
        };

        file_system.write_file(file_id.page(bloom_offset as usize), write_next_bloom_page)?;
        let file_size = bloom_offset + bloom_size;

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

        file_system.write_file(file_id.page(METADATA_OFFSET as usize), write_metadata)?;

        Ok((btree_metadata, filter))
    }

    pub fn open(
        file_id: FileId,
        file_system: &FileSystem,
    ) -> Result<(BTreeMetadata, BloomFilter), DbError> {
        let metadata_page = file_system.get(file_id.page(METADATA_OFFSET as usize))?;
        let metadata_page: Arc<MetadataPage> = bytemuck::cast_arc(metadata_page);
        let metadata = metadata_page.metadata;

        if metadata.magic != BEAR_MAGIC {
            return Err(DbError::CorruptSst);
        }
        if metadata.nodes_offset <= metadata.leafs_offset {
            return Err(DbError::CorruptSst);
        }

        let bloom_offset = metadata.bloom_offset;
        let bloom_size = metadata.bloom_size;
        let num_hashes = metadata.num_hashes;

        let bloom_pages_num = bloom_size.div_ceil(PAGE_SIZE as u64);

        let mut bloom_vec: aligned_vec::AVec<u8, aligned_vec::ConstAlign<4>> =
            aligned_vec::AVec::new(4);
        for page in 0..bloom_pages_num {
            let bloom_page = file_system.get(file_id.page((bloom_offset + page) as usize))?;
            let end = if page == bloom_pages_num - 1 {
                (bloom_size % (PAGE_SIZE as u64)) as usize
            } else {
                PAGE_SIZE
            };

            bloom_vec.extend_from_slice(&bloom_page.0[0..end]);
        }

        let filter = BloomFilter::from_bytes(&bloom_vec, num_hashes as usize);

        Ok((metadata, filter))
    }

    pub fn get(sst: &Sst, key: u64, file_system: &FileSystem) -> Result<Option<u64>, DbError> {
        let res = BTree::search(sst, key, file_system)?;
        let Some(res) = res else { return Ok(None) };

        let Ok((page_number, item_number)) = res else {
            return Ok(None);
        };

        let leaf_page = file_system.get(sst.file_id.page(page_number))?;
        let leaf_node: Arc<Leaf> = bytemuck::cast_arc(leaf_page);

        Ok(Some(leaf_node.pairs[item_number][1]))
    }

    #[cfg(not(feature = "binary_search"))]
    fn search(
        sst: &Sst,
        key: u64,
        file_system: &FileSystem,
    ) -> Result<Option<SearchResult>, DbError> {
        let nodes_offset = sst.btree_metadata.nodes_offset;
        let leafs_offset = sst.btree_metadata.leafs_offset;
        let tree_depth = sst.btree_metadata.tree_depth;

        let root_page = file_system.get(sst.file_id.page(nodes_offset as usize))?;
        let root_node: Arc<Node> = bytemuck::cast_arc(root_page);
        assert_ne!(root_node.length, 0);
        if root_node.pairs[(root_node.length - 1) as usize][0] < key {
            return Ok(None);
        }

        let mut current_node = root_node;

        let mut node_number: u64 = 0;
        let mut page_number: u64;
        let mut idx: usize;
        for level in 0..tree_depth {
            let sub_vec: &[[u64; 2]] =
                &current_node.as_ref().pairs[0..current_node.length as usize];

            let (Ok(i) | Err(i)) = sub_vec.binary_search_by_key(&key, |x| x[0]);
            idx = i;
            node_number = current_node.pairs[idx][1];

            if level == tree_depth - 1 {
                break;
            }

            page_number = node_number + nodes_offset;
            let current_page = file_system.get(sst.file_id.page(page_number as usize))?;
            current_node = bytemuck::cast_arc(current_page);
        }

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

    #[cfg(feature = "binary_search")]
    fn search(
        sst: &Sst,
        key: u64,
        file_system: &FileSystem,
    ) -> Result<Option<SearchResult>, DbError> {
        let nodes_offset = sst.btree_metadata.nodes_offset;
        let leafs_offset = sst.btree_metadata.leafs_offset;

        let root_page = file_system.get(sst.file_id.page(nodes_offset as usize))?;
        let root_node: Arc<Node> = bytemuck::cast_arc(root_page);
        if root_node.pairs[(root_node.length - 1) as usize][0] < key {
            return Ok(None);
        }

        let mut start_page_num = leafs_offset;
        let mut end_page_num = nodes_offset - 1;
        let mut page_number: usize;

        loop {
            page_number = (start_page_num + end_page_num) as usize / 2;
            let middle_page = file_system.get(sst.file_id.page(page_number))?;
            let leaf: Arc<Leaf> = bytemuck::cast_arc(middle_page);

            if key < leaf.pairs[0][0] {
                if page_number == start_page_num as usize {
                    page_number = start_page_num as usize;
                    break;
                }
                end_page_num = page_number as u64;
            } else if key > leaf.pairs[(leaf.length - 1) as usize][0] {
                if page_number == start_page_num as usize {
                    page_number = end_page_num as usize;
                    break;
                }
                start_page_num = page_number as u64;
            } else {
                break;
            }
        }

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

    #[allow(dead_code)]
    pub fn pretty_print_pages(file_id: FileId, file_system: &FileSystem) -> Result<(), DbError> {
        let metadata_page = file_system.get(file_id.page(METADATA_OFFSET as usize))?;
        let metadata: Arc<MetadataPage> = bytemuck::cast_arc(metadata_page);
        let metadata = metadata.metadata;

        if metadata.magic != BEAR_MAGIC {
            return Err(DbError::CorruptSst);
        }

        // Print metadata
        println!("\n╔════════════════════════════════════╗");
        println!("║          METADATA                  ║");
        println!("╠════════════════════════════════════╣");
        println!("║ Magic:        0x{:X}             ║", metadata.magic);
        println!(
            "║ Leafs:        {} -> {}           ",
            metadata.leafs_offset,
            metadata.nodes_offset - 1
        );
        println!(
            "║ Nodes:        {} -> {}           ",
            metadata.nodes_offset,
            metadata.size - 1
        );
        println!("║ Depth:        {}                  ", metadata.tree_depth);
        println!("╚════════════════════════════════════╝\n");

        // Print leafs
        for page_num in metadata.leafs_offset..metadata.nodes_offset {
            let page = file_system.get(file_id.page(page_num as usize))?;
            let leaf: Arc<Leaf> = bytemuck::cast_arc(page);

            println!("🍃 Leaf[{}] ({} pairs)", page_num, leaf.length);
            for i in 0..leaf.length as usize {
                println!("   {} -> {}", leaf.pairs[i][0], leaf.pairs[i][1]);
            }
            println!();
        }

        // Print nodes
        for page_num in metadata.nodes_offset..metadata.size {
            let page = file_system.get(file_id.page(page_num as usize))?;
            let node: Arc<Node> = bytemuck::cast_arc(page);

            println!("🌳 Node[{}] ({} entries)", page_num, node.length);
            for i in 0..node.length as usize {
                println!("   key:{} → page:{}", node.pairs[i][0], node.pairs[i][1]);
            }
            println!();
        }

        Ok(())
    }
}

/// Helper function that gets the largest leaf keys and their corresponding pages and
/// constructs the btree in memory
fn create_tree(btree_keys: Vec<u64>, leaf_pages: Vec<u64>, n: usize) -> Vec<Vec<Vec<(u64, u64)>>> {
    assert_eq!(btree_keys.len(), leaf_pages.len());
    // First build forward pyramid and track mappings
    let mut forward = vec![];
    let mut current = btree_keys.clone();
    loop {
        let chunks: Vec<Vec<u64>> = current.chunks(n).map(|chunk| chunk.to_vec()).collect();

        if chunks.len() <= 1 {
            forward.push(chunks);
            break;
        }

        forward.push(chunks.clone());
        current = chunks.iter().map(|chunk| *chunk.last().unwrap()).collect();
    }

    // Reverse and assign new pages
    forward.reverse();

    let mut result = vec![];
    let mut next_id = 1;

    for (level_idx, level) in forward.iter().enumerate() {
        if level_idx == forward.len() - 1 {
            // Bottom level: use original indices
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
            // Other levels: assign new pages
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
