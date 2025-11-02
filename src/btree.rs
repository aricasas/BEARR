/********************** REMOVE LATER ********************************************************/
#![allow(dead_code)]
#![allow(unused_variables)]
/********************************************************************************************/

use std::{
    ops::RangeInclusive,
    path::{Path, PathBuf},
    rc::Rc,
};

use crate::{DbError, PAGE_SIZE, file_system::Aligned, file_system::FileSystem, sst::Sst};

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

impl Page {
    fn new() -> Box<Self> {
        Box::new(Self::default())
    }
}

const _: () = assert!(size_of::<Page>() == PAGE_SIZE);

pub const BEAR_MAGIC: u64 = 0xBEA22;

#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
struct Metadata {
    magic: u64, // This is used to check the validity of the metadata
    leafs_offset: u64,
    nodes_offset: u64,
    tree_depth: u64,
    padding: [u8; PAGE_SIZE - 4 * 8],
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            magic: BEAR_MAGIC, // Tribute to BEARR
            leafs_offset: LEAF_OFFSET,
            nodes_offset: 0x1000,
            tree_depth: 0,
            padding: [Default::default(); _],
        }
    }
}

const _: () = assert!(size_of::<Metadata>() == PAGE_SIZE);

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
    file_system: &'b mut FileSystem,
    page_number: usize,
    item_number: usize,
    range: RangeInclusive<u64>,
    ended: bool,
}

impl<'a, 'b> Iterator for BTreeIter<'a, 'b> {
    type Item = Result<(u64, u64), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

// TODO: Write the Btree iterator
impl<'a, 'b> BTreeIter<'a, 'b> {
    pub fn new(
        sst: &'a Sst,
        range: RangeInclusive<u64>,
        file_system: &'b mut FileSystem,
    ) -> Result<Self, DbError> {
        let nodes_offset = sst.nodes_offset;
        let leafs_offset = sst.leafs_offset;
        let tree_depth = sst.tree_depth;

        assert!(nodes_offset > leafs_offset);

        if range.start() > range.end() {
            return Err(DbError::InvalidScanRange);
        }

        let mut match_page_number = 0;
        let mut match_item_number = 0;
        let mut found = false;

        let root_page = file_system.get(&sst.path, nodes_offset as usize)?;
        let root_node: Rc<Node> = bytemuck::cast_rc(root_page);

        for level in 0..tree_depth {
            let idx = match nums.binary_search(&range.start()) {
                Ok(i) => i,
                Err(i) => i,
            };
        }

        let ended = !found;

        let iter = Self {
            sst,
            file_system,
            page_number: match_page_number,
            item_number: match_item_number,
            range,
            ended,
        };

        Ok(iter)
    }

    fn go_to_next(&mut self) -> Option<Result<(u64, u64), DbError>> {
        if self.ended {
            return None;
        }

        let page_bytes = self.file_system.get(&self.sst.path, self.page_number);

        let buffered_page: Rc<Page> = match page_bytes {
            Ok(bytes) => bytemuck::cast_rc(bytes),
            Err(e) => return Some(Err(e)),
        };

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

        if self.page_number >= self.sst.nodes_offset as usize {
            // EOF
            self.ended = true;
            return Some(Ok(item));
        }

        Some(Ok(item))
    }
}

pub struct BTree {}

impl BTree {
    /// Creates a static B tree in a
    /// Returns a Vec containing the paths to all the levels
    pub fn write(
        path: impl AsRef<Path>,
        mut pairs: impl Iterator<Item = Result<(u64, u64), DbError>>,
        file_system: &mut FileSystem,
    ) -> Result<(u64, u64, u64), DbError> {
        let mut nodes_offset: u64;
        let tree_depth: u64;
        let mut largest_keys: Vec<u64> = Vec::new();
        let mut largest_pages: Vec<u64> = Vec::new();

        let mut leaf_count: u64 = 0;

        // leaf write closure trait
        let write_next_leaf = |page_bytes: &mut Aligned| {
            let leaf: &mut Leaf = bytemuck::cast_mut(page_bytes);
            leaf.length = 0;
            for (pair, k_v) in leaf.pairs.iter_mut().zip(&mut pairs) {
                match k_v {
                    Ok((k, v)) => *pair = [k, v],
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

        nodes_offset = file_system.write_file(&path, LEAF_OFFSET as usize, write_next_leaf)? as u64;
        nodes_offset += LEAF_OFFSET;

        // Construct the Btree in Memory
        let btree = create_tree(largest_keys, largest_pages, KEYS_PER_NODE);
        tree_depth = btree.len() as u64;

        let write_metadata = |page_bytes: &mut Aligned| {
            let metadata: &mut Metadata = bytemuck::cast_mut(page_bytes);
            metadata.leafs_offset = LEAF_OFFSET;
            metadata.nodes_offset = nodes_offset;
            metadata.tree_depth = tree_depth;
            Ok(false)
        };
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

        /* leafs --> nodes --> metadata */

        let nodes_written =
            file_system.write_file(&path, nodes_offset as usize, write_next_btree_page)? as u64;

        let metadata_pages =
            file_system.write_file(&path, METADATA_OFFSET as usize, write_metadata)? as u64;

        Ok((nodes_offset, LEAF_OFFSET, tree_depth))
    }

    pub fn open(
        path: impl AsRef<Path>,
        file_system: &FileSystem,
    ) -> Result<(u64, u64, u64), DbError> {
        let metadata_page = file_system.get(path, METADATA_OFFSET as usize)?;
        let metadata: Rc<Metadata> = bytemuck::cast_rc(metadata_page);

        if metadata.magic != BEAR_MAGIC {
            return Err(DbError::CorruptSst);
        }

        Ok((
            metadata.nodes_offset,
            metadata.leafs_offset,
            metadata.tree_depth,
        ))
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

/* Tests for SSTs */
#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use anyhow::Result;

    use super::*;

    struct TestPath {
        path: PathBuf,
    }

    impl TestPath {
        fn new(path: impl AsRef<Path>) -> Self {
            Self {
                path: path.as_ref().to_path_buf(),
            }
        }
    }

    impl AsRef<Path> for TestPath {
        fn as_ref(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            _ = fs::remove_file(&self.path);
        }
    }
}
