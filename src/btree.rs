/********************** REMOVE LATER ********************************************************/
#![allow(dead_code)]
#![allow(unused_variables)]
/********************************************************************************************/

use std::{
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use crate::{
    DbError, PAGE_SIZE, file_system::Aligned, file_system::FileSystem, sst::Page, sst::Sst,
};
//
#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
struct Metadata {
    leafs_offset: u64,
    nodes_offset: u64,
    padding: [u8; PAGE_SIZE - 2 * 8],
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            leafs_offset: 1,
            nodes_offset: 0x1000,
            padding: [Default::default(); _],
        }
    }
}

const LEAF_OFFSET: usize = 1;
const METADATA_OFFSET: usize = 0;
const KEYS_PER_NODE: usize = (PAGE_SIZE - 8) / 16;
// #[repr(C, align(4096))]
// #[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
// struct Leaf {
//     bytes: [u8; PAGE_SIZE], // TODO
// }
//
// // Make sure all the Leafs and Nodes of Btree are page aligned
// const _: () = assert!(size_of::<Node>() == PAGE_SIZE);
// const _: () = assert!(size_of::<Leaf>() == PAGE_SIZE);
//
// const NODE_CAPACITY: usize = PAGE_SIZE /;
// const LEAF_CAPACITY: usize = 0; // TODO

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
    num_pages: usize,
    ended: bool,
}

impl<'a, 'b> Iterator for BTreeIter<'a, 'b> {
    type Item = Result<(u64, u64), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<'a, 'b> BTreeIter<'a, 'b> {
    pub fn new(
        sst: &'a Sst,
        range: RangeInclusive<u64>,
        file_system: &mut FileSystem,
    ) -> Result<Self, DbError> {
        todo!()
    }
}

/// Creates a static B tree in a
/// Returns a Vec containing the paths to all the levels
pub fn write_btree(
    path: impl AsRef<Path>,
    pairs: &impl Iterator<Item = Result<(u64, u64), DbError>>,
    file_system: &mut FileSystem,
) -> Result<DbError> {
    let nodes_offset: &mut u64 = &mut 1;
    let mut btree: Vec<Vec<Vec<(u64, u64)>>>;
    let mut largest_keys: Vec<u64> = Vec::new();
    let mut largest_pages: Vec<u64> = Vec::new();

    let tree_depth: usize = 0;
    let leaf_count: u64 = 0;

    let write_metadata = |page_bytes: &mut Aligned| {
        let metadata: &mut Metadata = bytemuck::cast_mut(page_bytes);
        metadata.leafs_offset = LEAF_OFFSET as u64;
        metadata.nodes_offset = *nodes_offset;
        Ok(false)
    };

    // leaf write closure trait
    let write_next_leaf = |page_bytes: &mut Aligned| {
        let leaf: &mut Leaf = bytemuck::cast_mut(page_bytes);
        leaf.length = 0;
        for (pair, k_v) in leaf.pairs.iter_mut().zip(&mut *pairs) {
            match k_v {
                Ok((k, v)) => *pair = [k, v],
                Err(e) => return Err(e),
            }
            leaf.length += 1;
        }

        // Push the largest key in a page to the largest keys vector
        if leaf.length > 0 {
            largest_keys.push(leaf.pairs.last()[0]);
            largest_pages.push(leaf_count);
            leaf_count += 1;
        }
        Ok(leaf.length > 0)
    };

    *nodes_offset = file_system.write_file(&path, LEAF_OFFSET, write_next_leaf)? as u64;

    // Construct the Btree in Memory
    btree = create_tree(largest_keys, largest_pages, KEYS_PER_NODE);

    let mut btree_itter = btree.into_iter().flatten().collect().into_iter();

    // btree write closure trait
    let write_next_btree_page = |page_bytes: &mut Aligned| {
        let node: &mut Node = bytemuck::cast_mut(page_bytes);
        node.length = 0;
        for (pair, k_v) in node.pairs.iter_mut().zip(&mut btree_itter.next()) {
            match k_v {
                Ok((k, v)) => *pair = [k, v],
                Err(e) => return Err(e),
            }
            node.length += 1;
        }

        Ok(node.length > 0)
    };
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
