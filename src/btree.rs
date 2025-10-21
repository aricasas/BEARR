use std::{ops::RangeInclusive, path::Path};

use crate::{DbError, PAGE_SIZE, file_system::FileSystem, sst::Sst};

#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
struct Node {
    bytes: [u8; PAGE_SIZE],
}

#[repr(C, align(4096))]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy)]
struct Leaf {
    bytes: [u8; PAGE_SIZE],
}

const _: () = assert!(size_of::<Node>() == PAGE_SIZE);
const _: () = assert!(size_of::<Leaf>() == PAGE_SIZE);

const NODE_CAPACITY: usize = 0;
const LEAF_CAPACITY: usize = 0;

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

/// Creates a static B tree in a file (probably B+ tree?) (should we make an implicit B tree? (i.e. don't store pointers))
pub fn write_btree_to_files(
    path_prefix: impl AsRef<Path>,
    pairs: &impl Iterator<Item = Result<(u64, u64), DbError>>,
    file_system: &mut FileSystem,
) -> Result<(), DbError> {
    todo!()
}

fn find_leaf_containing_key(
    path: impl AsRef<Path>,
    key: u64,
    file_system: &mut FileSystem,
) -> Result<usize, DbError> {
    todo!()
}
