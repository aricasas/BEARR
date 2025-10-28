use std::path::{Path, PathBuf};

use crate::DbError;

struct HashTableEntry<V> {
    value: V,
    key: (PathBuf, usize),
    // TODO
}
pub struct HashTable<V> {
    inner: Vec<HashTableEntry<V>>,
    // TODO
}

impl<V> HashTable<V> {
    // TODO: return OOM if Vec::try_reserve_exact fails
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        todo!()
    }
    pub fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Option<&V> {
        todo!()
    }
    pub fn insert(&mut self, path: PathBuf, page_number: usize, value: V) {
        todo!()
    }
    pub fn remove(&mut self, path: impl AsRef<Path>, page_number: usize) -> V {
        todo!()
    }
    pub fn len(&self) -> usize {
        todo!()
    }
}

// TODO remove if note needed
fn hash_to_index(path: impl AsRef<Path>, page_number: usize, container_length: usize) -> usize {
    todo!()
}
