use std::path::{Path, PathBuf};

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
    pub fn new(capacity: usize) -> Self {
        todo!()
    }
    pub fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Option<&V> {
        todo!()
    }
    pub fn insert(&mut self, path: PathBuf, page_number: usize) {
        todo!()
    }
    pub fn remove(&mut self, path: impl AsRef<Path>, page_number: usize) -> V {
        todo!()
    }
}

// TODO remove if note needed
fn hash_to_index(path: impl AsRef<Path>, page_number: usize, container_length: usize) -> usize {
    todo!()
}
