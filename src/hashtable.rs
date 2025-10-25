use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::DbError;

struct HashTableEntry<V> {
    value: V,
    key: (PathBuf, usize),
    // TODO
}
pub struct HashTable<V> {
    inner: Vec<HashTableEntry<V>>,
    mock: HashMap<(PathBuf, usize), V>,
    capacity: usize, // TODO
}

impl<V> HashTable<V> {
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        let mut mock = HashMap::new();
        mock.try_reserve(capacity)?;

        Ok(Self {
            inner: Vec::new(),
            mock,
            capacity,
        })
    }
    pub fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Option<&V> {
        self.mock.get(&(path.as_ref().to_owned(), page_number))
    }
    pub fn insert(&mut self, path: PathBuf, page_number: usize, value: V) {
        assert!(self.mock.len() < self.capacity);
        assert!(self.mock.insert((path, page_number), value).is_none());
    }
    pub fn remove(&mut self, path: impl AsRef<Path>, page_number: usize) -> V {
        self.mock
            .remove(&(path.as_ref().to_owned(), page_number))
            .unwrap()
    }
}

// TODO remove if note needed
fn hash_to_index(path: impl AsRef<Path>, page_number: usize, container_length: usize) -> usize {
    todo!()
}
