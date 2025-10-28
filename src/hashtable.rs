use std::{
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use crate::DbError;

struct HashTableEntry<V> {
    key: (PathBuf, usize),
    value: V,
    hash: usize,
}

pub struct HashTable<V> {
    inner: Vec<Option<HashTableEntry<V>>>,
    capacity: usize,
    len: usize,
}

impl<V> HashTable<V> {
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        // Largely arbitrary multiplier; satisfies need for between 10% and 20% extra capacity
        // + 1 to ensure the table will always contain at least one empty bucket
        let num_buckets = capacity * 9 / 8 + 1;
        let mut inner = Vec::new();
        inner.try_reserve_exact(num_buckets)?;
        for _ in 0..num_buckets {
            inner.push(None);
        }
        Ok(Self {
            inner,
            capacity,
            len: 0,
        })
    }

    /// Returns the index in the table where the given key is found,
    /// or an error with the index of an empty bucket.
    fn find(&self, path: impl AsRef<Path>, page_number: usize) -> Result<usize, usize> {
        let path = path.as_ref();
        let hash = hash_to_index(path, page_number, self.num_buckets());
        let mut i = hash;
        loop {
            if let Some(entry) = &self.inner[i] {
                let (p, pg) = &entry.key;
                if entry.hash == hash && p == path && *pg == page_number {
                    return Ok(i);
                }
            } else {
                return Err(i);
            }
            i = (i + 1) % self.num_buckets();
        }
    }

    pub fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Option<&V> {
        match self.find(path, page_number) {
            Ok(i) => self.inner[i].as_ref().map(|entry| &entry.value),
            Err(_) => None,
        }
    }

    pub fn insert(&mut self, path: PathBuf, page_number: usize, value: V) {
        assert_ne!(
            self.len, self.capacity,
            "cannot insert into at-capacity hashtable"
        );

        let (Ok(i) | Err(i)) = self.find(&path, page_number);
        let hash = hash_to_index(&path, page_number, self.num_buckets());
        let entry = HashTableEntry {
            key: (path, page_number),
            value,
            hash,
        };
        self.inner[i] = Some(entry);
    }

    pub fn remove(&mut self, path: impl AsRef<Path>, page_number: usize) -> V {
        let mut i = self
            .find(path, page_number)
            .expect("must only delete keys that are in the table");
        let result = self.inner[i].take().unwrap();
        let mut hole = i;
        loop {
            i = (i + 1) % self.num_buckets();
            let Some(entry) = &self.inner[i] else {
                return result.value;
            };
            let hash = entry.hash;
            let needs_move = if hash <= i {
                (hash..i).contains(&hole)
            } else {
                !(i..hash).contains(&hole)
            };
            if needs_move {
                self.inner.swap(i, hole);
                hole = i;
            }
        }
    }

    fn num_buckets(&self) -> usize {
        self.inner.len()
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

fn hash_to_index(path: impl AsRef<Path>, page_number: usize, container_length: usize) -> usize {
    let mut path = path.as_ref().to_path_buf();
    path.push(page_number.to_string());
    // Seed was taken directly and arbitrarily from Wikipedia test cases
    murmur_hash::murmur3_32(path.as_os_str().as_bytes(), 0x9747b28c) as usize % container_length
}

/// Implementation of the 32-bit MurmurHash3 hash function.
/// https://en.wikipedia.org/wiki/MurmurHash
mod murmur_hash {
    use std::{array, num::Wrapping as W};

    pub fn murmur3_32(key: &[u8], seed: u32) -> u32 {
        let len = key.len();

        let c1 = W(0xcc9e2d51);
        let c2 = W(0x1b873593);
        let r1 = 15;
        let r2 = 13;
        let m = W(5);
        let n = W(0xe6546b64);

        let mut hash = W(seed);

        let (chunks, remainder) = key.as_chunks::<4>();
        let remainder: [u8; 4] = array::from_fn(|i| remainder.get(i).copied().unwrap_or(0));

        for &chunk in chunks {
            let mut k = W(u32::from_le_bytes(chunk));

            k *= c1;
            k = W(k.0.rotate_left(r1));
            k *= c2;

            hash ^= k;
            hash = W(hash.0.rotate_left(r2));
            hash = (hash * m) + n;
        }

        {
            let mut remainder = W(u32::from_le_bytes(remainder));

            remainder *= c1;
            remainder = W(remainder.0.rotate_left(r1));
            remainder *= c2;

            hash ^= remainder;
        }

        hash ^= len as u32;

        hash ^= hash >> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >> 16;

        hash.0
    }

    #[cfg(test)]
    mod tests {
        use rstest::rstest;

        use super::*;

        #[rstest]
        #[case(0x00000000, 0x00000000, "")]
        #[case(0x00000001, 0x514e28b7, "")]
        #[case(0xffffffff, 0x81f16f39, "")]
        #[case(0x00000000, 0xba6bd213, "test")]
        #[case(0x9747b28c, 0x704b81dc, "test")]
        #[case(0x00000000, 0xc0363e43, "Hello, world!")]
        #[case(0x9747b28c, 0x24884cba, "Hello, world!")]
        #[case(0x00000000, 0x2e4ff723, "The quick brown fox jumps over the lazy dog")]
        #[case(0x9747b28c, 0x2fa826cd, "The quick brown fox jumps over the lazy dog")]
        fn test_murmur(#[case] seed: u32, #[case] expected: u32, #[case] key: &str) {
            assert_eq!(murmur3_32(key.as_bytes(), seed), expected);
        }
    }
}
