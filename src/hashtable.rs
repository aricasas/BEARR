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

/// A hash table for mapping (path, page number) pairs to some value.
///
/// Uses MurmurHash with linear probing.
pub struct HashTable<V> {
    inner: Vec<Option<HashTableEntry<V>>>,
    capacity: usize,
    len: usize,
}

impl<V> HashTable<V> {
    /// Creates and returns an empty hash table with the given capacity.
    ///
    /// Returns `DbError::Oom` if allocation fails.
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

    /// Returns a reference to the value with the given key,
    /// or None if the key doesn't exist in the hash table.
    pub fn get(&self, path: impl AsRef<Path>, page_number: usize) -> Option<&V> {
        match self.find(path, page_number) {
            Ok(i) => self.inner[i].as_ref().map(|entry| &entry.value),
            Err(_) => None,
        }
    }

    /// Inserts or updates the given key with the given value.
    ///
    /// Panics if the hash table is already at capacity.
    pub fn insert(&mut self, path: PathBuf, page_number: usize, value: V) {
        let i = match self.find(&path, page_number) {
            Ok(i) => i,
            Err(i) => {
                assert_ne!(
                    self.len, self.capacity,
                    "cannot insert into at-capacity hashtable"
                );

                self.len += 1;

                i
            }
        };

        let hash = hash_to_index(&path, page_number, self.num_buckets());
        let entry = HashTableEntry {
            key: (path, page_number),
            value,
            hash,
        };

        self.inner[i] = Some(entry);
    }

    /// Removes and returns the value of the entry with the given key.
    ///
    /// Panics if the key doesn't exist in the hash table.
    pub fn remove(&mut self, path: impl AsRef<Path>, page_number: usize) -> V {
        let mut i = self
            .find(path, page_number)
            .expect("must only delete keys that are in the table");

        self.len -= 1;

        let result = self.inner[i].take().unwrap();

        // https://en.wikipedia.org/wiki/Linear_probing#Deletion
        // Invariant to uphold: for all entries (k, v),
        // there are no holes (empty buckets) between bucket hash(k) (inclusive)
        // and the bucket that the entry resides in
        // (inclusive/exclusive doesn't matter since the hole's not there anyway).
        let mut hole = i;
        loop {
            i = (i + 1) % self.num_buckets();
            // If an empty bucket is reached,
            // then the invariant is, by assumption, already upheld for all entries after that bucket.
            let Some(entry) = &self.inner[i] else {
                return result.value;
            };

            let hash = entry.hash;

            /*
                0 1 2 3 4 5 6 7
                    ^     ^
                    hash  entry
                    ^^^^^
                entries where the hole, if present there, must be filled (2..5)

                0 1 2 3 4 5 6 7
                    ^     ^
                    entry hash
                ^^^       ^^^^^
                entries where the hole, if present there, must be filled (everywhere but 2..5)

                0 1 2 3 4 5 6 7
                        ^ entry and hash
                entries where the hole, if present there, must be filled: (empty)
            */
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

    /// Returns the number of entries in the hash table.
    pub fn len(&self) -> usize {
        self.len
    }
}

fn murmur_hash_to_index(
    path: impl AsRef<Path>,
    page_number: usize,
    container_length: usize,
) -> usize {
    let mut path = path.as_ref().to_path_buf();
    path.push(page_number.to_string());

    // Seed was taken directly and arbitrarily from Wikipedia test cases
    murmur_hash::murmur3_32(path.as_os_str().as_bytes(), 0x9747b28c) as usize % container_length
}

#[cfg(not(feature = "mock_hash"))]
use murmur_hash_to_index as hash_to_index;

#[cfg(feature = "mock_hash")]
pub fn hash_to_index(
    _path: impl AsRef<Path>,
    page_number: usize,
    container_length: usize,
) -> usize {
    page_number % container_length
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;

    use super::*;

    fn insert_many(table: &mut HashTable<u64>, pairs: &[(&str, usize, u64)]) {
        for &(path, page_number, value) in pairs {
            table.insert(path.into(), page_number, value);
        }
    }

    fn assert_pairs(
        table: &HashTable<u64>,
        pairs: impl IntoIterator<Item = (&'static str, usize, Option<u64>)>,
    ) {
        for (path, page_number, value) in pairs {
            assert_eq!(
                table.get(path, page_number).copied(),
                value,
                "({path:?}, {page_number:?}) should be {value:?}"
            );
        }
    }

    fn inspect(table: &HashTable<u64>) {
        for (i, e) in table.inner.iter().enumerate() {
            if let Some(e) = e {
                println!("{i}: {:?} {:?}", e.key, e.value);
            } else {
                println!("{i}: -");
            }
        }
        println!();
    }

    #[test]
    pub fn test_basic() -> Result<()> {
        let mut table = HashTable::new(32)?;
        let n = table.num_buckets();
        let [a, b, c, d, e, f] = [10, 12, 14, n - 2, 0, n - 4];

        insert_many(
            &mut table,
            &[
                ("0", b, 3),
                ("0", a, 1),
                ("0", c, 4),
                ("0", d, 1),
                ("1", d, 5),
                ("2", d, 9),
                ("1", a, 2),
                ("2", a, 6),
                ("1", b, 5),
                ("0", e, 3),
                ("1", e, 5),
                ("2", e, 8),
            ],
        );
        inspect(&table);

        assert_pairs(
            &table,
            [
                ("0", b, Some(3)),
                ("0", a, Some(1)),
                ("0", c, Some(4)),
                ("0", d, Some(1)),
                ("1", d, Some(5)),
                ("2", d, Some(9)),
                ("1", a, Some(2)),
                ("2", a, Some(6)),
                ("1", b, Some(5)),
                ("0", e, Some(3)),
                ("1", e, Some(5)),
                ("2", e, Some(8)),
                ("2", b, None),
                ("4", d, None),
                ("0", f, None),
            ],
        );
        assert_eq!(table.len(), 12);

        insert_many(
            &mut table,
            &[
                ("0", c, 9),
                ("3", a, 7),
                ("2", b, 9),
                ("3", d, 3),
                ("3", e, 2),
                ("2", d, 3),
                ("1", a, 8),
                ("1", c, 4),
                ("0", b, 6),
                ("0", e, 2),
                ("0", d, 6),
                ("4", e, 4),
            ],
        );
        inspect(&table);

        assert_pairs(
            &table,
            [
                ("0", b, Some(6)),
                ("0", a, Some(1)),
                ("0", c, Some(9)),
                ("0", d, Some(6)),
                ("1", d, Some(5)),
                ("2", d, Some(3)),
                ("1", a, Some(8)),
                ("2", a, Some(6)),
                ("1", b, Some(5)),
                ("0", e, Some(2)),
                ("1", e, Some(5)),
                ("2", e, Some(8)),
                ("3", a, Some(7)),
                ("2", b, Some(9)),
                ("3", d, Some(3)),
                ("3", e, Some(2)),
                ("1", c, Some(4)),
                ("4", e, Some(4)),
                ("0", f, None),
                ("4", a, None),
                ("2", c, None),
            ],
        );
        assert_eq!(table.len(), 18);

        Ok(())
    }

    #[test]
    fn test_remove() -> Result<()> {
        let mut table = HashTable::new(32)?;
        let n = table.num_buckets();
        let [a, b, c, d, e, f] = [10, 12, 14, n - 2, 0, n - 4];

        let pairs = [
            ("0", b, 6),
            ("0", a, 1),
            ("0", c, 9),
            ("0", d, 6),
            ("1", d, 5),
            ("2", d, 3),
            ("1", a, 8),
            ("2", a, 6),
            ("1", b, 5),
            ("0", e, 2),
            ("1", e, 5),
            ("2", e, 8),
            ("3", a, 7),
            ("2", b, 9),
            ("3", d, 3),
            ("3", e, 2),
            ("1", c, 4),
            ("4", e, 4),
        ];

        let mut reference: HashMap<_, _> = HashMap::from_iter(
            pairs
                .iter()
                .map(|&(path, page_number, value)| ((path, page_number), Some(value))),
        );
        reference.insert(("0", f), None);
        reference.insert(("4", a), None);
        reference.insert(("2", c), None);

        insert_many(&mut table, &pairs);
        assert_eq!(table.len(), 18);
        inspect(&table);

        let mut assert_remove = |path, page_number, expected_value, expected_new_len| {
            *reference.get_mut(&(path, page_number)).unwrap() = None;
            assert_eq!(table.remove(path, page_number), expected_value);
            assert_eq!(table.len(), expected_new_len);
            inspect(&table);
            assert_pairs(
                &table,
                reference
                    .iter()
                    .map(|(&(path, page_number), &value)| (path, page_number, value)),
            );
        };

        assert_remove("1", c, 4, 17);
        assert_remove("3", e, 2, 16);
        assert_remove("0", a, 1, 15);
        assert_remove("0", d, 6, 14);
        assert_remove("0", b, 6, 13);
        assert_remove("2", e, 8, 12);
        assert_remove("2", a, 6, 11);
        assert_remove("1", a, 8, 10);
        assert_remove("1", b, 5, 9);
        assert_remove("1", d, 5, 8);
        assert_remove("3", d, 3, 7);
        assert_remove("1", e, 5, 6);

        let mut assert_remove_absent = |path, page_number| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                table.remove(path, page_number)
            }));
            assert!(result.is_err());
        };

        assert_remove_absent("4", d);
        assert_remove_absent("0", f);
        assert_remove_absent("2", a);

        Ok(())
    }

    #[test]
    fn test_over_capacity() -> Result<()> {
        let mut table = HashTable::new(128)?;

        for i in 0..128 {
            table.insert("*".into(), i, i);
        }

        for i in 0..128 {
            table.insert("*".into(), i, i * 2);
        }

        for i in 0..128 {
            table.remove("*", i);
        }

        for i in 0..128 {
            table.insert("/".into(), i, i);
        }

        let result = std::panic::catch_unwind(move || table.insert("*".into(), 31, 4));
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_murmur_hash_to_index() {
        assert!((0..16).contains(&murmur_hash_to_index("monad", 2, 16)));

        assert_ne!(
            murmur_hash_to_index("monad", 3, 32),
            murmur_hash_to_index("monad", 5, 32),
        );

        assert_ne!(
            murmur_hash_to_index("monad", 7, 64),
            murmur_hash_to_index("monoid", 7, 64),
        );
    }
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
