use crate::{DbError, file_system::PageId, hash::HashFunction};

struct HashTableEntry<V> {
    key: PageId,
    value: V,
    /// The bucket that the key hashed to.
    /// Not necessarily the bucket that the entry actually resides in, thanks to probing.
    hash: usize,
}

/// A hash table for mapping (path, page number) pairs to some value.
///
/// Uses MurmurHash with linear probing.
pub struct HashTable<V> {
    inner: Vec<Option<HashTableEntry<V>>>,
    capacity: usize,
    len: usize,
    hash_function: HashFunction,
}

impl<V> HashTable<V> {
    /// Creates and returns an empty hash table with the given capacity.
    ///
    /// Returns `DbError::Oom` if allocation fails.
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        // Use a load factor of 75% -- simply going off what Java does.
        // Not entirely sure how differences in design affect things,
        // but it's more than the 10%-20% extra capacity in the lecture slides.
        // We decided that the extra space used is worth it
        // in exchange for having more wiggle room.
        // + 1 to ensure the table will always contain at least one empty bucket,
        let num_buckets = capacity * 4 / 3 + 1;

        let mut inner = Vec::new();
        inner.try_reserve_exact(num_buckets)?;
        for _ in 0..num_buckets {
            inner.push(None);
        }

        Ok(Self {
            inner,
            capacity,
            len: 0,
            hash_function: HashFunction::new(),
        })
    }

    fn hash_to_bucket(&self, page_id: PageId) -> usize {
        self.hash_function
            .hash_to_index(page_id, self.num_buckets())
    }

    /// Returns the index in the table where the given key is found,
    /// or an error with the index of an empty bucket.
    fn find(&self, page_id: PageId) -> Result<usize, usize> {
        let hash = self.hash_to_bucket(page_id);

        let mut i = hash;
        loop {
            if let Some(entry) = &self.inner[i] {
                if entry.key == page_id {
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
    pub fn get(&self, page_id: PageId) -> Option<&V> {
        match self.find(page_id) {
            Ok(i) => self.inner[i].as_ref().map(|entry| &entry.value),
            Err(_) => None,
        }
    }

    /// Inserts or updates the given key with the given value.
    ///
    /// Panics if the hash table is already at capacity.
    pub fn insert(&mut self, page_id: PageId, value: V) {
        let i = match self.find(page_id) {
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

        let hash = self.hash_to_bucket(page_id);
        let entry = HashTableEntry {
            key: page_id,
            value,
            hash,
        };

        self.inner[i] = Some(entry);
    }

    /// Removes and returns the value of the entry with the given key.
    ///
    /// Panics if the key doesn't exist in the hash table.
    pub fn remove(&mut self, page_id: PageId) -> V {
        let mut i = self
            .find(page_id)
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

#[cfg(test)]
mod tests {
    // TODO: redo

    // use std::collections::HashMap;
    //
    // use anyhow::Result;
    //
    // use crate::test_util::assert_panics;
    //
    // use super::*;
    //
    // fn insert_many(table: &mut HashTable<u64>, pairs: &[(usize, usize, u64)]) {
    //     for &(path, page_number, value) in pairs {
    //         table.insert(path.into(), page_number, value);
    //     }
    // }
    //
    // fn assert_pairs(
    //     table: &HashTable<u64>,
    //     pairs: impl IntoIterator<Item = (&'static str, usize, Option<u64>)>,
    // ) {
    //     for (path, page_number, value) in pairs {
    //         assert_eq!(
    //             table.get(path, page_number).copied(),
    //             value,
    //             "({path:?}, {page_number:?}) should be {value:?}"
    //         );
    //     }
    // }
    //
    // fn inspect(table: &HashTable<u64>) {
    //     for (i, e) in table.inner.iter().enumerate() {
    //         if let Some(e) = e {
    //             println!("{i}: {:?} {:?}", e.key, e.value);
    //         } else {
    //             println!("{i}: -");
    //         }
    //     }
    //     println!();
    // }
    //
    // #[test]
    // pub fn test_basic() -> Result<()> {
    //     let mut table = HashTable::new(32)?;
    //     let n = table.num_buckets();
    //     let [a, b, c, d, e, f] = [10, 12, 14, n - 2, 0, n - 4];
    //
    //     insert_many(
    //         &mut table,
    //         &[
    //             ("0", b, 3),
    //             ("0", a, 1),
    //             ("0", c, 4),
    //             ("0", d, 1),
    //             ("1", d, 5),
    //             ("2", d, 9),
    //             ("1", a, 2),
    //             ("2", a, 6),
    //             ("1", b, 5),
    //             ("0", e, 3),
    //             ("1", e, 5),
    //             ("2", e, 8),
    //         ],
    //     );
    //     inspect(&table);
    //
    //     assert_pairs(
    //         &table,
    //         [
    //             ("0", b, Some(3)),
    //             ("0", a, Some(1)),
    //             ("0", c, Some(4)),
    //             ("0", d, Some(1)),
    //             ("1", d, Some(5)),
    //             ("2", d, Some(9)),
    //             ("1", a, Some(2)),
    //             ("2", a, Some(6)),
    //             ("1", b, Some(5)),
    //             ("0", e, Some(3)),
    //             ("1", e, Some(5)),
    //             ("2", e, Some(8)),
    //             ("2", b, None),
    //             ("4", d, None),
    //             ("0", f, None),
    //         ],
    //     );
    //     assert_eq!(table.len(), 12);
    //
    //     insert_many(
    //         &mut table,
    //         &[
    //             ("0", c, 9),
    //             ("3", a, 7),
    //             ("2", b, 9),
    //             ("3", d, 3),
    //             ("3", e, 2),
    //             ("2", d, 3),
    //             ("1", a, 8),
    //             ("1", c, 4),
    //             ("0", b, 6),
    //             ("0", e, 2),
    //             ("0", d, 6),
    //             ("4", e, 4),
    //         ],
    //     );
    //     inspect(&table);
    //
    //     assert_pairs(
    //         &table,
    //         [
    //             ("0", b, Some(6)),
    //             ("0", a, Some(1)),
    //             ("0", c, Some(9)),
    //             ("0", d, Some(6)),
    //             ("1", d, Some(5)),
    //             ("2", d, Some(3)),
    //             ("1", a, Some(8)),
    //             ("2", a, Some(6)),
    //             ("1", b, Some(5)),
    //             ("0", e, Some(2)),
    //             ("1", e, Some(5)),
    //             ("2", e, Some(8)),
    //             ("3", a, Some(7)),
    //             ("2", b, Some(9)),
    //             ("3", d, Some(3)),
    //             ("3", e, Some(2)),
    //             ("1", c, Some(4)),
    //             ("4", e, Some(4)),
    //             ("0", f, None),
    //             ("4", a, None),
    //             ("2", c, None),
    //         ],
    //     );
    //     assert_eq!(table.len(), 18);
    //
    //     Ok(())
    // }
    //
    // #[test]
    // fn test_remove() -> Result<()> {
    //     let mut table = HashTable::new(32)?;
    //     let n = table.num_buckets();
    //     let [a, b, c, d, e, f] = [10, 12, 14, n - 2, 0, n - 4];
    //
    //     let pairs = [
    //         ("0", b, 6),
    //         ("0", a, 1),
    //         ("0", c, 9),
    //         ("0", d, 6),
    //         ("1", d, 5),
    //         ("2", d, 3),
    //         ("1", a, 8),
    //         ("2", a, 6),
    //         ("1", b, 5),
    //         ("0", e, 2),
    //         ("1", e, 5),
    //         ("2", e, 8),
    //         ("3", a, 7),
    //         ("2", b, 9),
    //         ("3", d, 3),
    //         ("3", e, 2),
    //         ("1", c, 4),
    //         ("4", e, 4),
    //     ];
    //
    //     let mut reference: HashMap<_, _> = HashMap::from_iter(
    //         pairs
    //             .iter()
    //             .map(|&(path, page_number, value)| ((path, page_number), Some(value))),
    //     );
    //     reference.insert(("0", f), None);
    //     reference.insert(("4", a), None);
    //     reference.insert(("2", c), None);
    //
    //     insert_many(&mut table, &pairs);
    //     assert_eq!(table.len(), 18);
    //     inspect(&table);
    //
    //     let mut assert_remove = |path, page_number, expected_value, expected_new_len| {
    //         *reference.get_mut(&(path, page_number)).unwrap() = None;
    //         assert_eq!(table.remove(path, page_number), expected_value);
    //         assert_eq!(table.len(), expected_new_len);
    //         inspect(&table);
    //         assert_pairs(
    //             &table,
    //             reference
    //                 .iter()
    //                 .map(|(&(path, page_number), &value)| (path, page_number, value)),
    //         );
    //     };
    //
    //     assert_remove("1", c, 4, 17);
    //     assert_remove("3", e, 2, 16);
    //     assert_remove("0", a, 1, 15);
    //     assert_remove("0", d, 6, 14);
    //     assert_remove("0", b, 6, 13);
    //     assert_remove("2", e, 8, 12);
    //     assert_remove("2", a, 6, 11);
    //     assert_remove("1", a, 8, 10);
    //     assert_remove("1", b, 5, 9);
    //     assert_remove("1", d, 5, 8);
    //     assert_remove("3", d, 3, 7);
    //     assert_remove("1", e, 5, 6);
    //
    //     assert_panics(|| _ = table.remove("4", d));
    //     assert_panics(|| _ = table.remove("0", f));
    //     assert_panics(|| _ = table.remove("2", a));
    //
    //     Ok(())
    // }
    //
    // #[test]
    // fn test_over_capacity() -> Result<()> {
    //     let mut table = HashTable::new(128)?;
    //
    //     for i in 0..128 {
    //         table.insert("*".into(), i, i);
    //     }
    //
    //     for i in 0..128 {
    //         table.insert("*".into(), i, i * 2);
    //     }
    //
    //     for i in 0..128 {
    //         table.remove("*", i);
    //     }
    //
    //     for i in 0..128 {
    //         table.insert("/".into(), i, i);
    //     }
    //
    //     assert_panics(|| table.insert("*".into(), 31, 4));
    //
    //     Ok(())
    // }
}
