use std::{
    cmp::{self, Ordering},
    collections::BinaryHeap,
};

use crate::{DbError, btree::BTreeIter, memtable::MemTableIter};

pub enum Sources<'a> {
    MemTable(MemTableIter<'a, u64, u64>),
    BTree(BTreeIter<'a, 'a>),
}

impl<'a> Iterator for Sources<'a> {
    type Item = Result<(u64, u64), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::MemTable(mem_table_iter) => {
                let kv = mem_table_iter.next()?;
                Some(Ok(kv))
            }
            Self::BTree(btree_iter) => btree_iter.next(),
        }
    }
}

pub struct MergedIterator<I: Iterator<Item = Result<(u64, u64), DbError>>> {
    /// Sorted by age, lower index means newer
    levels: Vec<I>,
    heap: BinaryHeap<cmp::Reverse<Entry>>,
    last_key: Option<u64>,
    delete_tombstones: bool,
    ended: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Entry {
    key: u64,
    level: usize,
    value: u64,
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key).then(self.level.cmp(&other.level))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: Iterator<Item = Result<(u64, u64), DbError>>> MergedIterator<I> {
    /// Creates a new iterator that merges several iterators into a single output.
    /// It merges them sorted by keys, while skipping repeated keys.
    /// For a key with several iterators returning it, only the value of the iterator at the highest level is
    /// returned and the others are skipped.
    ///
    /// TODO: explain delete_tombstones
    ///
    /// `levels[0]`is the highest level and `levels[levels.len() - 1]` is the lowest level
    pub fn new(mut levels: Vec<I>, delete_tombstones: bool) -> Result<Self, DbError> {
        let mut starting = Vec::new();
        starting.try_reserve_exact(levels.len())?;

        for (level, iter) in levels.iter_mut().enumerate() {
            if let Some(entry) = iter.next() {
                let (key, value) = entry?;
                starting.push(cmp::Reverse(Entry { key, level, value }));
            }
        }
        let heap = BinaryHeap::from(starting);
        let ended = heap.is_empty();

        Ok(Self {
            levels,
            heap,
            last_key: None,
            delete_tombstones,
            ended,
        })
    }
    fn pop_and_replace(&mut self) -> Result<Option<Entry>, DbError> {
        // PeekMut allows doing extract_min and insert_new without performing sift_down twice
        let Some(mut min) = self.heap.peek_mut() else {
            return Ok(None);
        };

        let cmp::Reverse(save) = *min;

        let replacement = self.levels[min.0.level].next();
        match replacement {
            Some(Ok((key, value))) => {
                // Insert the new key value pair in the spot of the one we're removing
                // PeekMut takes care of sifting it down
                min.0.key = key;
                min.0.value = value;
            }
            None => {
                // No replacement, have to actually remove the min
                drop(min);
                self.heap.pop();
            }
            Some(Err(e)) => return Err(e),
        }
        Ok(Some(save))
    }
}

impl<I: Iterator<Item = Result<(u64, u64), DbError>>> Iterator for MergedIterator<I> {
    type Item = Result<(u64, u64), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }

        let mut min;
        loop {
            min = match self.pop_and_replace() {
                Ok(Some(min)) => min,
                Ok(None) => {
                    // No key value pairs left in the minheap, so we're done
                    self.ended = true;
                    return None;
                }
                Err(e) => {
                    self.ended = true;
                    return Some(Err(e));
                }
            };

            // Skip entries with keys we've seen at a higher level already

            // TODO: check for tombstones when needed
            if self.last_key.is_none_or(|last_key| min.key > last_key) {
                break;
            }
        }

        self.last_key = Some(min.key);
        Some(Ok((min.key, min.value)))
    }
}

// TODO: add more tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_one() {
        let iter = (1u64..=5).map(|i| Ok((i, i)));
        let mut merged = MergedIterator::new(vec![iter], false).unwrap();

        assert_eq!(merged.next(), Some(Ok((1, 1))));
        assert_eq!(merged.next(), Some(Ok((2, 2))));
        assert_eq!(merged.next(), Some(Ok((3, 3))));
        assert_eq!(merged.next(), Some(Ok((4, 4))));
        assert_eq!(merged.next(), Some(Ok((5, 5))));
        assert_eq!(merged.next(), None);
    }
    #[test]
    fn test_merge_two() {
        let x = Box::new((0u64..=3).map(|i| Ok((i, i)))) as Box<dyn Iterator<Item = _>>;
        let y = Box::new((2u64..=5).map(|i| Ok((i, i * 2)))) as Box<dyn Iterator<Item = _>>;

        let mut merged = MergedIterator::new(vec![x, y], false).unwrap();
        assert_eq!(merged.next(), Some(Ok((0, 0))));
        assert_eq!(merged.next(), Some(Ok((1, 1))));
        assert_eq!(merged.next(), Some(Ok((2, 2))));
        assert_eq!(merged.next(), Some(Ok((3, 3))));
        assert_eq!(merged.next(), Some(Ok((4, 8))));
        assert_eq!(merged.next(), Some(Ok((5, 10))));
        assert_eq!(merged.next(), None);

        let x = Box::new((0u64..=3).map(|i| Ok((i, i)))) as Box<dyn Iterator<Item = _>>;
        let y = Box::new((2u64..=5).map(|i| Ok((i, i * 2)))) as Box<dyn Iterator<Item = _>>;

        let mut merged = MergedIterator::new(vec![y, x], false).unwrap();
        assert_eq!(merged.next(), Some(Ok((0, 0))));
        assert_eq!(merged.next(), Some(Ok((1, 1))));
        assert_eq!(merged.next(), Some(Ok((2, 4))));
        assert_eq!(merged.next(), Some(Ok((3, 6))));
        assert_eq!(merged.next(), Some(Ok((4, 8))));
        assert_eq!(merged.next(), Some(Ok((5, 10))));
        assert_eq!(merged.next(), None);
    }
}
