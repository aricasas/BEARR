use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use crate::{DbError, memtable::MemTableIter, sst::SstIter};

pub enum Sources<'a> {
    MemTable(MemTableIter<'a, u64, u64>),
    Sst(SstIter<'a, 'a>),
}

impl<'a> Iterator for Sources<'a> {
    type Item = Result<(u64, u64), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Sources::MemTable(mem_table_iter) => {
                let kv = mem_table_iter.next()?;
                Some(Ok(kv))
            }
            Sources::Sst(sst_iter) => sst_iter.next(),
        }
    }
}

pub struct MergedIterator<I: Iterator<Item = Result<(u64, u64), DbError>>> {
    /// Smaller index means newer
    levels: Vec<I>,
    heap: BinaryHeap<Reverse<Entry>>,
    last_key: Option<u64>,
    ended: bool,
}

#[derive(Clone, PartialEq, Eq)]
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
    pub fn new(mut levels: Vec<I>) -> Result<Self, DbError> {
        let mut starting = Vec::new();
        starting.try_reserve_exact(levels.len())?;

        for (level, iter) in levels.iter_mut().enumerate() {
            if let Some(entry) = iter.next() {
                let (key, value) = entry?;
                starting.push(Reverse(Entry { key, level, value }));
            }
        }
        let heap = BinaryHeap::from(starting);
        let ended = heap.is_empty();

        Ok(Self {
            levels,
            heap,
            last_key: None,
            ended,
        })
    }
}

impl<I: Iterator<Item = Result<(u64, u64), DbError>>> Iterator for MergedIterator<I> {
    type Item = Result<(u64, u64), DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }

        let mut key;
        let mut value;

        loop {
            let Some(mut head) = self.heap.peek_mut() else {
                self.ended = true;
                return None;
            };

            let level = head.0.level;
            key = head.0.key;
            value = head.0.value;

            let iter = &mut self.levels[level];

            if let Some(entry) = iter.next() {
                let (key, value) = match entry {
                    Ok(kv) => kv,
                    Err(e) => {
                        self.ended = true;
                        return Some(Err(e));
                    }
                };

                *head = Reverse(Entry { key, level, value });
            } else {
                drop(head);
                self.heap.pop();
            }

            if self.last_key.is_none_or(|last_key| last_key < key) {
                self.last_key = Some(key);
                break;
            }
        }

        Some(Ok((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_one() {
        let iter = (1u64..=5).map(|i| Ok((i, i)));
        let mut merged = MergedIterator::new(vec![iter]).unwrap();

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

        let mut merged = MergedIterator::new(vec![x, y]).unwrap();
        assert_eq!(merged.next(), Some(Ok((0, 0))));
        assert_eq!(merged.next(), Some(Ok((1, 1))));
        assert_eq!(merged.next(), Some(Ok((2, 2))));
        assert_eq!(merged.next(), Some(Ok((3, 3))));
        assert_eq!(merged.next(), Some(Ok((4, 8))));
        assert_eq!(merged.next(), Some(Ok((5, 10))));
        assert_eq!(merged.next(), None);

        let x = Box::new((0u64..=3).map(|i| Ok((i, i)))) as Box<dyn Iterator<Item = _>>;
        let y = Box::new((2u64..=5).map(|i| Ok((i, i * 2)))) as Box<dyn Iterator<Item = _>>;

        let mut merged = MergedIterator::new(vec![y, x]).unwrap();
        assert_eq!(merged.next(), Some(Ok((0, 0))));
        assert_eq!(merged.next(), Some(Ok((1, 1))));
        assert_eq!(merged.next(), Some(Ok((2, 4))));
        assert_eq!(merged.next(), Some(Ok((3, 6))));
        assert_eq!(merged.next(), Some(Ok((4, 8))));
        assert_eq!(merged.next(), Some(Ok((5, 10))));
        assert_eq!(merged.next(), None);
    }
}
