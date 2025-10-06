use std::iter::Peekable;

use crate::DBError;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
/// An `Option`-like enum for the elements of a level.
/// The `None`-like variant, `Element::End`, is treated as larger than all other elements.
pub enum Element<T> {
    /// An actual element from a level.
    Regular(T),
    /// Indicates that the level has no more elements.
    End,
}

impl<T> Element<T> {
    fn unwrap(self) -> T {
        let Element::Regular(x) = self else {
            panic!("tried to unwrap `Element::End`")
        };
        x
    }
}

/// A list of peekable iterators (levels).
///
/// 0 is the topmost level. Levels at the top get priority over levels below.
pub trait Source<K, V> {
    fn num_levels(&self) -> usize;

    /// Returns the key at the given level, without advancing.
    fn peek(&mut self, i: usize) -> Result<Element<K>, DBError>;

    /// Returns the next key-value pair for the given level and advances the level.
    fn next(&mut self, i: usize) -> Result<Element<(K, V)>, DBError>;
}

/// A source that takes from the memtable and SSTs.
///
/// The topmost SST should be in front for `sst_scans`.
pub struct DBSource<M: Iterator, S: Iterator> {
    pub memtable_scan: Peekable<M>,
    pub sst_scans: Vec<Peekable<S>>,
}

impl<
    M: Iterator<Item = Result<(u64, u64), DBError>>,
    S: Iterator<Item = Result<(u64, u64), DBError>>,
> Source<u64, u64> for DBSource<M, S>
{
    fn num_levels(&self) -> usize {
        self.sst_scans.len() + 1
    }

    fn peek(&mut self, i: usize) -> Result<Element<u64>, DBError> {
        let next_element = if i == 0 {
            self.memtable_scan.peek()
        } else {
            self.sst_scans[i - 1].peek()
        };
        match next_element.cloned() {
            Some(Ok((k, _))) => Ok(Element::Regular(k)),
            Some(Err(e)) => Err(e),
            None => Ok(Element::End),
        }
    }

    fn next(&mut self, i: usize) -> Result<Element<(u64, u64)>, DBError> {
        let next_element = if i == 0 {
            self.memtable_scan.next()
        } else {
            self.sst_scans[i - 1].next()
        };
        match next_element {
            Some(Ok((k, v))) => Ok(Element::Regular((k, v))),
            Some(Err(e)) => Err(e),
            None => Ok(Element::End),
        }
    }
}

pub fn scan<K: Ord, V>(mut source: impl Source<K, V>) -> Result<Vec<(K, V)>, DBError> {
    let mut out = Vec::new();
    sweep(&mut source, &Element::End, 0, &mut out)?;
    Ok(out)
}

/// For a limit `m` and a starting level `s`,
///
/// (a) adds all keys `< m` from levels `>= s` onward to the output
///
/// and (b) advances each level `>= s` to remove all keys `<= m`.
///
/// Only the topmost instance of a key is added.
fn sweep<K: Ord, V>(
    source: &mut impl Source<K, V>,
    m: &Element<K>,
    s: usize,
    out: &mut Vec<(K, V)>,
) -> Result<(), DBError> {
    // Invariants for a key `k` on a level `s`:
    // (c) `k` is advanced only after all keys `<= k` on levels `>= s + 1` are advanced.
    // (d) `k` is advanced before any key `> k` on a level `>= s + 1` is advanced.
    // (All invariants hold true on the initial call.)
    // From the traversal order, it follows that:
    // (e) If `k < m`, then `k` on `s` is the topmost instance of the key.
    // (f) If `k == m`, then `k` on `s` is not the topmost instance of the key.

    // Reached the bottom, no more levels to work with.
    // (a) and (b) trivially kept since there are no levels `>= s`.
    if s == source.num_levels() {
        return Ok(());
    }

    loop {
        let k = &source.peek(s)?;
        if k < m {
            // Maintain (c): advance all keys `<= k` on levels `>= s + 1`.
            // Maintain (d): keys `> k` are left untouched.
            sweep(source, k, s + 1, out)?;
            // `next()` should correspond to `k`,
            // which, being strictly less than `m`, must be a regular element.
            out.push(source.next(s)?.unwrap());
        } else {
            // Ensure (b): all keys `<= m` are removed from levels `>= s + 1`.
            sweep(source, m, s + 1, out)?;
            if k == m {
                // Ensure (b): at this point, there are no more keys `<= m` from level `s`.
                source.next(s)?;
            }
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, hash::Hash, iter::Peekable, vec};

    use proptest::{
        collection::{hash_map, vec},
        proptest,
    };

    use super::*;

    struct MemSource {
        data: Vec<Peekable<vec::IntoIter<(u64, u64)>>>,
    }

    impl MemSource {
        fn new(data: Vec<Vec<(u64, u64)>>) -> Self {
            Self {
                data: data.into_iter().map(|v| v.into_iter().peekable()).collect(),
            }
        }
    }

    fn element_from_option<T>(x: Option<T>) -> Element<T> {
        match x {
            Some(x) => Element::Regular(x),
            None => Element::End,
        }
    }

    impl Source<u64, u64> for MemSource {
        fn num_levels(&self) -> usize {
            self.data.len()
        }
        fn peek(&mut self, i: usize) -> Result<Element<u64>, DBError> {
            Ok(element_from_option(self.data[i].peek().map(|&(k, _)| k)))
        }
        fn next(&mut self, i: usize) -> Result<Element<(u64, u64)>, DBError> {
            Ok(element_from_option(self.data[i].next()))
        }
    }

    #[test]
    fn test_empty() {
        assert_eq!(scan(MemSource::new(vec![vec![]])).unwrap(), &[]);
    }

    #[test]
    fn test_end_limit() {
        assert_eq!(
            scan(MemSource::new(vec![vec![(3, 1), (4, 1), (5, 9)]])).unwrap(),
            &[(3, 1), (4, 1), (5, 9)]
        );
    }

    #[test]
    fn test_regular_limit() {
        assert_eq!(
            scan(MemSource::new(vec![
                vec![(4, 2)],
                vec![(2, 1), (3, 1), (4, 1), (5, 9), (6, 9)],
            ]))
            .unwrap(),
            &[(2, 1), (3, 1), (4, 2), (5, 9), (6, 9)]
        );
    }

    fn oracle<K: Ord + Copy + Hash + Eq, V>(
        mut source: impl Source<K, V>,
    ) -> Result<Vec<(K, V)>, DBError> {
        let mut result = HashMap::new();
        for i in (0..source.num_levels()).rev() {
            while let Element::Regular((k, v)) = source.next(i)? {
                result.insert(k, v);
            }
        }
        let mut result: Vec<_> = result.into_iter().collect();
        result.sort_by_key(|&(k, _)| k);
        Ok(result)
    }

    #[test]
    fn test_recursive() {
        let data = vec![
            vec![(3, 1), (4, 1), (5, 9)],
            vec![(2, 6), (5, 3), (8, 5)],
            vec![(2, 3), (3, 9), (9, 7)],
        ];
        let expected = [(2, 6), (3, 1), (4, 1), (5, 9), (8, 5), (9, 7)];
        assert_eq!(oracle(MemSource::new(data.clone())).unwrap(), &expected);
        assert_eq!(scan(MemSource::new(data)).unwrap(), &expected);
    }

    proptest! {
        #[test]
        fn compare_against_oracle(
            data in vec(hash_map(0..8u64, 0..8u64, 0..10), 0..5)
        ) {
            let data: Vec<_> = data
                .into_iter()
                .map(|m| {
                    let mut v: Vec<_> = m.into_iter().collect();
                    v.sort();
                    v
                })
                .collect();
            let expected = oracle(MemSource::new(data.clone())).unwrap();
            assert_eq!(
                scan(MemSource::new(data.clone())).unwrap(),
                expected,
                "{data:?}"
            );
        }
    }
}
