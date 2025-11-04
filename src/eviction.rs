use std::path::PathBuf;

use crate::{
    DbError,
    hashtable::HashTable,
    list::{EntryId, List},
};

#[derive(Debug, Clone, Copy)]
pub enum EvictionId {
    AIn(EntryId),
    AM(EntryId),
}

/// Implements the 2Q eviction policy. From [this paper](https://dl.acm.org/doi/10.5555/645920.672996).
///
/// Maintains a FIFO queue called A_in for pages that have just been accessed.
/// Another FIFO queue called A_out for pages that have been evicted from A_in recently.
/// And a LRU queue called A_m for pages that were accessed again while in A_out (i.e. hot pages).
///
/// A_in has a capacity of k_in which we set to ~25% of the total capacity of the buffer pool.
/// A_in is not kept strictly within capacity k_in.
///
/// A_out has a capacity of k_out which we set to ~50% of the total capacity of the buffer pool.
/// A_out *is* kept strictly within capacity k_out.
///
/// On eviction, if A_in has at least k_in elements, we evict the from A_in. Otherwise, we evict
/// the from A_m.
///
/// If we evict a page from A_in (but not from A_m), we place it on A_out. Possibly replacing the oldest
/// reference in A_out if it is at capacity.
/// A_out only stores whether we've recently evicted a page, but not the actual page contents, so it is
/// not too much memory.
pub struct Eviction {
    a_in: List<(PathBuf, usize)>,
    a_m: List<(PathBuf, usize)>,
    a_out: List<(PathBuf, usize)>,
    map_out: HashTable<EntryId>,
    k_in: usize,
    k_out: usize,
}

impl Eviction {
    /// Creates a new 2Q eviction handler with a given max capacity.
    /// Returns `DbError::Oom` if allocation fails
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        let k_in = capacity / 4 + 1; // ~25%
        let k_out = capacity / 2 + 1; // ~50%

        let map_out = HashTable::new(k_out)?;

        Ok(Self {
            a_in: List::new(capacity)?, // not k_in because it might grow past that as long as we haven't evicted
            a_m: List::new(capacity)?,
            a_out: List::new(k_out)?,
            map_out,
            k_in,
            k_out,
        })
    }

    /// Returns a `VictimChooser` that allows you to select a victim to evict, ordered by how 2Q
    /// would have evicted them.
    pub fn choose_victim(&'_ self) -> VictimChooser<'_> {
        VictimChooser::new(self)
    }

    pub fn evict(&mut self, victim: EvictionId) {
        match victim {
            EvictionId::AIn(id) => {
                // Make space in a_out
                if self.a_out.len() >= self.k_out {
                    // a_out and map_out can't be empty
                    let evicted_a_out = self.a_out.pop_front().unwrap();
                    self.map_out.remove(evicted_a_out.0, evicted_a_out.1);
                }

                let evicted_a_in = self.a_in.delete(id);
                let a_out_id = self.a_out.push_back(evicted_a_in.clone());
                self.map_out
                    .insert(evicted_a_in.0, evicted_a_in.1, a_out_id);
            }
            EvictionId::AM(id) => {
                self.a_m.delete(id);
            }
        }
    }

    /// Inserts a new page into the eviction handler. Must be a page that hasn't been inserted before.
    /// Panics if inserting above capacity
    pub fn insert_new(&mut self, path: PathBuf, page_number: usize) -> EvictionId {
        if let Some(&idx_out) = self.map_out.get(&path, page_number) {
            let page = self.a_out.delete(idx_out);
            let id = self.a_m.push_back(page);
            EvictionId::AM(id)
        } else {
            let id = self.a_in.push_back((path, page_number));
            EvictionId::AIn(id)
        }
    }

    /// Mark a page as used
    pub fn touch(&mut self, id: EvictionId) {
        match id {
            EvictionId::AIn(_) => {}
            EvictionId::AM(id) => self.a_m.move_to_back(id),
        }
    }
}

pub struct VictimChooser<'a> {
    eviction: &'a Eviction,
    last_id: Option<EvictionId>,
    ended: bool,
}

impl<'a> VictimChooser<'a> {
    fn new(eviction: &'a Eviction) -> Self {
        Self {
            eviction,
            last_id: None,
            ended: false,
        }
    }
}
impl<'a> Iterator for VictimChooser<'a> {
    type Item = (EvictionId, &'a (PathBuf, usize));

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }

        match self.last_id {
            None => {
                // If |a_in| over k_in, evict from a_in
                if self.eviction.a_in.len() > self.eviction.k_in {
                    // At this point a_in can't be empty
                    let (id, front) = self.eviction.a_in.front().unwrap();
                    let eviction_id = EvictionId::AIn(id);
                    self.last_id = Some(eviction_id);
                    Some((eviction_id, front))
                }
                // Otherwise try evict from a_m
                else if let Some((id, front)) = self.eviction.a_m.front() {
                    let eviction_id = EvictionId::AM(id);
                    self.last_id = Some(eviction_id);
                    Some((eviction_id, front))
                } else {
                    self.ended = true;
                    self.last_id = None;
                    None
                }
            }
            Some(EvictionId::AIn(id)) => {
                // They didn't want AIn(id) as the victim, choose next in a_in
                if let Some((next_id, next_entry)) = self.eviction.a_in.get_next(id) {
                    let eviction_id = EvictionId::AIn(next_id);
                    self.last_id = Some(eviction_id);
                    Some((eviction_id, next_entry))
                } else {
                    self.ended = true;
                    self.last_id = None;
                    None
                }
            }
            Some(EvictionId::AM(id)) => {
                // They didn't want AM(id) as the victim, choose next in a_m
                if let Some((next_id, next_entry)) = self.eviction.a_m.get_next(id) {
                    let eviction_id = EvictionId::AM(next_id);
                    self.last_id = Some(eviction_id);
                    Some((eviction_id, next_entry))
                } else {
                    self.ended = true;
                    self.last_id = None;
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    #[test]
    fn test_evict_a_in() -> Result<()> {
        let mut eviction = Eviction::new(20)?;
        eviction.insert_new("1".into(), 0);

        // TODO

        Ok(())
    }
}
