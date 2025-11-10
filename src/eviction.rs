use std::path::PathBuf;

use crate::{
    DbError,
    hashtable::HashTable,
    list::{EntryId, List},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
            debug_assert_eq!((&page.0, page.1), (&path, page_number));
            self.map_out.remove(path, page_number);
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
                // If |a_in| > k_in, evict from a_in
                // or if a_m is empty and a_in is our only option
                if self.eviction.a_in.len() > self.eviction.k_in
                    || (!self.eviction.a_in.is_empty() && self.eviction.a_m.is_empty())
                {
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
                }
                // If done with a_in, switch to a_m
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

    fn make_page(id: usize) -> (PathBuf, usize) {
        (PathBuf::from(format!("p{}", id)), id)
    }

    #[test]
    fn test_a_in_fifo() -> Result<()> {
        let mut ev = Eviction::new(8)?;

        // Insert several new pages to A_in
        let (path, num) = make_page(1);
        let p1 = ev.insert_new(path, num);
        let (path, num) = make_page(2);
        let p2 = ev.insert_new(path, num);
        let (path, num) = make_page(3);
        let p3 = ev.insert_new(path, num);

        assert!(matches!(p1, EvictionId::AIn(_)));
        assert!(matches!(p2, EvictionId::AIn(_)));
        assert!(matches!(p3, EvictionId::AIn(_)));

        assert!(ev.a_in.len() == 3);
        assert!(ev.a_out.is_empty());
        assert!(ev.a_m.is_empty());

        ev.touch(p2);

        // Evict from A_in is FIFO, even though we touched page 2
        let victim = ev.choose_victim().next().unwrap().0;
        assert_eq!(victim, p1);
        ev.evict(victim);
        let victim = ev.choose_victim().next().unwrap().0;
        assert_eq!(victim, p2);
        ev.evict(victim);
        let victim = ev.choose_victim().next().unwrap().0;
        assert_eq!(victim, p3);
        ev.evict(victim);

        assert!(ev.a_in.is_empty());

        Ok(())
    }

    #[test]
    fn test_evict_to_a_out() -> Result<()> {
        let mut ev = Eviction::new(4)?;

        for i in 0..4 {
            let (path, num) = make_page(i);
            ev.insert_new(path, num);
        }

        // Eviction from A_in
        let first_victim = ev.choose_victim().next().unwrap();
        assert!(first_victim.1.1 == 0);
        ev.evict(first_victim.0);

        // Should now appear in A_out
        assert!(ev.a_out.len() == 1);
        assert!(ev.a_out.front().unwrap().1 == &make_page(0));

        Ok(())
    }

    #[test]
    fn test_reaccess_moves_to_am() -> Result<()> {
        let mut ev = Eviction::new(6)?;

        // Insert to A_in
        let (path, num) = make_page(0);
        ev.insert_new(path.clone(), num);

        // Evict to A_out
        let victim = ev.choose_victim().next().unwrap();

        assert_eq!(&(path.clone(), num), victim.1);
        ev.evict(victim.0);

        // Now that page should be in A_out
        assert!(ev.map_out.get(path.clone(), num).is_some());

        // Re-accessing should move it to A_m
        let new_id = ev.insert_new(path.clone(), num);
        assert!(matches!(new_id, EvictionId::AM(_)));
        assert!(ev.a_m.len() == 1);

        Ok(())
    }

    #[test]
    fn test_am_lru() -> Result<()> {
        let mut ev = Eviction::new(8)?;

        let mut ids = Vec::new();
        for i in 0..3 {
            // Insert to A_in
            let (path, num) = make_page(i);
            let id = ev.insert_new(path.clone(), num);

            // Evict to A_out
            ev.evict(id);

            // Re insert to A_m
            let id = ev.insert_new(path, num);
            ids.push(id);
        }

        assert!(ev.a_m.len() == 3);

        // Simulate re-access
        ev.touch(ids[0]);

        // After touching, should move to back (most recently used)
        let last_item = ev.a_m.back().unwrap();
        assert_eq!(last_item.1, &make_page(0));

        ev.touch(ids[1]);

        let last_item = ev.a_m.back().unwrap();
        assert_eq!(last_item.1, &make_page(1));

        ev.touch(ids[2]);

        let last_item = ev.a_m.back().unwrap();
        assert_eq!(last_item.1, &make_page(2));

        Ok(())
    }

    #[test]
    fn test_choose_victim_order() -> Result<()> {
        let mut ev = Eviction::new(10)?;

        // Insert 10 pages
        for i in 0..10 {
            let (path, num) = make_page(i);
            ev.insert_new(path, num);
        }

        // Move 5 pages to A_m
        for i in 0..5 {
            let victim = ev.choose_victim().next().unwrap();
            let (path, num) = victim.1.clone();
            assert_eq!(num, i);

            ev.evict(victim.0);
            ev.insert_new(path, num);
        }

        // Check eviction order matches FIFO/LRU expectations
        let mut chooser = ev.choose_victim();

        assert!(matches!(chooser.next(), Some((EvictionId::AIn(_), p_n)) if p_n == &make_page(5)));
        assert!(matches!(chooser.next(), Some((EvictionId::AIn(_), p_n)) if p_n == &make_page(6)));
        assert!(matches!(chooser.next(), Some((EvictionId::AIn(_), p_n)) if p_n == &make_page(7)));
        assert!(matches!(chooser.next(), Some((EvictionId::AIn(_), p_n)) if p_n == &make_page(8)));
        assert!(matches!(chooser.next(), Some((EvictionId::AIn(_), p_n)) if p_n == &make_page(9)));
        assert!(matches!(chooser.next(), Some((EvictionId::AM(_), p_n)) if p_n == &make_page(0)));
        assert!(matches!(chooser.next(), Some((EvictionId::AM(_), p_n)) if p_n == &make_page(1)));
        assert!(matches!(chooser.next(), Some((EvictionId::AM(_), p_n)) if p_n == &make_page(2)));
        assert!(matches!(chooser.next(), Some((EvictionId::AM(_), p_n)) if p_n == &make_page(3)));
        assert!(matches!(chooser.next(), Some((EvictionId::AM(_), p_n)) if p_n == &make_page(4)));
        assert!(chooser.next().is_none());
        assert!(chooser.next().is_none());

        Ok(())
    }

    #[test]
    fn test_a_out_capacity() -> Result<()> {
        let mut ev = Eviction::new(8)?;
        let k_out = ev.k_out;

        // Fill beyond A_out capacity
        for i in 0..(k_out + 3) {
            let (path, num) = make_page(i);
            ev.insert_new(path, num);
            let victim = ev.choose_victim().next().unwrap();
            ev.evict(victim.0);
        }

        // A_out should not exceed k_out
        assert!(ev.a_out.len() == k_out);

        Ok(())
    }
}
