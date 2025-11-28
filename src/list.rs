use crate::DbError;

const NULL: usize = usize::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryId(usize);

#[derive(Clone, Debug)]
struct Node<T> {
    prev: usize,
    next: usize,
    entry: T,
}

#[derive(Debug)]
/// Doubly linked list stored in a contiguous buffer with a fixed capacity.
///
/// Supports O(1) insertions, deletions, reordering.
pub struct List<T> {
    buffer: Vec<Option<Node<T>>>,
    free_list: Vec<usize>,
    front: usize,
    back: usize,
    size: usize,
}

impl<T: Clone> List<T> {
    /// Creates a new empty list with the given cpacity
    ///
    /// Returns `DbError::Oom` if not enough memory for
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(capacity)?;
        buffer.resize(capacity, None);

        let mut free_list = Vec::new();
        free_list.try_reserve_exact(capacity)?;
        free_list.extend(0..capacity);

        Ok(Self {
            buffer,
            free_list,
            front: NULL,
            back: NULL,
            size: 0,
        })
    }

    /// Returns the current size of the list.
    /// O(1) worst case.
    pub fn len(&self) -> usize {
        debug_assert_eq!(self.buffer.len(), self.free_list.len() + self.size);
        debug_assert_eq!(
            self.size,
            self.buffer.iter().filter(|n| n.is_some()).count()
        );
        debug_assert_eq!(
            self.free_list.len(),
            self.buffer.iter().filter(|n| n.is_none()).count()
        );

        self.size
    }

    /// Returns whether the list is empty.
    /// O(1) worst case.
    pub fn is_empty(&self) -> bool {
        if self.size == 0 {
            debug_assert_eq!(self.free_list.len(), self.buffer.len());
            debug_assert_eq!(self.front, NULL);
            debug_assert_eq!(self.back, NULL);

            true
        } else {
            debug_assert!(self.free_list.len() < self.buffer.len());
            debug_assert_ne!(self.front, NULL);
            debug_assert_ne!(self.back, NULL);

            false
        }
    }

    /// Gets the id and the entry of the front of the list, if the list is not empty.
    /// O(1) worst case.
    pub fn front(&self) -> Option<(EntryId, &T)> {
        let front_id = EntryId(self.front);
        self.get(front_id).map(|entry| (front_id, entry))
    }

    /// Gets the id and the entry of the back of the list, if the list is not empty.
    /// O(1) worst case.
    #[cfg(test)]
    pub fn back(&self) -> Option<(EntryId, &T)> {
        let back_id = EntryId(self.back);
        self.get(back_id).map(|entry| (back_id, entry))
    }

    /// Given an id to a valid entry, returns the id and the value of the next entry in the list
    /// if it exists.
    /// O(1) worst case.
    /// Panics if given an invalid id.
    pub fn get_next(&self, id: EntryId) -> Option<(EntryId, &T)> {
        let next_idx = self.buffer[id.0].as_ref().unwrap().next;
        let next_id = EntryId(next_idx);
        self.get(next_id).map(|entry| (next_id, entry))
    }

    /// Gets the entry stored at the given id, or `None` if given an invalid id
    /// O(1) worst case.
    pub fn get(&self, id: EntryId) -> Option<&T> {
        self.buffer
            .get(id.0)
            .and_then(|node| node.as_ref().map(|node| &node.entry))
    }

    /// Deletes an entry from the queue given its id.
    /// O(1) worst case.
    /// Panics if given an invalid id
    pub fn delete(&mut self, node_id: EntryId) -> T {
        let node_idx = node_id.0;
        let node = self.buffer[node_idx].take().unwrap();

        // Update queue structure
        self.size -= 1;
        self.free_list.push(node_idx);

        if self.front == node_idx {
            self.front = node.next;
        }

        if self.back == node_idx {
            self.back = node.prev;
        }

        // Unlink node from neighbors
        if let Some(prev) = self.buffer.get_mut(node.prev) {
            prev.as_mut().unwrap().next = node.next;
        }

        if let Some(next) = self.buffer.get_mut(node.next) {
            next.as_mut().unwrap().prev = node.prev;
        }

        node.entry
    }

    /// Removes and returns the entry at the front of the list if there is one.
    /// O(1) worst case.
    pub fn pop_front(&mut self) -> Option<T> {
        if self.is_empty() {
            None
        } else {
            let front_id = EntryId(self.front);
            Some(self.delete(front_id))
        }
    }

    /// Pushes a new entry into the back of the list and returns its id.
    /// O(1) worst case.
    /// Panics if out of capacity
    pub fn push_back(&mut self, entry: T) -> EntryId {
        let idx = self.free_list.pop().unwrap();

        let node = Node {
            prev: self.back,
            next: NULL,
            entry,
        };
        self.buffer[idx] = Some(node);

        if let Some(prev) = self.buffer.get_mut(self.back) {
            prev.as_mut().unwrap().next = idx;
        }
        self.back = idx;

        self.size += 1;

        if self.size == 1 {
            self.front = idx;
        }

        EntryId(idx)
    }

    /// Moves the entry at the given id to the back of the queue.
    /// Doesn't invalidate the id.
    /// O(1) worst case.
    /// Panics if given an invalid id.
    pub fn move_to_back(&mut self, node_id: EntryId) {
        let entry = self.delete(node_id);
        let new_id = self.push_back(entry);
        debug_assert_eq!(node_id, new_id);
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::test_util::assert_panics;

    use super::*;

    #[test]
    fn test_push_in_order() -> Result<()> {
        let mut list = List::new(10)?;
        list.push_back(1);
        list.push_back(2);
        list.push_back(3);

        assert_eq!(list.pop_front(), Some(1));
        assert_eq!(list.pop_front(), Some(2));
        assert_eq!(list.pop_front(), Some(3));
        assert_eq!(list.pop_front(), None);

        list.push_back(4);
        list.push_back(5);

        assert_eq!(list.pop_front(), Some(4));
        assert_eq!(list.pop_front(), Some(5));
        assert_eq!(list.pop_front(), None);

        Ok(())
    }

    #[test]
    fn test_reordering() -> Result<()> {
        let mut list = List::new(10)?;
        let one = list.push_back(1);
        let two = list.push_back(2);
        let three = list.push_back(3);

        list.move_to_back(three);
        list.move_to_back(two);
        list.move_to_back(one);

        assert_eq!(list.pop_front(), Some(3));
        assert_eq!(list.pop_front(), Some(2));
        assert_eq!(list.pop_front(), Some(1));
        assert_eq!(list.pop_front(), None);

        Ok(())
    }

    #[test]
    fn test_deletion() -> Result<()> {
        let mut list = List::new(10)?;

        let one = list.push_back(1);
        let two = list.push_back(2);
        let three = list.push_back(3);

        list.delete(two);
        list.delete(three);
        list.delete(one);

        assert!(list.is_empty());

        list.push_back(1);
        let two = list.push_back(2);
        list.push_back(3);

        list.delete(two);

        assert_eq!(list.pop_front(), Some(1));
        assert_eq!(list.pop_front(), Some(3));
        assert_eq!(list.pop_front(), None);

        Ok(())
    }

    #[test]
    fn test_get_next() -> Result<()> {
        let mut list = List::new(10)?;

        list.push_back(1);
        list.push_back(2);
        list.push_back(3);

        let mut curr = list.front().unwrap();
        assert_eq!(curr.1, &1);

        curr = list.get_next(curr.0).unwrap();
        assert_eq!(curr.1, &2);

        curr = list.get_next(curr.0).unwrap();
        assert_eq!(curr.1, &3);

        assert!(list.get_next(curr.0).is_none());

        Ok(())
    }

    #[test]
    fn test_capacity() -> Result<()> {
        let mut list = List::new(3)?;

        list.push_back(1);
        list.push_back(2);
        list.push_back(3);

        assert_panics(|| _ = list.push_back(4));

        list = List::new(0)?;

        assert_panics(|| _ = list.push_back(1));

        Ok(())
    }

    #[test]
    fn test_chaotic() -> Result<()> {
        let mut list = List::new(10)?;

        let a = list.push_back('a');
        list.push_back('b');
        list.push_back('c');
        let d = list.push_back('d');
        let e = list.push_back('e');
        list.push_back('f');
        let g = list.push_back('g');
        list.push_back('h');

        list.move_to_back(a);
        let new_b = list.push_back('b');
        list.move_to_back(d);
        assert_eq!(list.pop_front(), Some('b'));
        let new_a = list.push_back('a');
        assert_eq!(list.pop_front(), Some('c'));
        list.push_back('i');
        list.push_back('c');
        list.move_to_back(e);
        assert_eq!(list.delete(new_b), 'b');
        list.pop_front();
        assert_eq!(list.delete(d), 'd');

        assert_eq!(list.len(), 7);
        assert!(!list.is_empty());

        assert_eq!(list.front(), Some((g, &'g')));
        assert_eq!(list.get(a), Some(&'a'));
        assert_eq!(list.get_next(a), Some((new_a, &'a')));
        assert_eq!(list.get_next(e), None);

        assert_eq!(list.pop_front(), Some('g'));
        assert_eq!(list.pop_front(), Some('h'));
        assert_eq!(list.pop_front(), Some('a'));
        assert_eq!(list.pop_front(), Some('a'));
        assert_eq!(list.pop_front(), Some('i'));
        assert_eq!(list.pop_front(), Some('c'));
        assert_eq!(list.pop_front(), Some('e'));

        assert_eq!(list.len(), 0);
        assert!(list.is_empty());

        assert_eq!(list.front(), None);
        assert_eq!(list.get(a), None);

        assert_panics(|| _ = list.delete(a));
        assert_panics(|| list.move_to_back(a));
        assert_panics(|| _ = list.get_next(a));

        Ok(())
    }
}
