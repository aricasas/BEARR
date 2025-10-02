use std::{cmp::Ordering, ops::RangeInclusive};

use crate::DBError;

/// An in-memory memtable.
///
/// Stores key-value pairs in a Red-Black tree.
///
/// The Red-Black tree design and implementation is inspired by this [reference](https://web.archive.org/web/20190207151651/http://www.eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx).
/// The algorithm for scanning the tree is inspired by this [reference](https://en.wikipedia.org/wiki/Tree_traversal#Advancing_to_the_next_or_previous_node).
#[derive(Debug)]
pub struct MemTable<K: Ord + Clone + Default, V: Clone + Default> {
    /// Index to the root element of the tree. If tree is empty then `root==NULL`.
    root: usize,
    /// Backing storage for the nodes in the tree.
    nodes: Vec<Node<K, V>>,
}

/// The nodes in our Red-Black tree.
#[derive(Debug)]
struct Node<K, V> {
    key: K,
    value: V,
    /// Indices to left and right nodes, in that order.
    link: [usize; 2],
    /// True if node is colored red, false if node is colored black.
    red: bool,
}

/// Constant representing an impossible index into the `nodes` Vec in a `MemTable`
const NULL: usize = usize::MAX;
/// Constant used to access the left child of a node
const LEFT: usize = 0;
/// Constant used to access the right child of a node
const RIGHT: usize = 1;

impl<K: Ord + Clone + Default, V: Clone + Default> MemTable<K, V> {
    /// Creates a new empty `MemTable`.
    ///
    /// Allocates enough space to hold `memtable_size` key-value pairs.
    /// If allocation fails, returns `DBError::OOM`.
    pub fn new(capacity: usize) -> Result<Self, DBError> {
        let mut nodes = Vec::new();
        nodes
            .try_reserve_exact(capacity)
            .map_err(|_| DBError::OOM)?;

        Ok(Self { root: NULL, nodes })
    }

    /// Searches `MemTable` for key-value pair with given key and returns the value associated, if it exists.
    pub fn get(&self, key: K) -> Option<V> {
        let mut curr = self.root;

        while curr != NULL {
            let node = self.node(curr);
            match key.cmp(&node.key) {
                Ordering::Less => curr = node.link[LEFT],
                Ordering::Greater => curr = node.link[RIGHT],
                Ordering::Equal => return Some(node.value.clone()),
            }
        }

        None
    }

    /// Updates or inserts a key-value pair into the `MemTable`.
    ///
    /// If there is a pair with matching key already, it changes the value in-place.
    /// Otherwise, it inserts the new pair.
    ///
    /// Returns `DBError::MemTableFull` if inserting would cause the Memtable to exceed capacity.
    ///
    /// Based on the Top-Down `jsw_insert` implementation from [here](https://web.archive.org/web/20190207151651/http://www.eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx).
    pub fn put(&mut self, key: K, value: V) -> Result<(), DBError> {
        if self.root == NULL {
            self.root = self.make_node(key, value)?;
            self.node_mut(self.root).red = false;
            return Ok(());
        }

        // Dummy root
        let mut head = Node {
            key: K::default(),
            value: V::default(),
            link: [NULL, self.root],
            red: false,
        };

        // Cursor and ancestors
        let mut q = self.root;
        let mut p = NULL;
        let mut g = NULL;
        let mut t = NULL;

        // Current and last direction of traversing tree
        let mut dir = 0;
        let mut last = 0;

        // Traverse tree downwards in one pass
        loop {
            if q == NULL {
                // Insert new node as leaf
                q = self.make_node(key.clone(), value.clone())?;
                self.node_mut(p).link[dir] = q;
            } else {
                let q_node = self.node(q);
                let left = q_node.link[LEFT];
                let right = q_node.link[RIGHT];

                // Color flip
                if self.is_red(left) && self.is_red(right) {
                    self.node_mut(q).red = true;
                    self.node_mut(left).red = false;
                    self.node_mut(right).red = false;
                }
            }

            if self.is_red(q) && self.is_red(p) {
                // Red violation

                // dir2 is RIGHT iff g is right child of t
                let dir2 = usize::from(self.node_or(t, &head).link[RIGHT] == g);

                if q == self.node(p).link[last] {
                    self.node_mut_or(t, &mut head).link[dir2] = self.single_rotation(g, 1 - last);
                } else {
                    self.node_mut_or(t, &mut head).link[dir2] = self.double_rotation(g, 1 - last);
                }
            }

            if self.node(q).key == key {
                // Found key
                self.node_mut(q).value = value;
                break;
            }

            // Update traversal directions
            last = dir;
            dir = usize::from(self.node(q).key < key);

            // Update cursors
            t = g;
            g = p;
            p = q;
            q = self.node(q).link[dir];
        }

        self.root = head.link[1];

        self.node_mut(self.root).red = false;

        Ok(())
    }

    /// Returns the number of key-value pairs currently stored in `MemTable`.
    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    /// Returns an iterator of key-value pairs over the given range of keys.
    /// The pairs returned are ordered increasing by their key.
    ///
    /// Returns `DBError::OOM` if there is not enough memory to store the state of the iterator.
    pub fn scan(&self, range: RangeInclusive<K>) -> Result<MemTableIter<'_, K, V>, DBError> {
        MemTableIter::new(self, range)
    }

    /// Creates a new node in the memtable.
    ///
    /// Returns `DBError::MemTableFull` if this would make the memtable exceed capacity.
    fn make_node(&mut self, key: K, value: V) -> Result<usize, DBError> {
        if self.size() >= self.nodes.capacity() {
            return Err(DBError::MemTableFull);
        }

        let node = Node {
            key,
            value,
            link: [NULL, NULL],
            red: true,
        };
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    /// Access a given node immutably.
    ///
    /// Panics if `node` doesn't point to a valid node in the `MemTable`.
    fn node(&self, node: usize) -> &Node<K, V> {
        &self.nodes[node]
    }

    /// Access a given node mutably.
    ///
    /// Panics if `node` doesn't point to a valid node in the `MemTable`.
    fn node_mut(&mut self, node: usize) -> &mut Node<K, V> {
        &mut self.nodes[node]
    }

    /// Access a given node immutably. If `node==NULL`, accesses `other` instead.
    fn node_or<'a: 'b, 'b>(&'a self, node: usize, other: &'b Node<K, V>) -> &'b Node<K, V> {
        if node == NULL { other } else { self.node(node) }
    }

    /// Access a given node mutably. If `node==NULL`, accesses `other` instead.
    fn node_mut_or<'a: 'b, 'b>(
        &'a mut self,
        node: usize,
        other: &'b mut Node<K, V>,
    ) -> &'b mut Node<K, V> {
        if node == NULL {
            other
        } else {
            self.node_mut(node)
        }
    }

    /// Returns true iff `node` points to a valid red node in the `MemTable`.
    fn is_red(&self, node: usize) -> bool {
        node != NULL && self.node(node).red
    }

    /// Performs a single rotation in the given direction to the tree rooted at `node`.
    ///
    /// Panics if `node` doesn't point to a valid node in the `MemTable`
    fn single_rotation(&mut self, node: usize, dir: usize) -> usize {
        let save = self.node(node).link[1 - dir];

        self.node_mut(node).link[1 - dir] = self.node(save).link[dir];
        self.node_mut(save).link[dir] = node;

        self.node_mut(node).red = true;
        self.node_mut(save).red = false;

        save
    }

    /// Performs a single rotation in the given direction to the tree rooted at `node`.
    ///
    /// Panics if `node` doesn't point to a valid node in the `MemTable`.
    fn double_rotation(&mut self, root: usize, dir: usize) -> usize {
        self.node_mut(root).link[1 - dir] =
            self.single_rotation(self.node(root).link[1 - dir], 1 - dir);

        self.single_rotation(root, dir)
    }
}

pub struct MemTableIter<'a, K: Ord + Clone + Default, V: Clone + Default> {
    /// memtable over which we are iterating
    memtable: &'a MemTable<K, V>,
    /// A stack of ancestors to the current node.
    /// The top of the stack is the current node. If the stack is empty, the iterator is done.
    stack: Vec<usize>,
    /// The end of the range we iterate over.
    end: K,
}
impl<'a, K: Ord + Clone + Default, V: Clone + Default> Iterator for MemTableIter<'a, K, V> {
    type Item = Result<(&'a K, &'a V), DBError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.in_order_iterate().transpose()
    }
}

impl<'a, K: Ord + Clone + Default, V: Clone + Default> MemTableIter<'a, K, V> {
    /// Create an iterator that returns key-value pairs from `memtable` with keys in the given range, sorted increasing by key.
    ///
    /// Returns `DBError::OOM` if not enough memory for the stack
    fn new(memtable: &'a MemTable<K, V>, range: RangeInclusive<K>) -> Result<Self, DBError> {
        if range.start() > range.end() {
            return Err(DBError::InvalidScanRange);
        }

        let key = range.start();
        let mut curr = memtable.root;
        let mut stack = Vec::new();

        // Search for the key in the tree while storing visited nodes on stack
        while curr != NULL {
            stack.try_reserve(1).map_err(|_| DBError::OOM)?;
            stack.push(curr);

            let node = memtable.node(curr);

            match key.cmp(&node.key) {
                Ordering::Less => curr = node.link[LEFT],
                Ordering::Greater => curr = node.link[RIGHT],
                Ordering::Equal => {
                    return Ok(MemTableIter {
                        memtable,
                        stack,
                        end: range.end().clone(),
                    });
                }
            }
        }

        Ok(MemTableIter {
            memtable,
            stack: Vec::new(),
            end: range.end().clone(),
        })
    }

    /// Return key-value pair of node at the top of the stack,
    /// and move stack so the new top is the next inorder node of the memtable.
    ///
    /// Returns `DBError::OOM` if not enough memory for the stack
    fn in_order_iterate(&mut self) -> Result<Option<(&'a K, &'a V)>, DBError> {
        if let Some(&curr) = self.stack.last() {
            let curr_node = self.memtable.node(curr);

            // key-value pair we will return
            let kv_pair = (&curr_node.key, &curr_node.value);

            if curr_node.key > self.end {
                self.stack = Vec::new();
                return Ok(None);
            }

            let right = curr_node.link[RIGHT];

            if right == NULL {
                // If curr has no right child, go to the closest rightwards ancestor
                self.go_to_rightwards_ancestor();
                Ok(Some(kv_pair))
            } else {
                // If curr has right child, go to its leftmost child
                self.go_to_leftmost_child(right)?;
                Ok(Some(kv_pair))
            }
        } else {
            Ok(None)
        }
    }

    /// Appends to the stack until we reach the leftmost child of `node`.
    /// This child could be `node` itself.
    ///
    /// Returns `DBError::OOM` if not enough memory for the stack
    fn go_to_leftmost_child(&mut self, node: usize) -> Result<(), DBError> {
        let mut curr = node;
        loop {
            self.stack.try_reserve(1).map_err(|_| DBError::OOM)?;
            self.stack.push(curr);

            // Traverse left in the tree
            curr = self.memtable.node(curr).link[LEFT];

            if curr == NULL {
                return Ok(());
            }
        }
    }

    /// Pops the stack until we get to a node that is further right than us.
    /// If there is no ancestor further right than us, pops the stack until it's empty.
    ///
    /// Panics if stack is empty.
    fn go_to_rightwards_ancestor(&mut self) {
        loop {
            let curr = self.stack.pop().unwrap();

            if self
                .stack
                .last()
                .is_none_or(|&parent| self.memtable.node(parent).link[LEFT] == curr)
            {
                // Parent of current node has it as a left child, or stack is empty
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small() -> Result<(), DBError> {
        let mut memtable: MemTable<u64, u64> = MemTable::new(5)?;

        // Insert one node
        memtable.put(0, 0)?;
        dbg!(&memtable);

        // Update node
        memtable.put(0, 1)?;
        dbg!(&memtable);

        // Insert three nodes
        for i in 0..3 {
            memtable.put(5 + i, 10 + i)?;
            dbg!(&memtable);
        }

        // Check memtable has 4 nodes and is a valid red black tree
        assert_eq!(memtable.size(), 4);
        validate_red_black(&memtable, memtable.root).unwrap();

        Ok(())
    }

    #[test]
    fn test_large() -> Result<(), DBError> {
        let mut memtable: MemTable<u64, u64> = MemTable::new(5_000_000)?;

        for i in 0..4_000_000 {
            memtable.put(i, i * 10)?;
        }

        for i in 1_000_000..3_000_000 {
            memtable.put(i, i * 20)?;
        }

        for i in 0..1_000_000 {
            assert_eq!(memtable.get(i), Some(i * 10));
        }
        for i in 1_000_000..3_000_000 {
            assert_eq!(memtable.get(i), Some(i * 20));
        }
        for i in 3_000_000..4_000_000 {
            assert_eq!(memtable.get(i), Some(i * 10));
        }

        memtable.put(10_000_000, 12345)?;
        assert_eq!(memtable.get(10_000_000), Some(12345));

        Ok(())
    }

    #[test]
    fn test_insert_in_order() -> Result<(), DBError> {
        let mut memtable: MemTable<u64, u64> = MemTable::new(100)?;

        // Insert 100 nodes
        for i in 0..100 {
            assert_eq!(memtable.size(), i as usize);
            memtable.put(i, i * 10)?;
            assert_eq!(memtable.size(), i as usize + 1);
            validate_red_black(&memtable, memtable.root).unwrap();
        }

        // Check correct values stored
        for i in 0..100 {
            assert_eq!(memtable.get(i), Some(i * 10));
        }

        // Check get doesn't return when at wrong keys
        for i in 200..300 {
            assert_eq!(memtable.get(i), None);
        }

        Ok(())
    }

    #[test]
    fn test_insert_in_reverse() -> Result<(), DBError> {
        let mut memtable: MemTable<u64, u64> = MemTable::new(100)?;

        // Insert 100 nodes
        for i in (0..100).rev() {
            memtable.put(i, i * 10)?;
            validate_red_black(&memtable, memtable.root).unwrap();
        }

        // Check correct values stored
        for i in 0..100 {
            assert_eq!(memtable.get(i), Some(i * 10));
        }

        // Check get doesn't return when at wrong keys
        for i in 200..300 {
            assert_eq!(memtable.get(i), None);
        }

        Ok(())
    }

    #[test]
    fn test_update() -> Result<(), DBError> {
        let mut memtable: MemTable<u64, u64> = MemTable::new(100)?;

        // Insert 100 nodes
        for i in 0..100 {
            memtable.put(i, i * 10)?;
        }
        assert_eq!(memtable.size(), 100);

        // Update the value of every other node
        for i in 0..100 {
            if i % 2 == 0 {
                memtable.put(i, i * 20)?;
                validate_red_black(&memtable, memtable.root).unwrap();
            }
        }

        // Check all keys map to correct value
        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(memtable.get(i), Some(i * 20));
            } else {
                assert_eq!(memtable.get(i), Some(i * 10));
            }
        }

        Ok(())
    }

    #[test]
    fn test_full_capacity() -> Result<(), DBError> {
        let mut memtable: MemTable<u64, u64> = MemTable::new(100)?;

        // Fill memtable
        for i in 0..100 {
            memtable.put(i, i * 10)?;
        }
        assert_eq!(memtable.size(), 100);

        // Updating existing node
        assert_eq!(memtable.put(20, 200), Ok(()));

        // Try to insert node when full produces error
        assert_eq!(memtable.put(150, 200), Err(DBError::MemTableFull));

        // Check correct values still stored
        for i in 0..100 {
            assert_eq!(memtable.get(i), Some(i * 10));
        }

        validate_red_black(&memtable, memtable.root).unwrap();

        Ok(())
    }

    #[test]
    fn test_scan_valid_ranges() -> Result<(), DBError> {
        let mut memtable: MemTable<u64, u64> = MemTable::new(100)?;

        // Insert 100 nodes
        for i in 0..100 {
            memtable.put(i, i * 10)?;
        }

        // Test all possible ranges
        for lower in 0..105 {
            for upper in lower..105 {
                let mut scan = memtable.scan(lower..=upper)?;

                if lower >= 100 {
                    assert!(scan.next().is_none());
                    continue;
                }

                for i in lower..=upper.min(99) {
                    let (&k, &v) = scan.next().unwrap()?;

                    assert_eq!(k, i);
                    assert_eq!(v, i * 10);
                }

                assert!(scan.next().is_none());
            }
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::reversed_empty_ranges)]
    fn test_scan_invalid_ranges() -> Result<(), DBError> {
        let mut memtable: MemTable<i64, u64> = MemTable::new(100)?;

        // Insert 100 nodes
        for i in 0..100 {
            memtable.put(i, i as u64 * 10)?;
        }

        // Test several invalid scan ranges
        assert!(matches!(
            memtable.scan(20..=10),
            Err(DBError::InvalidScanRange)
        ));

        assert!(matches!(
            memtable.scan(10..=0),
            Err(DBError::InvalidScanRange)
        ));

        assert!(matches!(
            memtable.scan(100..=99),
            Err(DBError::InvalidScanRange)
        ));

        assert!(matches!(
            memtable.scan(99..=98),
            Err(DBError::InvalidScanRange)
        ));

        assert!(matches!(
            memtable.scan(0..=-1),
            Err(DBError::InvalidScanRange)
        ));

        Ok(())
    }

    /// Checks that the tree rooted at `root` in the `MemTable` is a valid binary tree and satisfies the Red-Black conditions.
    ///
    /// If the tree is valid, returns the black height of the tree.
    ///
    /// Based on the implementation of `jsw_rb_assert` from [here](https://web.archive.org/web/20190207151651/http://www.eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx).
    fn validate_red_black<K: Ord + Clone + Default, V: Clone + Default>(
        memtable: &MemTable<K, V>,
        root: usize,
    ) -> Result<usize, ()> {
        if root == NULL {
            Ok(1)
        } else {
            let root = memtable.node(root);
            let left = root.link[LEFT];
            let right = root.link[RIGHT];

            if root.red && (memtable.is_red(left) || memtable.is_red(right)) {
                // Red violation
                return Err(());
            }

            let left_bh = validate_red_black(memtable, left)?;
            let right_bh = validate_red_black(memtable, right)?;

            if (left != NULL && memtable.node(left).key >= root.key)
                || (right != NULL && memtable.node(right).key <= root.key)
            {
                // Binary tree violation
                return Err(());
            }

            if left_bh != right_bh {
                // Black violation
                return Err(());
            }

            if root.red {
                Ok(left_bh)
            } else {
                Ok(left_bh + 1)
            }
        }
    }
}
