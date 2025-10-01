use std::{cmp::Ordering, ops::RangeInclusive};

use crate::DBError;

/// An in-memory MemTable.
///
/// Stores key-value pairs in a Red-Black tree.
///
/// The Red-Black tree design and implementation is inspired by this [reference](https://web.archive.org/web/20190207151651/http://www.eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx).
/// The algorithm for scanning the tree is inspired by this [reference](https://en.wikipedia.org/wiki/Tree_traversal#Advancing_to_the_next_or_previous_node).
#[derive(Debug)]
pub struct MemTable<K: Ord + Clone + Default, V: Clone + Default> {
    root: usize,
    nodes: Vec<Node<K, V>>,
}

/// The nodes in our Red-Black tree.
#[derive(Debug)]
struct Node<K, V> {
    key: K,
    value: V,
    link: [usize; 2],
    red: bool,
}

/// Constant representing an impossible index into the `nodes` Vec in a MemTable
const NULL: usize = usize::MAX;
/// Constant used to access the left child of a node
const LEFT: usize = 0;
/// Constant used to access the right child of a node
const RIGHT: usize = 1;

impl<K: Ord + Clone + Default, V: Clone + Default> MemTable<K, V> {
    /// Creates a new empty MemTable.
    ///
    /// Allocates enough space to hold `memtable_size` key-value pairs.
    /// If allocation fails, returns `DBError::OOM`.
    pub fn new(memtable_size: usize) -> Result<Self, DBError> {
        let mut nodes = Vec::new();
        if nodes.try_reserve_exact(memtable_size).is_err() {
            Err(DBError::OOM)
        } else {
            Ok(Self { root: NULL, nodes })
        }
    }

    /// Searches MemTable for node with given key and returns the value associated if it exists.
    pub fn get(&self, key: K) -> Option<V> {
        let mut curr = self.root;

        while curr != NULL {
            let node = self.node(curr);
            match key.cmp(&node.key) {
                Ordering::Less => curr = node.link[LEFT],
                Ordering::Equal => return Some(node.value.clone()),
                Ordering::Greater => curr = node.link[RIGHT],
            }
        }

        None
    }

    /// Updates or inserts a key-value pair into the MemTable.
    ///
    /// If there is a node with matching key already, it changes the value in-place.
    /// Otherwise, it inserts a new node.
    ///
    /// Based on the Top-Down `jsw_insert` implementation from [here](https://web.archive.org/web/20190207151651/http://www.eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx).
    pub fn put(&mut self, key: K, value: V) -> Result<(), DBError> {
        if self.root == NULL {
            self.root = self.make_node(key, value)?;
        } else {
            // Dummy root
            let mut head = Node {
                key: K::default(),
                value: V::default(),
                link: [NULL, self.root],
                red: false,
            };

            let mut g = NULL;
            let mut t = NULL;
            let mut p = NULL;
            let mut q = self.root;

            let mut dir = 0;
            let mut last = 0;

            loop {
                if q == NULL {
                    // Insert new node as leaf
                    q = self.make_node(key.clone(), value.clone())?;
                    self.node_mut(p).link[dir] = q;
                } else if self.is_red(self.node(q).link[LEFT])
                    && self.is_red(self.node(q).link[RIGHT])
                {
                    // Color flip
                    self.node_mut(q).red = true;
                    self.node_mut(self.node(q).link[LEFT]).red = false;
                    self.node_mut(self.node(q).link[RIGHT]).red = false;
                }

                if self.is_red(q) && self.is_red(p) {
                    // Red violation
                    let dir2 = (self.node_or(t, &head).link[RIGHT] == g) as usize;

                    if q == self.node(p).link[last] {
                        self.node_mut_or(t, &mut head).link[dir2] =
                            self.single_rotation(g, 1 - last);
                    } else {
                        self.node_mut_or(t, &mut head).link[dir2] =
                            self.double_rotation(g, 1 - last);
                    }
                }

                if self.node(q).key == key {
                    // Found key
                    self.node_mut(q).value = value;
                    break;
                }

                last = dir;
                dir = (self.node(q).key < key) as usize;

                if g != NULL {
                    t = g;
                }

                g = p;
                p = q;
                q = self.node(q).link[dir];
            }

            self.root = head.link[1];
        }

        self.node_mut(self.root).red = false;

        Ok(())
    }

    /// Returns the number of key-value pairs currently stored in the MemTable.
    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    /// Returns an iterator of key-value pairs over the given range of keys.
    pub fn scan(&self, range: RangeInclusive<K>) -> MemTableIter<'_, K, V> {
        todo!()
    }

    /// Creates a new node in the memtable.
    ///
    /// Returns an error if this would make the memtable exceed capacity.
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
    /// Panics if `node` doesn't point to a valid node in the MemTable.
    fn node(&self, node: usize) -> &Node<K, V> {
        &self.nodes[node]
    }

    /// Access a given node mutably.
    ///
    /// Panics if `node` doesn't point to a valid node in the MemTable.
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

    /// Returns true iff `node` points to a valid red node in the MemTable.
    fn is_red(&self, node: usize) -> bool {
        node != NULL && self.node(node).red
    }

    /// Performs a single rotation in the given direction to the tree rooted at `node`.
    ///
    /// Panics if `node` doesn't point to a valid node in the MemTable
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
    /// Panics if `node` doesn't point to a valid node in the MemTable.
    fn double_rotation(&mut self, root: usize, dir: usize) -> usize {
        self.node_mut(root).link[1 - dir] =
            self.single_rotation(self.node(root).link[1 - dir], 1 - dir);

        self.single_rotation(root, dir)
    }
}

pub struct MemTableIter<'a, K: Ord + Clone + Default, V: Clone + Default> {
    memtable: &'a MemTable<K, V>,
    stack: Vec<usize>,
}
impl<'a, K: Ord + Clone + Default, V: Clone + Default> Iterator for MemTableIter<'a, K, V> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small() -> Result<(), DBError> {
        let mut memtable = MemTable::new(5)?;

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
    fn test_insert_in_order() -> Result<(), DBError> {
        let mut memtable = MemTable::new(100)?;

        // Insert 100 nodes
        for i in 0..100 {
            assert_eq!(memtable.size(), i);
            memtable.put(i, i * 10)?;
            assert_eq!(memtable.size(), i + 1);
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
        let mut memtable = MemTable::new(100)?;

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
        let mut memtable = MemTable::new(100)?;

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
    fn test_max_size() -> Result<(), DBError> {
        let mut memtable = MemTable::new(100)?;

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

    /// Checks that the tree rooted at `root` in the MemTable is a valid binary tree and satisfies the Red-Black conditions.
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

            let left_bh = validate_red_black(&memtable, left)?;
            let right_bh = validate_red_black(&memtable, right)?;

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
