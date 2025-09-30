use std::{cmp::Ordering, mem, ops::RangeInclusive};

/// Shorthand type for a possibly null pointer to a node on the heap
type Link<K, V> = Option<Box<Node<K, V>>>;
/// Constant used to access the left child of a node
const LEFT: usize = 0;
/// Constant used to access the right child of a node
const RIGHT: usize = 1;

/// An in-memory MemTable.
///
/// Stores key value pairs in a Red-Black tree.
///
/// The Red-Black tree design and implementation is inspired by this [reference](https://web.archive.org/web/20190207151651/http://www.eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx).
/// The algorithm for scanning the tree is inspired by this [reference](https://en.wikipedia.org/wiki/Tree_traversal#Advancing_to_the_next_or_previous_node).
#[derive(Debug)]
pub struct MemTable<K: Ord + Clone, V: Clone> {
    size: usize,
    root: Link<K, V>,
}

/// The nodes in our Red-Black tree.
#[derive(Debug)]
struct Node<K: Ord, V> {
    key: K,
    value: V,
    children: [Link<K, V>; 2],
    red: bool,
}

impl<K: Ord + Clone, V: Clone> MemTable<K, V> {
    /// Creates a new empty MemTable.
    pub fn new() -> Self {
        Self {
            size: 0,
            root: None,
        }
    }

    /// Searches MemTable for node with given key and returns the value associated if it exists.
    pub fn get(&self, key: K) -> Option<V> {
        let mut curr = &self.root;

        while let Some(node) = curr {
            match key.cmp(&node.key) {
                Ordering::Less => curr = &node.children[LEFT],
                Ordering::Equal => return Some(node.value.clone()),
                Ordering::Greater => curr = &node.children[RIGHT],
            }
        }

        None
    }

    /// Puts a new key value pair into the MemTable.
    ///
    /// If there is a node with matching key already, it changes the value in-place.
    /// Otherwise, it inserts a new node.
    ///
    /// Based on the Top-Down implementation from [here](https://web.archive.org/web/20190207151651/http://www.eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx).
    pub fn put(&mut self, key: K, value: V) {
        if self.root.is_some() {
            let mut g: Link<K, V> = None;
            let mut p: Link<K, V> = None;
            let mut q: Link<K, V> = None;

            let mut g_dir = 0;
            let mut p_dir = 1000;
            let mut q_dir = 100;

            let mut last = 10;
            let mut dir = 0;

            q = self.root.take();

            let mut restore = &mut self.root;

            loop {
                if let Some(q) = &mut q
                    && Node::is_red(&q.children[LEFT])
                    && Node::is_red(&q.children[RIGHT])
                {
                    q.red = true;
                    q.children[LEFT].as_mut().unwrap().red = false;
                    q.children[RIGHT].as_mut().unwrap().red = false;
                } else {
                    q = Some(Node::boxed(key.clone(), value.clone()));
                    self.size += 1;
                    q_dir = dir;
                }

                if Node::is_red(&q) && Node::is_red(&p) {
                    p.as_mut().unwrap().children[q_dir] = q;
                    g.as_mut().unwrap().children[p_dir] = p;

                    if q_dir == last {
                        g = Node::single_rotation(g, 1 - last);
                    } else {
                        g = Node::double_rotation(g, 1 - last);
                    }
                    p = None;
                    q = None;
                }

                {
                    let q = q.as_mut().unwrap();

                    if q.key == key {
                        q.value = value;
                        break;
                    }

                    last = dir;
                    dir = if q.key < key { RIGHT } else { LEFT };
                }

                *restore = g;
                g = p;
                p = q;
                q = p.as_mut().unwrap().children[dir].take();

                g_dir = p_dir;
                p_dir = q_dir;
                q_dir = dir;

                if restore.is_some() {
                    restore = &mut (*restore).as_mut().unwrap().children[g_dir];
                }
            }

            if g.is_some() {
                *restore = g;
                restore = &mut (*restore).as_mut().unwrap().children[g_dir];
            }
            if p.is_some() {
                *restore = p;
                restore = &mut (*restore).as_mut().unwrap().children[p_dir];
            }
            if q.is_some() {
                *restore = q;
                restore = &mut (*restore).as_mut().unwrap().children[q_dir];
            }
        } else {
            // Empty tree
            self.root = Some(Node::boxed(key, value));
        }
    }

    /// # This is a temporary method
    /// Puts a new key value pair into the MemTable without respecting Red-Black tree constraints.
    pub fn put_unbalanced(&mut self, key: K, value: V) {
        if Node::put_unbalanced(&mut self.root, key, value) {
            self.size += 1;
        }
    }

    pub fn llrb_put(&mut self, key: K, value: V) {
        let mut new_insertion = false;
        (self.root, new_insertion) = Node::llrb_put(self.root.take(), key, value);
        self.root.as_mut().unwrap().red = false;
        if new_insertion {
            self.size += 1;
        }
    }

    /// Returns the number of nodes currently in the MemTable
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns an iterator
    pub fn scan(&self, range: RangeInclusive<K>) -> MemTableIter<'_, K, V> {
        todo!()
    }
}

impl<K: Ord, V> Node<K, V> {
    /// Creates a new red Node with the given key and value and no children.
    fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            children: [None, None],
            red: true,
        }
    }

    /// Creates a new red Node with the given key and value and no children,
    /// and stores in in the heap.
    fn boxed(key: K, value: V) -> Box<Self> {
        Box::new(Self::new(key, value))
    }

    /// Returns true iff node exists and is colored red.
    fn is_red(link: &Link<K, V>) -> bool {
        link.as_ref().is_some_and(|node| node.red)
    }

    fn rot_right(mut self: Box<Self>) -> Box<Self> {
        let mut child = self.children[LEFT].take().unwrap();
        self.children[LEFT] = child.children[RIGHT].take();
        child.children[RIGHT] = Some(self);

        child
    }
    fn rot_left(mut self: Box<Self>) -> Box<Self> {
        let mut child = self.children[RIGHT].take().unwrap();
        self.children[RIGHT] = child.children[LEFT].take();
        child.children[LEFT] = Some(self);

        child
    }
    fn swap_colors(node1: &mut Box<Self>, node2: &mut Box<Self>) {
        mem::swap(&mut node1.red, &mut node2.red);
    }

    fn llrb_put(root: Link<K, V>, key: K, value: V) -> (Link<K, V>, bool) {
        if root.is_none() {
            return (Some(Self::boxed(key, value)), true);
        }
        let mut root = root.unwrap();
        let mut inserted_new = false;

        if key < root.key {
            (root.children[LEFT], inserted_new) =
                Self::llrb_put(root.children[LEFT].take(), key, value);
        } else if key > root.key {
            (root.children[RIGHT], inserted_new) =
                Self::llrb_put(root.children[RIGHT].take(), key, value);
        } else {
            root.value = value;
            return (Some(root), false);
        }

        if Node::is_red(&root.children[RIGHT]) && !Node::is_red(&root.children[LEFT]) {
            root = Self::rot_left(root);
            let mut left = root.children[LEFT].take().unwrap();
            Self::swap_colors(&mut root, &mut left);
            root.children[LEFT] = Some(left);
        }

        if Node::is_red(&root.children[LEFT])
            && Node::is_red(&root.children[LEFT].as_ref().unwrap().children[LEFT])
        {
            root = Self::rot_right(root);

            let mut right = root.children[RIGHT].take().unwrap();
            Self::swap_colors(&mut root, &mut right);
            root.children[RIGHT] = Some(right);
        }

        if Node::is_red(&root.children[RIGHT]) && Node::is_red(&root.children[LEFT]) {
            root.red = !root.red;

            root.children[LEFT].as_mut().unwrap().red = false;
            root.children[RIGHT].as_mut().unwrap().red = false;
        }

        (Some(root), inserted_new)
    }

    /// Performs a single tree rotation in the given direction
    fn single_rotation(mut root: Link<K, V>, dir: usize) -> Link<K, V> {
        let mut save = root.as_mut().unwrap().children[1 - dir].take();

        root.as_mut().unwrap().children[1 - dir] = save.as_mut().unwrap().children[dir].take();
        save.as_mut().unwrap().children[dir] = root;

        save
    }

    /// Performs a double tree rotation in the given direction
    fn double_rotation(mut root: Link<K, V>, dir: usize) -> Link<K, V> {
        root.as_mut().unwrap().children[1 - dir] =
            Node::single_rotation(root.as_mut().unwrap().children[1 - dir].take(), 1 - dir);

        Node::single_rotation(root, dir)
    }

    /// # Warning! This function is for testing only.
    ///
    /// Checks that the tree rooted at this node is a valid binary tree and satisfies the Red-Black conditions.
    ///
    /// If the tree is valid, returns the black height of the tree.
    fn validate_red_black(root: &Link<K, V>) -> Result<usize, &'static str> {
        if let Some(root) = root {
            let left = &root.children[LEFT];
            let right = &root.children[RIGHT];

            if root.red && (Node::is_red(left) || Node::is_red(right)) {
                return Err("Red violation");
            }

            let left_height = Node::validate_red_black(left)?;
            let right_height = Node::validate_red_black(right)?;

            if left.as_ref().is_some_and(|l| l.key >= root.key)
                || right.as_ref().is_some_and(|r| r.key <= root.key)
            {
                return Err("Binary tree violation");
            }

            if left_height != right_height {
                return Err("Black violation");
            }

            if root.red {
                Ok(left_height)
            } else {
                Ok(left_height + 1)
            }
        } else {
            Ok(1)
        }
    }

    /// Inserts or updates a key value pair.
    ///
    /// Returns true iff a new node was created.
    fn put_unbalanced(root: &mut Link<K, V>, key: K, value: V) -> bool {
        if let Some(root) = root {
            match key.cmp(&root.key) {
                Ordering::Less => Node::put_unbalanced(&mut root.children[LEFT], key, value),
                Ordering::Equal => {
                    root.value = value;
                    false
                }
                Ordering::Greater => Node::put_unbalanced(&mut root.children[RIGHT], key, value),
            }
        } else {
            *root = Some(Self::boxed(key, value));
            true
        }
    }
}

pub struct MemTableIter<'a, K: Ord, V> {
    stack: Vec<&'a Node<K, V>>,
}
impl<'a, K: Ord, V> Iterator for MemTableIter<'a, K, V> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_in_order() {
        let mut memtable = MemTable::new();
        for i in 0..100 {
            assert_eq!(memtable.size(), i);
            memtable.llrb_put(i, i * 10);
            Node::validate_red_black(&memtable.root).unwrap();
            assert_eq!(memtable.size(), i + 1);
        }
        for i in 0..100 {
            assert_eq!(memtable.get(i), Some(i * 10));
        }
        for i in 200..300 {
            assert_eq!(memtable.get(i), None);
        }
    }

    #[test]
    fn test_insert_in_reverse() {
        let mut memtable = MemTable::new();
        for i in (0..100).rev() {
            memtable.llrb_put(i, i * 10);
            Node::validate_red_black(&memtable.root).unwrap();
        }
        for i in 0..100 {
            assert_eq!(memtable.get(i), Some(i * 10));
        }
        for i in 200..300 {
            assert_eq!(memtable.get(i), None);
        }
    }

    #[test]
    fn test_update() {
        let mut memtable = MemTable::new();
        for i in 0..100 {
            memtable.llrb_put(i, i * 10);
        }
        assert_eq!(memtable.size(), 100);

        for i in 0..100 {
            if i % 2 == 0 {
                memtable.llrb_put(i, i * 20);
            }
        }
        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(memtable.get(i), Some(i * 20));
            } else {
                assert_eq!(memtable.get(i), Some(i * 10));
            }
        }
    }
}
