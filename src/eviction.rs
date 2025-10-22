use std::collections::VecDeque;

use crate::DbError;

const NULL: usize = usize::MAX;

#[derive(Debug, Clone, Copy)]
pub enum EvictionId {
    Mock,
    In(usize),
    Out(usize),
    M(usize),
}

#[derive(Debug)]
pub struct Eviction<T> {
    mock_queue: VecDeque<T>,
    a1_in: Queue<T>,
    a1_out: Queue<T>,
    a_m: Queue<T>,
    k_in: usize,
    k_out: usize,
}

impl<T: Clone> Eviction<T> {
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        let k_in = capacity / 4; // ~25%
        let k_out = capacity / 2; // ~50%

        Ok(Self {
            mock_queue: VecDeque::new(),
            a1_in: Queue::new(k_in)?,
            a1_out: Queue::new(k_out)?,
            a_m: Queue::new(capacity)?,
            k_in,
            k_out,
        })
    }

    pub fn evict(&mut self) -> T {
        self.mock_queue.pop_front().unwrap()
    }

    pub fn insert_new(&mut self, entry: T) -> EvictionId {
        self.mock_queue.push_back(entry);
        EvictionId::Mock
    }

    pub fn touch(&mut self, id: EvictionId) {}
}

#[derive(Debug)]
struct Queue<T> {
    inner: Vec<T>,
    front: usize,
    back: usize,
}

impl<T> Queue<T> {
    fn new(capacity: usize) -> Result<Self, DbError> {
        let mut inner = Vec::new();
        inner.try_reserve_exact(capacity)?;

        Ok(Self {
            inner,
            front: NULL,
            back: NULL,
        })
    }
}
