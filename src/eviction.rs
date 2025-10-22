use crate::DbError;

#[derive(Debug, Clone, Copy)]
pub enum EvictionId {
    // we don't need any id/pointer in FIFO
    MockFifo,
}

#[derive(Debug)]
pub struct Eviction<T: Clone> {
    mock_queue: Vec<T>,
}

impl<T: Clone> Eviction<T> {
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        let _ = capacity;

        Ok(Self {
            mock_queue: Vec::new(),
        })
    }

    pub fn choose_victim(self) -> VictimChooser<T> {
        VictimChooser::new(self)
    }

    pub fn insert_new(&mut self, entry: T) -> EvictionId {
        self.mock_queue.push(entry);
        EvictionId::MockFifo
    }

    pub fn touch(&mut self, id: EvictionId) {
        let _ = id;
    }
}

pub struct VictimChooser<T: Clone> {
    eviction: Eviction<T>,
    chosen_idx: Option<usize>,
}

impl<T: Clone> VictimChooser<T> {
    /// Confirm choice of victim to evict. The last victim returned when calling .next() is evicted.
    ///
    /// If .next() was never called, or if .next() was called until it returns None, no victim is evicted.
    pub fn confirm(mut self) -> Eviction<T> {
        if let Some(idx) = self.chosen_idx
            && idx < self.eviction.mock_queue.len()
        {
            self.eviction.mock_queue.remove(idx);
        }

        self.eviction
    }
    fn new(eviction: Eviction<T>) -> Self {
        Self {
            eviction,
            chosen_idx: None,
        }
    }
}
impl<T: Clone> Iterator for VictimChooser<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.chosen_idx.map(|idx| idx + 1).unwrap_or(0);
        self.chosen_idx = Some(next);

        self.eviction.mock_queue.get(next).cloned()
    }
}
