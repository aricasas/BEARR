use std::path::PathBuf;

use crate::DbError;

#[derive(Debug, Clone, Copy)]
pub enum EvictionId {
    // we don't need any id/pointer in FIFO
    MockId(usize),
}

#[derive(Debug)]
pub struct Eviction {
    mock_queue: Vec<(PathBuf, usize)>,
}

impl Eviction {
    pub fn new(capacity: usize) -> Result<Self, DbError> {
        let _ = capacity;

        Ok(Self {
            mock_queue: Vec::new(),
        })
    }

    pub fn choose_victim(&self) -> VictimChooser<'_> {
        VictimChooser::new(self)
    }

    pub fn evict(&mut self, victim: EvictionId) {
        let EvictionId::MockId(idx) = victim;
        if idx < self.mock_queue.len() {
            self.mock_queue.remove(idx);
        }
    }

    pub fn insert_new(&mut self, path: PathBuf, page_number: usize) -> EvictionId {
        let idx = self.mock_queue.len();
        self.mock_queue.push((path, page_number));
        EvictionId::MockId(idx)
    }

    pub fn touch(&mut self, id: EvictionId) {
        let _ = id;
    }
}

pub struct VictimChooser<'a> {
    eviction: &'a Eviction,
    chosen_idx: Option<usize>,
}

impl<'a> VictimChooser<'a> {
    fn new(eviction: &'a Eviction) -> Self {
        Self {
            eviction,
            chosen_idx: None,
        }
    }
}

impl<'a> Iterator for VictimChooser<'a> {
    type Item = (EvictionId, &'a (PathBuf, usize));

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.chosen_idx.map(|idx| idx + 1).unwrap_or(0);
        self.chosen_idx = Some(next);

        self.eviction
            .mock_queue
            .get(next)
            .map(|e| (EvictionId::MockId(next), e))
    }
}
