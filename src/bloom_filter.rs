use bit_vec::BitVec;

use crate::hash::HashFunction;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    hash_functions: Vec<HashFunction>,
    bits: BitVec, // this is from a crate, or implement your own if you want
}

impl BloomFilter {
    pub fn empty(n_entries: usize, bits_per_entry: usize) -> Self {
        // Calculate how many hash functions it needs to be optimal: bits_per_entry * ln(2)
        // Careful because slides have error https://piazza.com/class/mf0gdjv1iov3n/post/77

        // TODO:Might want to round capacity up to multiple of PAGE_SIZE

        let bits = BitVec::from_elem(n_entries * bits_per_entry, false);
        let num_hashes = (bits_per_entry as f32 * f32::ln(2.0)).ceil() as usize;

        let hash_functions = (0..num_hashes).map(|_| HashFunction::new()).collect();

        Self {
            hash_functions,
            bits,
        }
    }

    pub fn from_hashes_and_bits(hash_functions: Vec<HashFunction>, bits: Vec<u8>) -> Self {
        let bits = BitVec::from_bytes(&bits);

        Self {
            hash_functions,
            bits,
        }
    }

    // TODO change this interface to return whatever you need to store in the file
    // This was just a guess to how it could work
    pub fn into_hashes_and_bits(self) -> (Vec<HashFunction>, Vec<u8>) {
        (self.hash_functions, self.bits.to_bytes())
    }

    pub fn insert(&mut self, key: u64) {
        let bitmap_len = self.bits.len();
        let hash_functions = &self.hash_functions;
        for hashed_index in hash_functions
            .iter()
            .map(|func| func.hash_to_index(key, bitmap_len))
        {
            self.bits.set(hashed_index, true);
        }
    }

    pub fn query(&self, key: u64) -> bool {
        let bitmap_len = self.bits.len();
        for hashed_index in self
            .hash_functions
            .iter()
            .map(|func| func.hash_to_index(key, bitmap_len))
        {
            if !self.bits.get(hashed_index).unwrap() {
                return false;
            }
        }
        true
    }
}
