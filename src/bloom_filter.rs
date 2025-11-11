use bit_vec::BitVec;

use crate::hash::HashFunction;

#[derive(Debug)]
pub struct BloomFilter {
    hash_functions: Vec<HashFunction>,
    bits: BitVec,
}

impl BloomFilter {
    pub fn empty(n_entries: usize, bits_per_entry: usize) -> Self {
        // Calculate how many hash functions it needs to be optimal: bits_per_entry * ln(2)
        // Careful because slides have error https://piazza.com/class/mf0gdjv1iov3n/post/77

        let bits = BitVec::with_capacity(n_entries * bits_per_entry);
        let num_hashes = (bits_per_entry as f32 * f32::ln(2.0)) as usize;

        let hash_functions = (0..num_hashes).map(|_| HashFunction::new()).collect();

        Self {
            hash_functions,
            bits,
        }
    }

    pub fn from_hashes_and_bits(hash_functions: Vec<HashFunction>, bits: &[u8]) -> Self {
        let bits = BitVec::from_bytes(bits);

        Self {
            hash_functions,
            bits,
        }
    }

    pub fn hashes(&self) -> &[HashFunction] {
        &self.hash_functions
    }

    pub fn bits(self) -> Vec<u8> {
        self.bits.to_bytes()
    }

    pub fn insert(&mut self, key: u64) {
        todo!()
    }

    pub fn query(&self, key: u64) -> bool {
        todo!()
    }
}
