use bit_vec::BitVec;

use crate::hash::HashFunction;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    pub hash_functions: Vec<HashFunction>, // Bloom filter's hash functions
    bits: BitVec,                          // Bloom filter's bitmap
}

impl BloomFilter {
    /// Create an empty bloom filter having the number of entries and bits per each entry
    pub fn empty(n_entries: usize, bits_per_entry: usize) -> Self {
        // Calculate how many hash functions it needs to be optimal: bits_per_entry * ln(2)
        let bits = BitVec::from_elem((n_entries * bits_per_entry).next_multiple_of(8), false);
        let num_hashes = (bits_per_entry as f32 * f32::ln(2.0)).ceil() as usize;

        let hash_functions = (0..num_hashes).map(|_| HashFunction::new()).collect();

        Self {
            hash_functions,
            bits,
        }
    }

    /// Turn a vector of bits into bloom filter by having the number of hashes
    pub fn from_bytes(filter_bytes: &[u8], num_hashes: usize) -> Self {
        let bits_offset = num_hashes * size_of::<HashFunction>();
        let hash_functions: &[HashFunction] = bytemuck::cast_slice(&filter_bytes[0..bits_offset]);
        let hash_functions = hash_functions.to_vec();
        let bits = BitVec::from_bytes(&filter_bytes[bits_offset..]);
        Self {
            hash_functions,
            bits,
        }
    }

    /// Turn filter into a vector of bytes
    pub fn turn_to_bytes(&self) -> Vec<u8> {
        let hash_bytes = bytemuck::cast_slice(&self.hash_functions);
        let mut hash_bytes = hash_bytes.to_vec();
        hash_bytes.append(&mut self.bits.to_bytes());
        hash_bytes
    }

    /// Insert a key into the bloom filter
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

    /// Search for a key inside the bloom filter
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

/* Tests for SSTs */
#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::repeat_with;

    #[test]
    /// Test to see if bloom filter works correctlly
    /// need to make sure it doesn't return any false negatives
    pub fn bloom_filter_test() {
        let entries_num = 10;
        let bits_per_entry = 20;
        let num_elements = 100;
        let entries: Vec<u64> = repeat_with(|| fastrand::u64(..))
            .take(num_elements)
            .collect();
        let mut filter = BloomFilter::empty(entries_num, bits_per_entry);
        for entry in &entries {
            filter.insert(*entry);
        }

        for entry in &entries {
            assert!(filter.query(*entry));
        }
    }
}
