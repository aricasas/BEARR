use std::{array, num::Wrapping as W};

/// A hash function with a specific seed to influence the output.
#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
pub struct HashFunction {
    seed: u32,
}

impl HashFunction {
    /// Returns a new hash function with a random seed.
    pub fn new() -> Self {
        Self {
            seed: fastrand::u32(..),
        }
    }

    /// Hashes the given key to an index into a container with the given length.
    pub fn hash_to_index(&self, key: impl bytemuck::Pod, length: usize) -> usize {
        hash(bytemuck::bytes_of(&key), self.seed) as usize % length
    }
}

/// Implementation of the 32-bit MurmurHash3 hash function.
/// https://en.wikipedia.org/wiki/MurmurHash
fn murmur3_32(key: &[u8], seed: u32) -> u32 {
    let len = key.len();

    let c1 = W(0xcc9e2d51);
    let c2 = W(0x1b873593);
    let r1 = 15;
    let r2 = 13;
    let m = W(5);
    let n = W(0xe6546b64);

    let mut hash = W(seed);

    let (chunks, remainder) = key.as_chunks::<4>();
    let remainder: [u8; 4] = array::from_fn(|i| remainder.get(i).copied().unwrap_or(0));

    for &chunk in chunks {
        let mut k = W(u32::from_le_bytes(chunk));

        k *= c1;
        k = W(k.0.rotate_left(r1));
        k *= c2;

        hash ^= k;
        hash = W(hash.0.rotate_left(r2));
        hash = (hash * m) + n;
    }

    {
        let mut remainder = W(u32::from_le_bytes(remainder));

        remainder *= c1;
        remainder = W(remainder.0.rotate_left(r1));
        remainder *= c2;

        hash ^= remainder;
    }

    hash ^= len as u32;

    hash ^= hash >> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >> 13;
    hash *= 0xc2b2ae35;
    hash ^= hash >> 16;

    hash.0
}

#[cfg(not(feature = "mock_hash"))]
use murmur3_32 as hash;

#[cfg(feature = "mock_hash")]
fn hash(key: &[u8], _seed: u32) -> u32 {
    usize::from_ne_bytes(key[8..16].try_into().unwrap()) as u32
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(0x00000000, 0x00000000, "")]
    #[case(0x00000001, 0x514e28b7, "")]
    #[case(0xffffffff, 0x81f16f39, "")]
    #[case(0x00000000, 0xba6bd213, "test")]
    #[case(0x9747b28c, 0x704b81dc, "test")]
    #[case(0x00000000, 0xc0363e43, "Hello, world!")]
    #[case(0x9747b28c, 0x24884cba, "Hello, world!")]
    #[case(0x00000000, 0x2e4ff723, "The quick brown fox jumps over the lazy dog")]
    #[case(0x9747b28c, 0x2fa826cd, "The quick brown fox jumps over the lazy dog")]
    fn test_murmur(#[case] seed: u32, #[case] expected: u32, #[case] key: &str) {
        assert_eq!(murmur3_32(key.as_bytes(), seed), expected);
    }

    fn murmur_hash_to_index(key: &str, length: usize, seed: u32) -> usize {
        murmur3_32(key.as_bytes(), seed) as usize % length
    }

    #[test]
    fn test_hash_function() {
        assert!((0..16).contains(&murmur_hash_to_index("monad", 16, 314)));

        assert_ne!(
            murmur_hash_to_index("monad", 32, 159),
            murmur_hash_to_index("monoid", 32, 159),
        );

        assert_ne!(
            murmur_hash_to_index("monad", 64, 265),
            murmur_hash_to_index("monad", 64, 358),
        );
    }
}
