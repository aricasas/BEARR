use std::{array, marker::PhantomData, num::Wrapping as W};

/// A hash function with a specific seed to influence the output.
#[repr(transparent)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
pub struct HashFunction<H = MurmurHash> {
    seed: u32,
    _hash: PhantomData<H>,
}

/// An algorithm for hashing bytes.
pub trait HashAlgorithm: bytemuck::Pod {
    /// Hashes the given key to a `u32` with the given `u32` seed.
    fn hash(key: &[u8], seed: u32) -> u32;
}

impl<H: HashAlgorithm> HashFunction<H> {
    /// Returns a new hash function with a random seed.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            seed: fastrand::u32(..),
            _hash: PhantomData,
        }
    }

    /// Hashes the given key to an index into a container with the given length.
    pub fn hash_to_index(&self, key: impl bytemuck::Pod, length: usize) -> usize {
        H::hash(bytemuck::bytes_of(&key), self.seed) as usize % length
    }
}

/// Implementation of the 32-bit MurmurHash3 hash function.
#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug, PartialEq, Eq)]
pub struct MurmurHash;

impl HashAlgorithm for MurmurHash {
    /// https://en.wikipedia.org/wiki/MurmurHash
    fn hash(key: &[u8], seed: u32) -> u32 {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur() {
        // Also taken from https://en.wikipedia.org/wiki/MurmurHash
        let cases = [
            (0x00000000, 0x00000000, ""),
            (0x00000001, 0x514e28b7, ""),
            (0xffffffff, 0x81f16f39, ""),
            (0x00000000, 0xba6bd213, "test"),
            (0x9747b28c, 0x704b81dc, "test"),
            (0x00000000, 0xc0363e43, "Hello, world!"),
            (0x9747b28c, 0x24884cba, "Hello, world!"),
            (
                0x00000000,
                0x2e4ff723,
                "The quick brown fox jumps over the lazy dog",
            ),
            (
                0x9747b28c,
                0x2fa826cd,
                "The quick brown fox jumps over the lazy dog",
            ),
        ];
        for (seed, expected, key) in cases {
            assert_eq!(MurmurHash::hash(key.as_bytes(), seed), expected);
        }
    }

    fn murmur_hash_to_index(key: &str, length: usize, seed: u32) -> usize {
        MurmurHash::hash(key.as_bytes(), seed) as usize % length
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
