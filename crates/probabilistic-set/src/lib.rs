use rand::Rng;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Serialize, Debug, Deserialize, Clone, Readable, Writable)]
pub struct ProbabilisticSet {
    // todo: consider making bits and array?
    bits: Vec<u8>,
    size_bits: usize,
    seed: u64, // Store the random seed
}

impl ProbabilisticSet {
    pub fn new(expected_items: usize, bits_per_item: usize) -> Self {
        let size_bits = expected_items * bits_per_item;
        let seed = rand::thread_rng().r#gen(); // Generate random seed

        ProbabilisticSet {
            bits: vec![0; (size_bits + 7) / 8],
            size_bits,
            seed,
        }
    }

    // Create with a specific seed (useful for testing or controlled randomness)
    pub fn with_seed(expected_items: usize, bits_per_item: usize, seed: u64) -> Self {
        let size_bits: usize = expected_items * bits_per_item;
        ProbabilisticSet {
            bits: vec![0; (size_bits + 7) / 8],
            size_bits,
            seed,
        }
    }

    pub fn insert(&mut self, item: u128) {
        let idx = self.hash(item) % self.size_bits;
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        self.bits[byte_idx] |= 1 << bit_idx;
    }

    pub fn contains(&self, item: u128) -> bool {
        let idx = self.hash(item) % self.size_bits;
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        self.bits[byte_idx] & (1 << bit_idx) != 0
    }

    fn hash(&self, item: u128) -> usize {
        let mut hasher = DefaultHasher::new();
        // Hash both the seed and the item
        self.seed.hash(&mut hasher);
        item.hash(&mut hasher);
        hasher.finish() as usize
    }

    // Get current seed (useful for debugging)
    pub fn seed(&self) -> u64 {
        self.seed
    }

    // Change the seed (and clear the bits since the hash function effectively changes)
    pub fn reseed(&mut self) {
        self.seed = rand::thread_rng().r#gen();
        self.bits.fill(0);
    }

    pub fn size_bytes(&self) -> usize {
        // Include size of seed in total
        self.bits.len() + std::mem::size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {}
