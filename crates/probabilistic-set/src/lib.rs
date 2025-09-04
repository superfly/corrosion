use rand::Rng;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Serialize, Debug, Deserialize, Clone, Readable, Writable)]
pub struct ProbSet {
    // todo: consider making bits and array?
    bits: Vec<u8>,
    size_bits: usize,
    seed: u64, // Store the random seed
}

impl ProbSet {
    pub fn new(expected_items: usize, bits_per_item: usize) -> Self {
        if expected_items == 0 || bits_per_item == 0 {
            panic!("expected_items and bits_per_item must be greater than 0");
        }

        let size_bits = expected_items * bits_per_item;
        let seed = rand::thread_rng().r#gen(); // Generate random seed

        ProbSet {
            bits: vec![0; size_bits.div_ceil(8)],
            size_bits,
            seed,
        }
    }

    // Create with a specific seed (useful for testing or controlled randomness)
    pub fn with_seed(expected_items: usize, bits_per_item: usize, seed: u64) -> Self {
        let size_bits: usize = expected_items * bits_per_item;
        ProbSet {
            bits: vec![0; size_bits.div_ceil(8)],
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
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_uuid_collision_rates() {
        // Test different configurations with UUIDs
        let configs = vec![(1000, 2), (1000, 4)];

        for (expected_items, bits_per_item) in configs {
            let collision_rate = measure_uuid_collision_rate(expected_items, bits_per_item, 1000);
            println!(
                "Config: {} items, {} bits/item -> Collision rate: {:.2}%",
                expected_items,
                bits_per_item,
                collision_rate * 100.0
            );
        }
    }

    #[test]
    fn test_uuid_false_positive_rate() {
        let mut set = ProbSet::new(1000, 4);
        let mut inserted_uuids = Vec::new();

        for _ in 0..500 {
            let uuid = Uuid::new_v4();
            let uuid_u128 = uuid.as_u128();
            set.insert(uuid_u128);
            inserted_uuids.push(uuid_u128);
        }

        for &uuid in &inserted_uuids {
            assert!(set.contains(uuid), "False negative detected!");
        }

        // Test false positive rate with 10,000 random UUIDs
        let test_size = 10000;
        let mut false_positives = 0;

        for _ in 0..test_size {
            let test_uuid = Uuid::new_v4().as_u128();

            if inserted_uuids.contains(&test_uuid) {
                continue;
            }

            if set.contains(test_uuid) {
                false_positives += 1;
            }
        }

        let false_positive_rate = false_positives as f64 / test_size as f64;
        println!("False positive rate: {:.2}%", false_positive_rate * 100.0);

        assert!(
            false_positive_rate < 0.20,
            "False positive rate too high: {:.2}%",
            false_positive_rate * 100.0
        );
    }

    #[test]
    fn test_capacity_vs_collision_rate() {
        let bits_per_item = 16;
        let max_capacity = 1000;

        // Test collision rates at different capacity utilizations
        for utilization in [0.25, 0.5, 0.75, 1.0, 1.25, 1.5] {
            let num_items = (max_capacity as f64 * utilization) as usize;
            let collision_rate =
                measure_uuid_collision_rate(max_capacity, bits_per_item, num_items);

            println!(
                "Utilization: {:.0}% -> Collision rate: {:.2}%",
                utilization * 100.0,
                collision_rate * 100.0
            );
        }
    }

    #[test]
    fn test_deterministic_behavior() {
        // Test that same seed produces same results
        let seed = 12345;
        let mut set1 = ProbSet::with_seed(1000, 16, seed);
        let mut set2 = ProbSet::with_seed(1000, 16, seed);

        let test_uuids: Vec<u128> = (0..100).map(|_| Uuid::new_v4().as_u128()).collect();

        for &uuid in &test_uuids {
            set1.insert(uuid);
            set2.insert(uuid);
        }

        for _ in 0..1000 {
            let test_uuid = Uuid::new_v4().as_u128();
            assert_eq!(
                set1.contains(test_uuid),
                set2.contains(test_uuid),
                "Sets with same seed should behave identically"
            );
        }
    }
    // Helper function to measure collision rate
    fn measure_uuid_collision_rate(
        expected_items: usize,
        bits_per_item: usize,
        num_test_items: usize,
    ) -> f64 {
        let set = ProbSet::new(expected_items, bits_per_item);
        let mut unique_positions = std::collections::HashMap::new();

        for _ in 0..num_test_items {
            let uuid = Uuid::new_v4().as_u128();
            let position = set.hash(uuid) % set.size_bits;
            *unique_positions.entry(position).or_insert(0) += 1;
        }

        let collisions = num_test_items - unique_positions.len();
        collisions as f64 / num_test_items as f64
    }
}
