use std::{hash::{Hash, Hasher}, collections::hash_map::DefaultHasher};

use hashers::fnv::FNV1aHasher64;

pub mod acmsketch;

const HASH_COUNT: u64 = 3;

/// Base data type for count-min-sketch
/// For thread safe version, check out AcmSketch
pub struct CmSketch {
    data: Vec<u64>,
    capacity: u64,
}

impl CmSketch {
    /// Creates a new count-min-sketch
    // Internally we'll use just an array and 3 hash functions
    // Instead of array of arrays we can use mod operation + a single array
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            data: vec![0; (HASH_COUNT * capacity).try_into().unwrap()]
        }
    }

    pub fn update<T>(&mut self, value: T) where T : Hash {
        self.data[Self::default_hash(self.capacity, &value)] += 1;
        self.data[Self::xxhash(self.capacity, &value) + 8] += 1;
        self.data[Self::fnv_hash(self.capacity, &value) + 16] += 1;
    }

    pub fn estimate<T>(&self, value: T) -> u64
    where T : Hash {
        let result_1 = self.data[Self::default_hash(self.capacity, &value)];
        let result_2 = self.data[Self::xxhash(self.capacity, &value) + 8];
        let result_3 = self.data[Self::fnv_hash(self.capacity, &value) + 16];
        let mut smallest = result_1;
        if result_2 < smallest { smallest = result_2; }
        if result_3 < smallest { smallest = result_3; }
        smallest
    }

    fn default_hash<T>(capacity: u64, value: &T) -> usize  
    where T : Hash {
        let mut default_hasher = DefaultHasher::new();
        value.hash(&mut default_hasher);
        let result = default_hasher.finish() % capacity;
        result.try_into().unwrap()
    }

    fn xxhash<T>(capacity: u64, value: &T) -> usize  
    where T : Hash {
        let mut xxhasher = xxhash_rust::xxh3::Xxh3::default();
        value.hash(&mut xxhasher);
        let result = xxhasher.finish() % capacity;
        result.try_into().unwrap()
    }

    fn fnv_hash<T>(capacity: u64, value: &T) -> usize  
    where T : Hash {
        let mut fnvhasher = FNV1aHasher64::default();
        value.hash(&mut fnvhasher);
        let result = fnvhasher.finish() % capacity;
        result.try_into().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_updated() {
        let mut sketch = CmSketch::new(8);
        sketch.update(5);
        let result = sketch.estimate(5);
        assert_eq!(1, result);
    }

    #[test]
    fn same_element_multiple_times_updated() {
        let mut sketch = CmSketch::new(8);
        sketch.update(5);
        sketch.update(5);
        sketch.update(5);
        let result = sketch.estimate(5);
        assert_eq!(3, result);
    }

    // Probabilistic test, sometime may fail even though it is correct
    #[test]
    fn different_elements_single_time_updated() {
        let mut sketch = CmSketch::new(24);
        sketch.update(3);
        sketch.update(4);
        sketch.update(5);
        let result = sketch.estimate(5);
        assert_eq!(1, result);
    }

        // Probabilistic test, sometime may fail even though it is correct
        #[test]
        fn different_elements_multiple_time_updated() {
            let mut sketch = CmSketch::new(24);
            sketch.update(3);
            sketch.update(3);
            sketch.update(4);
            sketch.update(4);
            sketch.update(4);
            sketch.update(5);
            let result1 = sketch.estimate(3);
            assert_eq!(2, result1);
            let result2 = sketch.estimate(4);
            assert_eq!(3, result2);
            let result3 = sketch.estimate(5);
            assert_eq!(1, result3);
        }
}