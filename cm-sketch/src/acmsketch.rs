use std::{hash::{Hash, Hasher}, collections::hash_map::DefaultHasher, sync::RwLock};
use anyhow::anyhow;
use anyhow::Result;

use hashers::fnv::FNV1aHasher64;

const HASH_COUNT: u64 = 3;

/// Thread safe implementation of count-min-sketch
pub struct AcmSketch {
    data: Vec<RwLock<u64>>,
    capacity: u64
}

impl AcmSketch {
    /// Creates a new count-min-sketch
    // Internally we'll use just an array and 3 hash functions
    // Instead of array of arrays we can use mod operation + a single array
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            data: (0..(HASH_COUNT * capacity)).map(|_| RwLock::new(0u64)).collect()
        }
    }

    pub fn update<T>(&mut self, value: T) -> Result<()>
    where T : Hash {
        let mut lock1 = self.data[Self::default_hash(self.capacity, &value)].write()
        .map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        let mut lock2 = self.data[Self::xxhash(self.capacity, &value) + 8].write()
        .map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;
        let mut lock3 = self.data[Self::fnv_hash(self.capacity, &value) + 16].write()
        .map_err(|e| anyhow!("Failed to acquire write lock: {}", e))?;

        *lock1 += 1;
        *lock2 += 1;
        *lock3 += 3;

        Ok(())
    }

    pub fn estimate<T>(&self, value: T) -> Result<u64>
    where T : Hash {
        let lock1 = self.data[Self::default_hash(self.capacity, &value)].read()
        .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        let lock2 = self.data[Self::xxhash(self.capacity, &value) + 8].read()
        .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        let lock3 = self.data[Self::fnv_hash(self.capacity, &value) + 16].read()
        .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))?;
        let mut smallest = *lock1;
        if *lock2 < smallest { smallest = *lock2; }
        if *lock3 < smallest { smallest = *lock3; }
        Ok(smallest)
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
        let mut sketch =AcmSketch::new(8);
        sketch.update(5).unwrap();
        let result = sketch.estimate(5).unwrap();
        assert_eq!(1, result);
    }

    #[test]
    fn same_element_multiple_times_updated() {
        let mut sketch = AcmSketch::new(8);
        sketch.update(5).unwrap();
        sketch.update(5).unwrap();
        sketch.update(5).unwrap();
        let result = sketch.estimate(5).unwrap();
        assert_eq!(3, result);
    }

    // Probabilistic test, sometime may fail even though it is correct
    #[test]
    fn different_elements_single_time_updated() {
        let mut sketch = AcmSketch::new(24);
        sketch.update(3).unwrap();
        sketch.update(4).unwrap();
        sketch.update(5).unwrap();
        let result = sketch.estimate(5).unwrap();
        assert_eq!(1, result);
    }

        // Probabilistic test, sometime may fail even though it is correct
        #[test]
        fn different_elements_multiple_time_updated() {
            let mut sketch = AcmSketch::new(24);
            sketch.update(3).unwrap();
            sketch.update(3).unwrap();
            sketch.update(4).unwrap();
            sketch.update(4).unwrap();
            sketch.update(4).unwrap();
            sketch.update(5).unwrap();
            let result1 = sketch.estimate(3).unwrap();
            assert_eq!(2, result1);
            let result2 = sketch.estimate(4).unwrap();
            assert_eq!(3, result2);
            let result3 = sketch.estimate(5).unwrap();
            assert_eq!(1, result3);
        }
}