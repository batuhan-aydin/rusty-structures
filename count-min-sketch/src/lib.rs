#![allow(dead_code)]

use std::{hash::{Hash, Hasher}, collections::hash_map::DefaultHasher};
use hashers::fnv::FNV1aHasher64;
use thiserror::Error;
use std::f64::consts::E;

#[derive(Error, Debug)]
pub enum CountMinError {
    #[error("epsilon and delta must be between 0 an 1")]
    WrongInput,
}

/// Base data type for count-min-sketch
pub struct CountMinSketch {
    data: Vec<u64>,
    depth: usize,
    width: usize,
}

impl CountMinSketch {
    /// Creates a new count-min-sketch
    /// epsilon is error rate
    /// delta is the confidence to correctness of estimate, the smaller the higher we're more confident 
    /// both epsilon and delta must be between 0 and 1, for instance 0,5
    pub fn new(epsilon: f64, delta: f64) -> Result<Self, CountMinError> {
        if epsilon < 0.0 || delta < 0.0 || epsilon > 1.0 || delta > 1.0 {
            return Err(CountMinError::WrongInput);
        }

        let width = (E / epsilon).ceil() as usize;
        let depth = (1. / delta).ln().ceil() as usize;
        Ok(Self {
            data: vec![0; width * depth],
            width,
            depth
        })
    }

    pub fn update<T>(&mut self, value: T, frequency: Option<u64>) where T : Hash {
        for i in 0..self.depth {
            self.data[Self::xxhash(self.width  as u64, &value, i as u64) + (i * self.width)] += frequency.unwrap_or(1)
        }
    }

    pub fn estimate<T>(&self, value: T) -> u64 where T : Hash {
        let mut smallest = u64::MAX;
        for i in 0..self.depth {
            let count = self.data[Self::xxhash(self.width as u64, &value, i as u64) + (i * self.width)];
            if count < smallest { smallest = count; }
        }
        smallest
    }

    fn default_hash<T>(capacity: u64, value: &T, seed: u64) -> usize  
    where T : Hash {
        let mut default_hasher = DefaultHasher::new();
        default_hasher.write_u64(seed);
        value.hash(&mut default_hasher);
        let result = default_hasher.finish() % capacity;
        result.try_into().unwrap()
    }

    fn xxhash<T>(capacity: u64, value: &T, seed: u64) -> usize  
    where T : Hash {
        let mut xxhasher = xxhash_rust::xxh3::Xxh3::default();
        xxhasher.write_u64(seed);
        value.hash(&mut xxhasher);
        let result = xxhasher.finish() % capacity;
        result.try_into().unwrap()
    }

    fn fnv_hash<T>(capacity: u64, value: &T, seed: u64) -> usize  
    where T : Hash {
        let mut fnvhasher = FNV1aHasher64::default();
        fnvhasher.write_u64(seed);
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
        let mut sketch = CountMinSketch::new(0.1, 0.1).unwrap();
        sketch.update(5, Some(1));
        let result = sketch.estimate(5);
        assert_eq!(1, result);
    }

    #[test]
    fn same_element_multiple_times_updated() {
        let mut sketch = CountMinSketch::new(0.1, 0.1).unwrap();
        sketch.update(5, Some(1));
        sketch.update(5, Some(1));
        sketch.update(5, Some(1));
        let result = sketch.estimate(5);
        assert_eq!(3, result);
    }

    // Probabilistic test, sometime may fail even though it is correct
    #[test]
    fn different_elements_single_time_updated() {
        let mut sketch = CountMinSketch::new(0.1, 0.1).unwrap();
        sketch.update(3, Some(1));
        sketch.update(4, Some(1));
        sketch.update(5, Some(1));
        let result = sketch.estimate(5);
        assert_eq!(1, result);
    }

        // Probabilistic test, sometime may fail even though it is correct
        #[test]
        fn different_elements_multiple_time_updated() {
            let mut sketch = CountMinSketch::new(0.1, 0.1).unwrap();
            sketch.update(3, Some(1));
            sketch.update(3, Some(1));
            sketch.update(4, Some(1));
            sketch.update(4, Some(1));
            sketch.update(4, Some(1));
            sketch.update(5, Some(1));
            let result1 = sketch.estimate(3);
            assert_eq!(2, result1);
            let result2 = sketch.estimate(4);
            assert_eq!(3, result2);
            let result3 = sketch.estimate(5);
            assert_eq!(1, result3);
        }
}