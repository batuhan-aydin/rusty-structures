use std::{hash::{Hasher, Hash}, collections::hash_map::DefaultHasher};
use anyhow::Result;

const BITS_PER_SEGMENT: usize = 8;

#[derive(Debug)]
pub struct BloomFilter {
    pub data: Vec<u8>,
    size: usize,
    seed: u64,
    number_of_bits: usize,
    num_of_hash_functions: usize
}

impl BloomFilter {
    
    pub fn new(max_size: usize, max_tolerance: Option<f64>, seed: Option<u64>) -> Result<BloomFilter> {
        let max_tolerance = max_tolerance.unwrap_or(0.01);
        let ln2 = evalexpr::eval("math::ln(2)")?.as_float()?;
        let calc = -(max_size as f64 * f64::ln(max_tolerance) / ln2 / ln2).ceil();
        let num_of_hash_functions = -((max_tolerance.ln()) / ln2).ceil();
        let num_of_elements = (calc / BITS_PER_SEGMENT as f64).ceil() as usize;
        Ok(
        BloomFilter { 
            data: vec![0; num_of_elements],
            seed: seed.unwrap_or(rand::random()),
            size: 0,
            number_of_bits: calc as usize,
            num_of_hash_functions: num_of_hash_functions as usize
        })
    } 

    pub fn contains<T>(&self, key: &T, positions: Option<&[usize]>) -> bool
    where T : Hash {
        let tmp_new;
        let positions = match positions {
            Some(value) => value,
            None => {
                tmp_new = self.key_2_positions(key);
                &tmp_new
            }
        };
        for position in positions {
            if !self.read_bit(*position as usize) {
                return false;
            }
        }
        return true;
    }

    pub fn insert<T>(&mut self, key: &T) where T : Hash {
        let positions = self.key_2_positions(key);
        if !self.contains(key, Some(&positions)) {
            for position in positions {
                self.write_bit(position as usize);
                self.size += 1;
            }
        }
    }

    /* 
    fn false_positive_probability(&self) -> f64 {
        (1.0 - std::f64::consts::E.powf((self.num_of_hash_functions * self.size / self.number_of_bits) as f64)).powf(self.num_of_hash_functions as f64)
    } 
    */

    fn read_bit(&self, index: usize) -> bool {
        let (element, bit) = self.find_bit_coordinates(index);
        if let Some(value) = self.data.get(element) {
            let result = (*value & (1 << bit)) >> bit;
            return result == 1;
        }
        false
    }

    fn write_bit(&mut self, index: usize) {
        let (element, bit) = self.find_bit_coordinates(index);
        if let Some(data) = self.data.get_mut(element) {
            *data = *data | (1_u8 << bit);
        }
    }

    fn find_bit_coordinates(&self, index: usize) -> (usize, usize) {
        let byte_index = index / BITS_PER_SEGMENT;
        let bit_offset = index % BITS_PER_SEGMENT;

        (byte_index, bit_offset)
    }

    fn key_2_positions<T>(&self, key: &T) -> Vec<usize>
    where T : Hash {
        let mut result = Vec::with_capacity(self.num_of_hash_functions as usize);
        for i in 0..self.num_of_hash_functions as usize {
            let mut xxhasher = twox_hash::XxHash64::with_seed(self.seed);
            key.hash(&mut xxhasher);
            let xxhash_result = xxhasher.finish() as usize;
    
            let mut default_hasher = DefaultHasher::default();
            key.hash(&mut default_hasher);
            let default_result = default_hasher.finish() as usize;

            result.push((xxhash_result.wrapping_add(i.wrapping_mul(default_result)).wrapping_add(i.wrapping_mul(i))) % self.number_of_bits);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_oneelement_true() {
        let mut bloom = BloomFilter::new(16, None, None).unwrap();
        bloom.insert(&1_u32.to_be_bytes());
        let result = bloom.contains(&1_u32.to_be_bytes(), None);

        assert!(result);
    }

    #[test]
    fn contains_oneelement_false() {
        let mut bloom = BloomFilter::new(16, None, None).unwrap();
        bloom.insert(&1_u32.to_be_bytes());
        dbg!("{:?}", &bloom.data);
        let result = bloom.contains(&2_u32.to_be_bytes(), None);

        assert!(!result);
    }
}