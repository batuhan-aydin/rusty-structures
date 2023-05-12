use std::{hash::{Hasher, Hash}, collections::hash_map::DefaultHasher};

use anyhow::Result;

const BITS_PER_SEGMENT: usize = 8;

#[derive(Debug)]
pub struct BloomFilter {
    pub data: Vec<u8>,
    size: usize,
    seed: u32,
    //number_of_bits: f64,
    num_of_hash_functions: f64
}

impl BloomFilter {
    
    pub fn new(max_size: usize, max_tolerance: Option<f32>, seed: Option<u32>) -> Result<BloomFilter> {
        let max_tolerance = evalexpr::eval(&format!("math::ln({})", max_tolerance.unwrap_or(1.0)))?.as_float()?;
        let ln2 = evalexpr::eval("math::ln(2)")?.as_float()?;
        let calc = -(max_size as f64 * max_tolerance / ln2 / ln2).ceil();
        let num_of_hash_functions = -(max_tolerance / ln2).ceil();
        let num_of_elements = (calc / BITS_PER_SEGMENT as f64).ceil() as usize;
        Ok(
        BloomFilter { 
            data: vec![0; num_of_elements],
            seed: seed.unwrap_or(rand::random()),
            size: 0,
            //number_of_bits: calc,
            num_of_hash_functions
        })
    } 

    pub fn contains<T>(&self, key: &T, positions: Option<&[u128]>) -> bool
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
    pub fn false_positive_probability(&self) -> f64 {
        (1.0 - std::f64::consts::E.powf(self.num_of_hash_functions * self.size as f64 / self.number_of_bits)).powf(self.num_of_hash_functions)
    } */

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

    fn key_2_positions<T>(&self, key: &T) -> Vec<u128>
    where T : Hash {
        let mut xxhasher = twox_hash::XxHash64::with_seed(i);
        key.hash(&mut xxhasher);
        let xxhash_result = xxhasher.finish();

        let mut default_hasher = DefaultHasher::default();
        key.hash(&mut default_hasher);
        let default_result = default_hasher.finish();

        let result = (0..self.num_of_hash_functions as u64)
        .map(|i| (xxhash_result + i * default_result + i * i) % self)
        .
        //let murmur_result = fastmurmur3::murmur3_x64_128(key, self.seed) ;
        //let fnv1_result = const_fnv1a_hash::fnv1a_hash_128(key, None);

        //(0..(self.num_of_hash_functions as u128))
        //.map(|x| (murmur_result.wrapping_add(x.wrapping_mul(fnv1_result)).wrapping_add(x.wrapping_mul(x))) % (self.data.len() * BITS_PER_SEGMENT) as u128)
        //.collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_oneelement_true() {
        let mut bloom = BloomFilter::new(8, None, None).unwrap();
        bloom.insert(&1_u32.to_be_bytes());
        let result = bloom.contains(&1_u32.to_be_bytes(), None);

        assert!(result);
    }

    #[test]
    fn contains_oneelement_false() {
        let mut bloom = BloomFilter::new(8, None, None).unwrap();
        bloom.insert(&1_u32.to_be_bytes());
        let result = bloom.contains(&2_u32.to_be_bytes(), None);

        assert!(!result);
    }
}