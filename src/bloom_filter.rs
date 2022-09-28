use crate::{RustyResult};

const BITS_PER_SEGMENT: usize = 8;

#[derive(Debug)]
pub struct BloomFilter {
    pub data: Vec<u8>,
    size: usize,
    seed: u32,
    number_of_bits: f64,
    num_of_hash_functions: f64
}

impl BloomFilter {
    
    pub fn new(max_size: usize, max_tolerance: Option<f32>, seed: Option<u32>) -> RustyResult<BloomFilter> {
        let max_tolerance = evalexpr::eval(&format!("math::ln({})", max_tolerance.unwrap_or(0.01)))?.as_float()?;
        let ln2 = evalexpr::eval("math::ln(2)")?.as_float()?;
        let calc = -(max_size as f64 * max_tolerance / ln2 / ln2).ceil();
        let num_of_hash_functions = -(max_tolerance / ln2).ceil();
        let num_of_elements = (calc / BITS_PER_SEGMENT as f64).ceil() as usize;
        Ok(
        BloomFilter { 
            data: vec![0; num_of_elements],
            seed: seed.unwrap_or(rand::random()),
            size: 0,
            number_of_bits: calc,
            num_of_hash_functions
        })
    } 

    pub fn contains(&self, key: &[u8], positions: Option<&[u128]>) -> bool {
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

    pub fn insert(&mut self, key: &[u8]) {
        let positions = self.key_2_positions(key);
        if !self.contains(key, Some(&positions)) {
            for position in positions {
                self.write_bit(position as usize);
                self.size += 1;
            }
        }
    }

    fn read_bit(&self, index: usize) -> bool {
        let (element, bit) = self.find_bit_coordinates(index);
        if let Some(value) = self.data.get(element) {
            let result = (*value & (1 << bit)) >> bit;
            return result == 1;
        }
        false
    }

    fn false_positive_probability(&self) -> f64 {
        (1.0 - std::f64::consts::E.powf(self.num_of_hash_functions * self.size as f64 / self.number_of_bits)).powf(self.num_of_hash_functions)
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

    fn key_2_positions(&self, key: &[u8]) -> Vec<u128> {
        let murmur_result = fastmurmur3::murmur3_x64_128(key, self.seed) ;
        let fnv1_result = const_fnv1a_hash::fnv1a_hash_128(key, None);

        (0..(self.num_of_hash_functions as u128))
        .map(|x| (murmur_result.wrapping_add(x.wrapping_mul(fnv1_result)).wrapping_add(x.wrapping_mul(x))) % BITS_PER_SEGMENT as u128)
        .collect()
    }
}