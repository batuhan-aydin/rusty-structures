use std::hash::{Hash, Hasher};

use bitvec::prelude::*;
use anyhow::Result;

#[derive(Debug)]
pub struct SimpleBloomFilter {
    number_of_elements: u64,
    false_positive_rate: f64,
    space: u64,
    number_of_hash_functions: u64,
    bit_array: BitVec
}

impl SimpleBloomFilter {
    pub fn new(number_of_elements: u64, false_positive_rate: f64) -> Result<SimpleBloomFilter> {
        
        let mut result = Self {
            number_of_elements,
            false_positive_rate,
            space: 0,
            number_of_hash_functions: 0,
            bit_array: BitVec::EMPTY
        };

        result.space  = result.calculate_space()?;
        result.number_of_hash_functions = result.calculate_number_of_hash_functions()?;
        result.bit_array = bitvec![0; result.space as usize];

        Ok(result)
    }

    pub fn insert(&mut self, item: &[u8]) {
        for i in 0..self.number_of_hash_functions {
            let mut hasher = twox_hash::XxHash64::with_seed(i);
            item.hash(&mut hasher);
            let index = hasher.finish() % self.space;
            self.bit_array.set(index as usize, true);
        }
    }

    pub fn lookup(&self, item: &[u8]) -> bool {
        for i in 0..self.number_of_hash_functions {
            let mut hasher = twox_hash::XxHash64::with_seed(i);
            item.hash(&mut hasher);
            let index = hasher.finish() % self.space;
            if let Some(value) = self.bit_array.get(index as usize) {
                if value == true { continue; }
            } 
            return false;
        }
        true
    }

    fn calculate_space(&self) -> Result<u64> {
        let first_phase = -evalexpr::eval(&format!("math::ln({})", self.false_positive_rate))?.as_float()? * 
    self.number_of_elements as f64;
        let second_phase = f64::powf(evalexpr::eval("math::ln(2)")?.as_float()?, 2_f64);
        let result = first_phase / second_phase;
        Ok(result as u64)
    }

    fn calculate_number_of_hash_functions(&self) -> Result<u64> {
        let result = self.space as f64 * evalexpr::eval("math::ln(2)")?.as_float()?
        / self.number_of_elements as f64;
        Ok(result as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_oneelement_returnstrue() {
        let mut bloom = SimpleBloomFilter::new(10, 0.01).unwrap();
        bloom.insert(&1_u32.to_be_bytes());
        let result = bloom.lookup(&1_u32.to_be_bytes());
        assert!(result);
    }

    #[test]
    fn lookup_oneelement_returnsfalse() {
        let mut bloom = SimpleBloomFilter::new(10, 0.01).unwrap();
        bloom.insert(&1_u32.to_be_bytes());
        let result = bloom.lookup(&2_u32.to_be_bytes());
        assert!(!result);
    }
}