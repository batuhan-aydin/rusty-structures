use super::{MetadataType, slot::Slot};
use anyhow::{Result, Ok};


/// The base filter struct. Size of quotient and remainder(how many bits?)
/// Size is how many bucket? Table is just keeping buckets.
pub struct QuotientFilter {
    quotient: u32,
    remainder: u32,
    size: u32,
    table: Vec<Slot>  
}

impl QuotientFilter {
    /// How many bits are the quotient and the remainder. Size will be 2^quotient.
    pub fn new(quotient: u32) -> Self {
        let size = u32::pow(2, quotient);
        Self { quotient, remainder: 64_u32 - quotient, size, table: vec![Slot::new(); size as usize] }
    }

    /// Inserts byte-value using fnv1a 
    pub fn insert_value(&mut self, value: &[u8]) {
        let fingerprint =  const_fnv1a_hash::fnv1a_hash_64(value, None);
        let result = self.insert(fingerprint);
    }

    pub fn read_value(&mut self, value: &[u8]) -> bool {
        let fingerprint =  const_fnv1a_hash::fnv1a_hash_64(value, None);
        self.lookup(fingerprint)
    }

    /// Inserts the element and returns the index
    pub fn insert(&mut self, fingerprint: u64) -> Result<usize> {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint);
        // mark the appropriate as occupied
        if let Some(bucket) = self.table.get_mut(quotient as usize) {
            bucket.set_metadata(MetadataType::BucketOccupied);
            // if selected is empty, we can set and return
            if bucket.remainder == 0 {
                bucket.set_remainder(remainder);               
                return Ok(quotient as usize);
            }

            let mut b = quotient;
            // Going to start of the cluster. Cluster is one or more runs.
            while let Some(bucket) = self.table.get(b as usize) {
                if bucket.get_metadata(MetadataType::IsShifted) { b -= 1; }
                else { break; }
            }
            let mut s = b;
            // We want to skip runs that have different quotient here
            // b tracks occupied buckets, and s tracks corresponding runs
            while b != quotient {
                // go to lowest in the run
                s += 1;
                while let Some(bucket) = self.table.get(s as usize) {
                    if bucket.get_metadata(MetadataType::RunContinued) { s += 1; }
                    else { break; }
                }
                b += 1;

                // skip empty buckets
                while let Some(bucket) = self.table.get(b as usize) {
                    if !bucket.get_metadata(MetadataType::BucketOccupied) { b += 1; }
                    else { break; }
                }
            }

            // Find the insert spot
            while let Some(bucket) = self.table.get(s as usize) {
                if bucket.remainder != 0 && remainder > bucket.remainder { s += 1; }
                else { break; }
            }

            let insert_index = s;
            let mut new_slot = self.table[s as usize].new_from_slot(remainder);
            new_slot.set_metadata(MetadataType::IsShifted);
            new_slot.set_metadata(MetadataType::RunContinued);
            // shift other ones
            // while we are shifting buckets, is_shifted should be updated as 1
            // however we shouldn't shift bucket_occupied bits
            let mut tmp_bucket = Slot::default();
            while let Some(bucket) = self.table.get_mut(s as usize) {
                if tmp_bucket.get_metadata(MetadataType::BucketOccupied) { tmp_bucket.set_metadata(MetadataType::BucketOccupied); }
                tmp_bucket = std::mem::replace(bucket, tmp_bucket);
                tmp_bucket.set_metadata(MetadataType::IsShifted);
                s = (s + 1) % 4;
                
                if self.table[s as usize].remainder == 0 {
                    self.table[s as usize] = tmp_bucket;
                    break;
                }
            }
            // here shifting is done. now we have to insert our new bucket using insert_index
            self.table[insert_index as usize] = new_slot;
            return Ok(insert_index as usize)

        } 


        Ok(0)
    }

    /// Returns if the element exists.
    /// Fingerprint is the result of the hash(element).
    // In order to find, we have to search the whole cluster. Cluster is one or more run sequence. A run is a sequence of remainders that have the same quotient.
    pub fn lookup(&mut self, fingerprint: u64) -> bool {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint);

        // The buckets are quotient-indexed. Remember, we have number of 2^quotient buckets.
        if let Some(bucket) = self.table.get(quotient as usize) {
            if !bucket.get_metadata(MetadataType::BucketOccupied) { return false; }
        } else { return false; }
        let mut b = quotient;
        // Going to start of the cluster. Cluster is one or more runs.
        while let Some(bucket) = self.table.get(b as usize) {
            if bucket.get_metadata(MetadataType::IsShifted) { b -= 1; }
            else { break; }
        }
        let mut s = b;

        // We want to skip runs that have different quotient here
        // b tracks occupied buckets, and s tracks corresponding runs
        while b != quotient {
            // go to lowest in the run
            s += 1;
            while let Some(bucket) = self.table.get(s as usize) {
                if bucket.get_metadata(MetadataType::RunContinued) { s += 1; }
                else { break; }
            }
            b += 1;

            // skip empty buckets
            while let Some(bucket) = self.table.get(b as usize) {
                if !bucket.get_metadata(MetadataType::BucketOccupied) { b += 1; }
                else { break; }
            }
        }

        // Now s is at the start of the run where our element might be in
        while let Some(bucket) = self.table.get(s as usize) {
            if bucket.remainder != remainder {
                s = (s + 1) % 4;
                if !self.table[s as usize].get_metadata(MetadataType::RunContinued) { return false; }
            } else {
                break;
            }
        }  
        true
    }

    fn fingerprint_destruction(&self, fingerprint: u64) -> (u64, u64) {
        let quotient = fingerprint / u64::pow(2, self.remainder);
        let remainder = fingerprint % u64::pow(2, self.remainder);

        (quotient, remainder)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_read_one_success() {
        let mut filter = QuotientFilter::new(5);
        filter.insert_value(&1_u8.to_be_bytes());
        let result = filter.read_value(&1_u8.to_be_bytes());

        assert!(result);
    }

    #[test]
    fn insert_and_read_one_failure() {
        let mut filter = QuotientFilter::new(5);
        filter.insert_value(&1_u8.to_be_bytes());
        let result = filter.read_value(&2_u8.to_be_bytes());

        assert!(!result);
    }
}