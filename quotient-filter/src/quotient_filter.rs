use super::{MetadataType, slot::Slot};
use anyhow::{Result, Ok};


/// The base filter struct. Size of quotient(index) and remainder(hash result's bit count - quotient)
/// Size is how many bucket? Table is just keeping buckets.
pub struct QuotientFilter {
    quotient: usize,
    remainder: u32,
    size: usize,
    table: Vec<Slot>  
}

impl QuotientFilter {
    /// How many bits are the quotient and the remainder. Size will be 2^quotient.
    pub fn new(quotient: usize) -> Result<Self> {
        let quotient_u32 = u32::try_from(quotient)?;
        let size = usize::pow(2, quotient_u32);
        Ok(Self { quotient, remainder: 64 - quotient_u32, size, table: vec![Slot::new(); size] })
    }

    /// Inserts byte-value using fnv1a 
    pub fn insert_value(&mut self, value: &[u8]) -> Result<usize> {
        let fingerprint =  const_fnv1a_hash::fnv1a_hash_64(value, None);
        self.insert(fingerprint)
    }

    pub fn read_value(&mut self, value: &[u8]) -> Result<bool> {
        let fingerprint =  const_fnv1a_hash::fnv1a_hash_64(value, None);
        self.lookup(fingerprint)
    }

    /// Inserts the element and returns the index
    pub fn insert(&mut self, fingerprint: u64) -> Result<usize> {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint)?;
        // mark the appropriate as occupied
        if let Some(bucket) = self.table.get_mut(quotient) {
            bucket.set_metadata(MetadataType::BucketOccupied);
            // if selected is empty, we can set and return
            if bucket.remainder == 0 {
                bucket.set_remainder(remainder);               
                return Ok(quotient);
            }

            let mut b = quotient;
            // Going to start of the cluster. Cluster is one or more runs.
            while let Some(bucket) = self.table.get(b) {
                if bucket.get_metadata(MetadataType::IsShifted) { b = self.index_down(b) }
                else { break; }
            }
            let mut s = b;
            // We want to skip runs that have different quotient here
            // b tracks occupied buckets, and s tracks corresponding runs
            while b != quotient {
                // go to lowest in the run
                s = self.index_up(s);
                while let Some(bucket) = self.table.get(s) {
                    if bucket.get_metadata(MetadataType::RunContinued) { s = self.index_up(s) }
                    else { break; }
                }
                b = self.index_up(b);

                // skip empty buckets
                while let Some(bucket) = self.table.get(b) {
                    if !bucket.get_metadata(MetadataType::BucketOccupied) { b = self.index_up(s) }
                    else { break; }
                }
            }

            // Find the insert spot
            while let Some(bucket) = self.table.get(s) {
                if bucket.remainder != 0 && remainder > bucket.remainder { s  = self.index_up(s) }
                else { break; }
            }

            let insert_index = s;
            let mut new_slot = self.table[s].new_from_slot(remainder);
            new_slot.set_metadata(MetadataType::IsShifted);
            new_slot.set_metadata(MetadataType::RunContinued);
            // shift other ones
            // while we are shifting buckets, is_shifted should be updated as 1
            // however we shouldn't shift bucket_occupied bits
            let mut tmp_bucket = Slot::default();
            while let Some(bucket) = self.table.get_mut(s) {
                if tmp_bucket.get_metadata(MetadataType::BucketOccupied) { tmp_bucket.set_metadata(MetadataType::BucketOccupied); }
                tmp_bucket = std::mem::replace(bucket, tmp_bucket);
                tmp_bucket.set_metadata(MetadataType::IsShifted);
                s = self.index_up(s);
                
                if self.table[s].remainder == 0 {
                    self.table[s] = tmp_bucket;
                    break;
                }
            }
            // here shifting is done. now we have to insert our new bucket using insert_index
            self.table[insert_index] = new_slot;
            return Ok(insert_index)

        } 


        Ok(0)
    }

    /// Returns if the element exists.
    /// Fingerprint is the result of the hash(element).
    // In order to find, we have to search the whole cluster. Cluster is one or more run sequence. A run is a sequence of remainders that have the same quotient.
    pub fn lookup(&mut self, fingerprint: u64) -> Result<bool> {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint)?;

        // The buckets are quotient-indexed. Remember, we have number of 2^quotient buckets.
        if let Some(bucket) = self.table.get(quotient) {
            if !bucket.get_metadata(MetadataType::BucketOccupied) { return Ok(false); }
        } else { return Ok(false); }
        let mut b = quotient;
        // Going to start of the cluster. Cluster is one or more runs.
        while let Some(bucket) = self.table.get(b) {
            if bucket.get_metadata(MetadataType::IsShifted) { b = self.index_down(b); }
            else { break; }
        }
        let mut s = b;

        // We want to skip runs that have different quotient here
        // b tracks occupied buckets, and s tracks corresponding runs
        while b != quotient {
            // go to lowest in the run
            s = self.index_up(s);
            while let Some(bucket) = self.table.get(s) {
                if bucket.get_metadata(MetadataType::RunContinued) { s = self.index_up(s); }
                else { break; }
            }
            b = self.index_up(b);

            // skip empty buckets
            while let Some(bucket) = self.table.get(b) {
                if !bucket.get_metadata(MetadataType::BucketOccupied) { b = self.index_up(b); }
                else { break; }
            }
        }

        // Now s is at the start of the run where our element might be in
        while let Some(bucket) = self.table.get(s) {
            if bucket.remainder != remainder {
                s = self.index_up(s);
                if !self.table[s].get_metadata(MetadataType::RunContinued) { return Ok(false); }
            } else {
                break;
            }
        }  
        Ok(true)
    }

    /// Gets the fingerprint(hashed value), returns quotient and remainder
    fn fingerprint_destruction(&self, fingerprint: u64) -> Result<(usize, u64)> {
        let quotient = fingerprint / u64::pow(2, self.remainder);
        let remainder = fingerprint % u64::pow(2, self.remainder);
        let quotient_usize = usize::try_from(quotient)?;
        Ok((quotient_usize, remainder))
    }

    #[inline(always)]
    fn index_up(&self, old_index: usize) -> usize {
        (old_index + 1) % (self.size - 1)
    }

    #[inline(always)]
    fn index_down(&self, old_index: usize) -> usize {
        if old_index == 0 { return self.size - 1; }
        old_index - 1
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_read_one_success() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        let result = filter.read_value(&1_u8.to_be_bytes()).unwrap();

        assert!(result);
    }

    #[test]
    fn insert_and_read_multiple_success() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        _ = filter.insert_value(&2_u8.to_be_bytes());
        _ = filter.insert_value(&3_u8.to_be_bytes());
        let result = filter.read_value(&2_u8.to_be_bytes()).unwrap();

        assert!(result);
    }

    #[test]
    fn insert_and_read_one_failure() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        let result = filter.read_value(&2_u8.to_be_bytes()).unwrap();

        assert!(!result);
    }

    #[test]
    fn insert_and_read_multiple_failure() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        _ = filter.insert_value(&2_u8.to_be_bytes());
        _ = filter.insert_value(&3_u8.to_be_bytes());
        let result = filter.read_value(&4_u8.to_be_bytes()).unwrap();
        
        assert!(!result);
    }
}