use super::{MetadataType, slot::Slot};
use anyhow::{Result, Ok};


/// The base filter struct. Size of quotient(index) and remainder(hash result's bit count - quotient)
/// Size is how many bucket? Table is just keeping buckets.
pub struct QuotientFilter {
    quotient: usize,
    remainder: u32,
    size: usize,
    pub table: Vec<Slot>  
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

    /// Reads byte-value using fnv1a
    pub fn read_value(&mut self, value: &[u8]) -> Result<bool> {
        let fingerprint =  const_fnv1a_hash::fnv1a_hash_64(value, None);
        self.lookup(fingerprint)
    }

    /// Inserts the element by using custom fingerprint and returns the index
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

            // Going to start of the cluster. Cluster is one or more runs.
            let mut b = self.get_start_of_the_cluster(quotient);
            let mut s = b;
            // We want to skip runs that have different quotient here
            // b tracks occupied slots, and s tracks corresponding runs
            while b != quotient {
                // go to lowest in the run
                s = self.index_up(s);
                s = self.get_lowest_of_run(s);
                b = self.index_up(b);

                // skip empty slots
                b = self.skip_empty_slots(b);
            }

            // Find the insert spot
            // s is here at the start of the run
            let mut first_run = false;
            while let Some(bucket) = self.table.get(s) {
                if !bucket.is_empty() && remainder > bucket.remainder { s  = self.index_up(s) }
                else { first_run = true; break; }
            }

            //  If it came to here, the quotient's place must be full. So it has to be shifted.
            let insert_index = s;
            let mut new_slot = Slot::new_with_reminder(remainder);
            if quotient != insert_index { new_slot.set_metadata(MetadataType::IsShifted) };
            if first_run { new_slot.set_metadata(MetadataType::RunContinued); }

            // shift other ones
            // while we are shifting buckets, is_shifted should be updated as 1
            // however we shouldn't shift bucket_occupied bits
            let mut tmp_bucket = Slot::default();
            while let Some(bucket) = self.table.get_mut(s) {
                if bucket.is_empty() { break; }

                if tmp_bucket.get_metadata(MetadataType::BucketOccupied) { tmp_bucket.set_metadata(MetadataType::BucketOccupied); }
                tmp_bucket = std::mem::replace(bucket, tmp_bucket);
                tmp_bucket.set_metadata(MetadataType::IsShifted);
                s = self.index_up(s);
                
                if self.table[s].is_empty() {
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

    /// Returns if the element exists, by using custom fingerprint
    pub fn lookup(&mut self, fingerprint: u64) -> Result<bool> {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint)?;

        // The buckets are quotient-indexed. Remember, we have number of 2^quotient buckets.
        if let Some(bucket) = self.table.get(quotient) {
            if !bucket.get_metadata(MetadataType::BucketOccupied) { return Ok(false); }
        } else { return Ok(false); }


         // Going to start of the cluster. Cluster is one or more runs.
        let mut b = self.get_start_of_the_cluster(quotient);

        let mut s = b;

        // We want to skip runs that have different quotient here
        // b tracks occupied buckets, and s tracks corresponding runs
        while b != quotient {
            // go to lowest in the run
            s = self.index_up(s);
            s = self.get_lowest_of_run(s);
            b = self.index_up(b);

            // skip empty buckets
            b = self.skip_empty_slots(b);
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

    fn get_start_of_the_cluster(&self, start_index: usize) -> usize {
        let mut index = start_index;
        while let Some(slot) = self.table.get(index) {
            if slot.get_metadata(MetadataType::IsShifted) { index = self.index_down(index); }
            else { break; }
        }
        index
    }

    fn get_lowest_of_run(&self, start_index: usize) -> usize {
        let mut index = start_index;
        while let Some(slot) = self.table.get(index) {
            if slot.get_metadata(MetadataType::RunContinued) { index = self.index_up(index) }
            else { break; }
        }
        index
    }

    fn skip_empty_slots(&self, start_index: usize) -> usize {
        let mut index = start_index;
        while let Some(bucket) = self.table.get(index) {
            if !bucket.get_metadata(MetadataType::BucketOccupied) { index = self.index_up(index) }
            else { break; }
        }
        index
    }

    #[inline(always)]
    fn index_up(&self, old_index: usize) -> usize {
        (old_index + 1) % (self.size)
    }

    #[inline(always)]
    fn index_down(&self, old_index: usize) -> usize {
        if old_index == 0 { return self.size; }
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