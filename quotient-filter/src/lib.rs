use slot::Slot;
use thiserror::Error;
use anyhow::{Result, Ok};

pub mod slot;

/// Tombstone: Is the particular bucket has a deleted element? TODO: implement
/// BucketOccupied: Any hash result with the particular quotient?
/// RunContinued: Is the particular bucket has the same quotient with the upper one?
/// IsShifted: Is the particular bucket in its original bucket?
enum MetadataType {
    Tombstone,
    BucketOccupied,
    RunContinued,
    IsShifted
}

#[derive(Error, Debug)]
enum QuotientFilterError {
    #[error("Invalid quotient access: `{0}`")]
    InvalidQuotientAccess(usize),
    #[error("Quotient cannot be more than 62 due to 64 bit hashing")]
    InvalidQuotientSize,
    #[error("Filters need to have the same size for merging")]
    NotEqualSize
}

#[derive(Default)]
struct ResizeHandler {
    index_up: ResizeOption,
    insert: ResizeOption,
    position: Position
}

#[derive(Default, PartialEq)]
enum ResizeOption {
    #[default]
    None,
    Original,
    Other,
    Both
}

#[derive(Default, PartialEq)]
enum Position {
    Equal,
    #[default]
    Different
}

pub struct QuotientFilter {
    remainder: u8,
    size: usize,
    table: Vec<Slot>  
}

impl QuotientFilter {
    pub fn new(quotient_size: u8) -> Result<Self> {
        if quotient_size > 62 { return Err(anyhow::Error::new(QuotientFilterError::InvalidQuotientSize)); }
        let size = usize::pow(2, quotient_size as u32);
        let remainder = 64 - quotient_size;
        
        Ok(Self {
            remainder,
            size,
            table: vec![Slot::new(); size]
        })
    }

        /// Inserts byte-value using fnv1a 
    pub fn insert_value(&mut self, value: &[u8]) -> Result<usize> {
        let fingerprint =  const_fnv1a_hash::fnv1a_hash_64(value, None);
        self.insert(fingerprint)
    }

    /// Reads byte-value using fnv1a
    pub fn read_value(&mut self, value: &[u8]) -> bool {
        let fingerprint =  const_fnv1a_hash::fnv1a_hash_64(value, None);
        self.lookup(fingerprint)
    }

    /// Deleted byte-value using fnv1a
    pub fn delete_value(&mut self, value: &[u8]) {
        let fingerprint =  const_fnv1a_hash::fnv1a_hash_64(value, None);
        self.delete(fingerprint);
    }

    /// Doubles the size of the table
    // We have to get its fingerprint back then insert again
    // TODO
    fn resize(&mut self) {
        let mut index: usize = 0;
        let mut old_table = std::mem::replace(&mut self.table, vec![Slot::new(); self.size * 2]);
        while let Some(bucket) = old_table.get_mut(index) {
            if !bucket.is_empty() {
                let mut fingerprint: u64 = 0;
                if bucket.get_metadata(MetadataType::RunContinued) {
                    let mut run_head_idx = index - 1;
                    while let Some(bucket) = old_table.get_mut(run_head_idx) {
                        if !bucket.get_metadata(MetadataType::RunContinued) { break; }
                        else { run_head_idx = self.index_down(run_head_idx); }
                    }
                }
                //let (new_index, new_slot) = bucket.get_new_slot(index, self.remainder, self.size);
                //new_table[new_index as usize] = new_slot;            
            }
            index = self.index_up(index);
            if index == 0 { break; }
        }
        //self.size *= 2;
        //self.remainder -= 1;
        //self.table = new_table;
    }

    /// Merges a second filter into original one and doubles its original size. They have to have the same size.
    // TODO
    fn merge(&mut self, other: &QuotientFilter) -> Result<()> {
        if self.size != other.size { return Err(anyhow::Error::new(QuotientFilterError::NotEqualSize)); }
        let mut new_table = vec![Slot::new(); self.size * 2];
        let mut resize_handler = ResizeHandler::default();
        let mut i = 0;
        let mut j = 0;
        while i < self.size && j < self.size {
            if self.table[i].is_empty() && other.table[j].is_empty() { 
                resize_handler.index_up = ResizeOption::Both;
            }
            else if self.table[i].is_empty() { 
                resize_handler.index_up = ResizeOption::Both;
                resize_handler.insert = ResizeOption::Other;
            } else if other.table[j].is_empty() {
                resize_handler.insert = ResizeOption::Original;
                resize_handler.index_up = ResizeOption::Both;
            } else {
                if self.table[i].remainder == self.table[j].remainder {
                    resize_handler.insert = ResizeOption::Original;
                    resize_handler.index_up = ResizeOption::Both;
                } else if self.table[i].remainder < self.table[j].remainder {
                    resize_handler.insert = ResizeOption::Original;
                    resize_handler.index_up = ResizeOption::Original;
                } else {
                    resize_handler.insert = ResizeOption::Other; 
                    resize_handler.index_up = ResizeOption::Other;
                }
            }

            resize_handler.position = if i == j { Position::Equal } else { Position::Different };

            if resize_handler.insert == ResizeOption::Original {
                let (new_index, new_slot) = self.table[i].get_new_slot(i, self.remainder, self.size);
                new_table[new_index] = new_slot; 
            } else if resize_handler.insert == ResizeOption::Other {
                let (new_index, new_slot) = other.table[j].get_new_slot(j, other.remainder, other.size);
                new_table[new_index] = new_slot; 
            }

            match resize_handler.index_up {
                ResizeOption::Original => i += 1,
                ResizeOption::Other => j += 1,
                ResizeOption::Both => { i += 1; j += 1; }
                ResizeOption::None => continue
            }
        }
        
        self.size *= 2;
        self.remainder -= 1;
        self.table = new_table;

        Ok(())
    }

    /// Returns if the element exists, by using custom fingerprint
    pub fn lookup(&mut self, fingerprint: u64) -> bool {
        self.get_index(fingerprint).is_some()
    }

    pub fn delete(&mut self, fingerprint: u64)  {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint).unwrap_or_default();

        if quotient == usize::default() && remainder == u64::default() { return;}

        if let Some(bucket) = self.table.get(quotient) {
            if !bucket.get_metadata(MetadataType::BucketOccupied) { return;}
        } else { return; }

        let mut b = self.get_start_of_the_cluster(quotient);
        let mut s = b;
        while b != quotient {
            s = self.index_up(s);
            s = self.get_lowest_of_run(s);
            b = self.index_up(b);

            b = self.skip_empty_slots(b);
        }

        // S at the start of the run. 
        // If only its the only element in the run, then we clear bucket_occupied bit
        let mut clear_bucket_occupied = true;
        let mut clear_head = false;
        let mut head_of_run_index: usize = 0;
        while let Some(bucket) = self.table.get(s) {
            if bucket.remainder != remainder {
                if !clear_head { head_of_run_index = s; }
                clear_head = true;
                s = self.index_up(s);
                clear_bucket_occupied = false;
                if !self.table[s].get_metadata(MetadataType::RunContinued) { return; }
            } else {
                if self.table[s + 1].get_metadata(MetadataType::RunContinued) { clear_head = false; clear_bucket_occupied = false; }
                break;
            }
        }  
        
        if clear_head { self.table[head_of_run_index].clear_metadata(MetadataType::BucketOccupied) }

        self.table[s].set_metadata(MetadataType::Tombstone);
        if clear_bucket_occupied { self.table[s].clear_metadata(MetadataType::BucketOccupied); }
    }

     /// Inserts the element by using custom fingerprint and returns the index
     pub fn insert(&mut self, fingerprint: u64) -> Result<usize> {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint)?;
        let is_quotient_occupied_before = self.table[quotient].is_occupied(); 
        // mark the appropriate as occupied
        if let Some(bucket) = self.table.get_mut(quotient) {
            bucket.set_metadata(MetadataType::BucketOccupied);
            // if selected is empty, we can set and return
            if bucket.is_empty() {
                bucket.clear_metadata(MetadataType::Tombstone);
                bucket.set_remainder(remainder);               
                return Ok(quotient);
            }

            // Going to start of the cluster. Cluster is one or more runs.
            let mut b = self.get_start_of_the_cluster(quotient);
            let mut s = b;
            // how many slots away our quotient from the anchor(cluster start slot)
            let away_from_anchor = if quotient < s { quotient + 1 + self.size - 1 } else { quotient } - s;
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

            if !is_quotient_occupied_before { 
                loop {
                    if !self.table[s].is_empty() { s = self.index_up(s) } else { break; }
                }
             }
            
            // Find the insert spot
            // s is here at the start of the run, if first of its run, first empty slot
            let is_part_of_existing_run = !self.table[s].is_empty();
            while let Some(bucket) = self.table.get(s) {
                if !bucket.is_empty() && remainder > bucket.remainder { s  = self.index_up(s) }
                else {  break; }
            }
            let mut insert_index = s;
            let mut extra_shift = false;
            // 1. find last slot of our quotient's run
            // 2. if we're first of our run && insert_index - 1. result != away_from_anchor 
            // then extra_shift = true and insert_index = last_run - away_from_anchor
            if !is_quotient_occupied_before {
                let mut last_run = self.index_up(quotient);
                while !self.table[last_run].is_run_continued() {
                    last_run = self.index_up(last_run);
                }
                let idx = if last_run > insert_index { insert_index + self.size } else { insert_index };
                if idx - last_run != away_from_anchor {
                    extra_shift = true;
                    for _ in 0..away_from_anchor { last_run = self.index_up(last_run); }
                    insert_index = last_run;
                }
            }
            //  If it came to here, the quotient's place must be full. So it has to be shifted.
            let mut new_slot = Slot::new_with_remainder(remainder);
            if quotient != insert_index { new_slot.set_metadata(MetadataType::IsShifted) };
            if is_part_of_existing_run { new_slot.set_metadata(MetadataType::RunContinued); }
            // shift other ones
            // while we are shifting buckets, is_shifted should be updated as 1
            // however we shouldn't shift bucket_occupied bits
            let mut tmp_bucket = Slot::default();
            while let Some(bucket) = self.table.get_mut(s) {
                if bucket.is_empty() { break; }
                if tmp_bucket.get_metadata(MetadataType::BucketOccupied) { tmp_bucket.set_metadata(MetadataType::BucketOccupied); }
                tmp_bucket = std::mem::replace(bucket, tmp_bucket);
                tmp_bucket.set_metadata(MetadataType::IsShifted);

                // if new slot is part of run, and pushing old slot, old slot is also runcontinued
                if is_part_of_existing_run { 
                    if tmp_bucket.is_run_start() { new_slot.clear_metadata(MetadataType::RunContinued); }
                    tmp_bucket.set_metadata(MetadataType::RunContinued);
                }
                s = self.index_up(s);
                if self.table[s].is_empty() {
                    self.table[s] = tmp_bucket;
                    break;
                }
            }
            
            if extra_shift {
                let mut tmp_bucket = Slot::default();
                let mut shift_index = insert_index;
                while let Some(bucket) = self.table.get_mut(shift_index) {
                    if bucket.is_empty() { break; }
                    tmp_bucket = std::mem::replace(bucket, tmp_bucket);
                    tmp_bucket.set_metadata(MetadataType::IsShifted);
                    shift_index = self.index_up(shift_index);
                    if self.table[shift_index].is_empty() {
                        self.table[shift_index] = tmp_bucket;
                        break;
                    }
                }
            }

            // here shifting is done. now we have to insert our new bucket using insert_index
            //if remove_old_run_head { new_slot.clear_metadata(MetadataType::RunContinued); }
            self.table[insert_index] = new_slot;
            return Ok(insert_index)

        } 

        Err(anyhow::Error::new(QuotientFilterError::InvalidQuotientAccess(quotient)))
    }

    pub fn get_index(&self, fingerprint: u64) -> Option<usize> {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint).unwrap_or_default();
        if quotient == usize::default() && remainder == u64::default() { return None; }

        // The buckets are quotient-indexed. Remember, we have number of 2^quotient buckets.
        if let Some(bucket) = self.table.get(quotient) {
            if !bucket.get_metadata(MetadataType::BucketOccupied) { return None; }
        } else { return None; }

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
                if !self.table[s].get_metadata(MetadataType::RunContinued) { return None; }
            } else {
                break;
            }
        }  
        Some(s)
    }

    /// Gets the fingerprint(hashed value), returns quotient and remainder
    fn fingerprint_destruction(&self, fingerprint: u64) -> Result<(usize, u64)> {
        let quotient = fingerprint / u64::pow(2, self.remainder as u32);
        let remainder = fingerprint % u64::pow(2, self.remainder as u32);       
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
        if old_index == 0 { return self.size - 1; }
        old_index - 1
    }

    #[inline(always)]
    fn idx_down(size: usize, old_index: usize) -> usize {
        if old_index == 0 { return size - 1; }
        old_index - 1
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_one() {
        let mut filter = QuotientFilter::new(2).unwrap();
        let idx = filter.insert_value(&1_u8.to_be_bytes()).unwrap();  // 1_u8's quotient is 2
        assert_eq!(idx, 2);
        assert!(filter.table[2].is_run_start());
        assert!(filter.table[2].is_cluster_start());
    }

    #[test]
    fn insert_two_same_quotient() {
        let mut filter = QuotientFilter::new(2).unwrap();
        let idx1 = filter.insert_value(&1_u8.to_be_bytes()).unwrap();  // 1_u8's quotient is 2
        let idx2 = filter.insert_value(&2_u8.to_be_bytes()).unwrap();  // 2_u8's quotient is 2
        assert_eq!(idx1, 2);
        assert_eq!(idx2, 3);
        assert!(!filter.table[3].is_occupied()); 
        assert!(filter.table[3].is_run_continued());
        assert!(filter.table[3].is_shifted());
    }

    #[test]
    fn insert_second_run_on_different_quotient() {
        let mut filter = QuotientFilter::new(2).unwrap();
        let idx1 = filter.insert_value(&1_u8.to_be_bytes()).unwrap();  // 1_u8's quotient is 2
        let idx2 = filter.insert_value(&2_u8.to_be_bytes()).unwrap();  // 2_u8's quotient is 2
        let idx3 = filter.insert_value(&567889965_u64.to_be_bytes()).unwrap(); // quotient is 3
        assert_eq!(idx1, 2);
        assert_eq!(idx2, 3);
        assert_eq!(idx3, 0);
        assert!(!filter.table[0].is_occupied()); 
        assert!(!filter.table[0].is_run_continued());
        assert!(filter.table[0].is_shifted());
    }

    // This was an edge case. The whole extra shift is added for this case.
    #[test]
    fn insert_multiple_runs_different_quotients_sequentially() {
        let mut filter = QuotientFilter::new(3).unwrap();
        let idx1 = filter.insert_value(&1_u8.to_be_bytes()).unwrap(); // 5
        let idx2 = filter.insert_value(&2_u8.to_be_bytes()).unwrap(); // 5
        let idx3 = filter.insert_value(&3_u8.to_be_bytes()).unwrap(); // 5
        let idx4 = filter.insert_value(&75324433_u32.to_be_bytes()).unwrap(); // 7
        let idx5 = filter.insert_value(&75324434_u32.to_be_bytes()).unwrap(); // 7
        let idx6 = filter.insert_value(&567889965_u64.to_be_bytes()).unwrap(); // 6

        assert_eq!(idx1, 5);
        assert_eq!(idx2, 6);
        assert_eq!(idx3, 6); // has a smaller remainder
        assert_eq!(idx4, 0);
        assert_eq!(idx5, 0); // has a smaller remainder
        assert_eq!(idx6, 0); // after extra shift, becomes the 0 cause it's quotient is closer to anchor

        assert!(!filter.table[0].is_occupied());
        assert!(!filter.table[0].is_run_continued());
        assert!(filter.table[0].is_shifted()); // 0 0 1

        assert!(!filter.table[1].is_occupied());
        assert!(!filter.table[1].is_run_continued());
        assert!(filter.table[1].is_shifted()); // 0 0 1

        assert!(!filter.table[2].is_occupied());
        assert!(filter.table[2].is_run_continued());
        assert!(filter.table[2].is_shifted()); // 0 1 1

        assert!(filter.table[5].is_occupied());
        assert!(!filter.table[5].is_run_continued());
        assert!(!filter.table[5].is_shifted()); // 1 0 0

        assert!(filter.table[6].is_occupied());
        assert!(filter.table[6].is_run_continued());
        assert!(filter.table[6].is_shifted()); // 1 1 1

        assert!(filter.table[7].is_occupied());
        assert!(filter.table[7].is_run_continued());
        assert!(filter.table[7].is_shifted()); // 1 1 1
    }

    #[test]
    fn insert_and_read_one_success() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        let result = filter.read_value(&1_u8.to_be_bytes());

        assert!(result);
    }

    #[test]
    fn insert_and_read_multiple_success() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        _ = filter.insert_value(&2_u8.to_be_bytes());
        _ = filter.insert_value(&3_u8.to_be_bytes());
        let result = filter.read_value(&2_u8.to_be_bytes());

        assert!(result);
    }

    #[test]
    fn insert_and_read_one_failure() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        let result = filter.read_value(&2_u8.to_be_bytes());

        assert!(!result);
    }

    #[test]
    fn insert_and_read_multiple_failure() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        _ = filter.insert_value(&2_u8.to_be_bytes());
        _ = filter.insert_value(&3_u8.to_be_bytes());
        let result = filter.read_value(&4_u8.to_be_bytes());
        
        assert!(!result);
    }

    #[test]
    fn delete_read_one_success() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        filter.delete_value(&1_u8.to_be_bytes());
        let result = filter.read_value(&1_u8.to_be_bytes());

        assert!(!result);
    }

    #[test]
    fn delete_read_multiple_success() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        _ = filter.insert_value(&2_u8.to_be_bytes());
        _ = filter.insert_value(&3_u8.to_be_bytes());
        _ = filter.insert_value(&4_u8.to_be_bytes());
        filter.delete_value(&2_u8.to_be_bytes());
        let result = filter.read_value(&2_u8.to_be_bytes());

        assert!(!result);
    }

    #[test]
    fn delete_read_multiple_value_multiple_success() {
        let mut filter = QuotientFilter::new(10).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        _ = filter.insert_value(&2_u8.to_be_bytes());
        _ = filter.insert_value(&3_u8.to_be_bytes());
        _ = filter.insert_value(&4_u8.to_be_bytes());
        _ = filter.insert_value(&5_u8.to_be_bytes());
        _ = filter.insert_value(&6_u8.to_be_bytes());
        filter.delete_value(&2_u8.to_be_bytes());
        filter.delete_value(&3_u8.to_be_bytes());
        filter.delete_value(&6_u8.to_be_bytes());
        let result1 = filter.read_value(&2_u8.to_be_bytes());
        let result2 = filter.read_value(&3_u8.to_be_bytes());
        let result3 = filter.read_value(&6_u8.to_be_bytes());

        assert!(!result1);
        assert!(!result2);
        assert!(!result3);
    }
}