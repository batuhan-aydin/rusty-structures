use std::collections::BTreeMap;

use crate::{QuotientFilterError, MetadataType};

use super::slot::Slot;
use anyhow::{Result, Ok};

pub struct QuotientFilter {
    count: usize,
    remainder: u8,
    size: usize,
    table: Vec<Slot>  
}

impl QuotientFilter {
    /// Creates a new filter.
    /// Quotient size defines the size, ex. quotient_size = 2, size of table is 2^2 = 4
    /// And 32 - 2 = 30 rest of the bits will be used for remainder
    pub fn new(quotient_size: u8) -> Result<Self> {
        if quotient_size > 30 { return Err(anyhow::Error::new(QuotientFilterError::InvalidQuotientSize)); }
        let size = usize::pow(2, quotient_size as u32);
        let remainder = 32 - quotient_size;
        
        Ok(Self {
            count: 0,
            remainder,
            size,
            table: vec![Slot::new(); size]
        })
    }

    /// Inserts byte-value using murmur3 
    pub fn insert_value(&mut self, value: &[u8]) -> Result<usize> {
        let fingerprint =  const_murmur3::murmur3_32(value, 2023);
        self.insert(fingerprint)
    }

    /// Reads byte-value using murmur3
    pub fn lookup_value(&mut self, value: &[u8]) -> bool {
        let fingerprint =  const_murmur3::murmur3_32(value, 2023); 
        self.lookup(fingerprint)
    }

    /// Deleted byte-value using murmur3
    pub fn delete_value(&mut self, value: &[u8]) {
        let fingerprint =  const_murmur3::murmur3_32(value, 2023);
        self.delete(fingerprint);
    }

    /// How much space are we spending
    pub fn space(&self) -> u64 {
        u64::pow(2, 32 - self.remainder as u32) * (self.remainder as u64 + 8)
    }

    /// Doubles the size of the table
    // We have to get its fingerprint back then insert again
    pub fn resize(&mut self) -> anyhow::Result<()>{
        // do cluster by cluster. 
        let mut is_first = false;
        let mut first_anchor = usize::default();
        let mut index: usize = 0;
        let mut fingerprints: Vec<u32> = Vec::with_capacity(self.count as usize);
        while let Some(anchor_idx) = self.get_next_anchor(index) {
            if anchor_idx == first_anchor { break; }
            if !is_first { first_anchor = anchor_idx; is_first = true; }
            let mut quotient_cache = anchor_idx;
            let mut slot_idx = anchor_idx;
            // an anchor's fingerprint is just its quotient and its remainder side by side
            let mut fingerprint = self.table[anchor_idx].reconstruct_fingerprint(anchor_idx, self.remainder);
        
            fingerprints.push(fingerprint);
            slot_idx = self.index_up(slot_idx);
            while !self.table[slot_idx].is_empty() {
                while self.table[slot_idx].is_run_continued() {
                    fingerprint = self.table[slot_idx].reconstruct_fingerprint(quotient_cache, self.remainder);
                    fingerprints.push(fingerprint);
                    slot_idx = self.index_up(slot_idx);
                }
                if !self.table[slot_idx].is_empty() {
                    quotient_cache = self.get_next_occupied(quotient_cache).ok_or(anyhow::Error::new(QuotientFilterError::NotAbleToFindOccupied))?;
                    if self.table[slot_idx].is_run_start() {
                        fingerprint = self.table[slot_idx].reconstruct_fingerprint(quotient_cache, self.remainder);
                        fingerprints.push(fingerprint);
                        slot_idx = self.index_up(slot_idx);
                      }
                } else {
                    break;
                }
            }
            index = anchor_idx;
        } 

        let mut old_table = std::mem::replace(&mut self.table, vec![Slot::new(); self.size * 2]);
        self.size *= 2;
        self.remainder -= 1;
        self.count = 0;

        for fingerprint in fingerprints {
            // If any error happens during insertion, we're taking back everything
            if let Err(e) = self.insert(fingerprint) {
                std::mem::swap(&mut self.table, &mut old_table);
                self.size /= 2;
                self.remainder += 1;
                return Err(e);
            }
        }
        Ok(())
    }

    /// Merges a second filter into original one and doubles its original size. They have to have the same size.
    pub fn merge(&mut self, other: &QuotientFilter) -> Result<()> {
        if self.size != other.size { return Err(anyhow::Error::new(QuotientFilterError::NotEqualSize)); }

        // Collect all quotient and corresponding fingerprints
        let mut map_1 = self.collect_fingerprint_map()?;
        let mut map_2 = other.collect_fingerprint_map()?;
        for (index, fingerprints) in &mut map_1 {
            if let Some(value) = map_2.get_mut(index) {
                fingerprints.append(value);
                fingerprints.sort_unstable();
              }
        }
        for (index, fingerprints) in map_2 {
            if fingerprints.len() > 0 { map_1.insert(index, fingerprints); }
        }

        // Resize
        let mut old_table = std::mem::replace(&mut self.table, vec![Slot::new(); self.size * 2]);
        self.size *= 2;
        self.remainder -= 1;
        self.count = 0;

        for (_, fingerprints) in map_1 {
            for fingerprint in fingerprints {
                if let Err(e) = self.insert(fingerprint) {
                    std::mem::swap(&mut self.table, &mut old_table);
                    self.size /= 2;
                    self.remainder += 1;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Returns if the element exists, by using custom fingerprint
    pub fn lookup(&mut self, fingerprint: u32) -> bool {
        self.get_index(fingerprint).is_some()
    }

    pub fn delete(&mut self, fingerprint: u32)  {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint).unwrap_or_default();

        if quotient == usize::default() && remainder == u32::default() { return;}

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
                if self.table[self.index_up(s)].get_metadata(MetadataType::RunContinued) { clear_head = false; clear_bucket_occupied = false; }
                break;
            }
        }  
        
        if clear_head { self.table[head_of_run_index].clear_metadata(MetadataType::BucketOccupied) }

        self.table[s].set_metadata(MetadataType::Tombstone);
        self.count -= 1;
        if clear_bucket_occupied { self.table[s].clear_metadata(MetadataType::BucketOccupied); }
    }

     /// Inserts the element by using custom fingerprint and returns the index
     pub fn insert(&mut self, fingerprint: u32) -> Result<usize> {
        if self.size - self.count as usize - 1 == 0 { self.resize()?; }
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint)?;
        dbg!(quotient);
        dbg!(remainder);
        let is_quotient_occupied_before = self.table[quotient].is_occupied(); 
        // mark the appropriate as occupied
        if let Some(bucket) = self.table.get_mut(quotient) {
            bucket.set_metadata(MetadataType::BucketOccupied);
            // if selected is empty, we can set and return
            if bucket.is_empty() {
                bucket.clear_metadata(MetadataType::Tombstone);
                bucket.set_remainder(remainder);    
                self.count += 1;           
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
                    // If we are taking place of run start
                    if tmp_bucket.is_run_start() { 
                        if tmp_bucket.is_occupied() { 
                            new_slot.set_metadata(MetadataType::BucketOccupied); 
                            tmp_bucket.clear_metadata(MetadataType::BucketOccupied);
                        }
                        new_slot.clear_metadata(MetadataType::RunContinued); 
                    }
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
            self.table[insert_index] = new_slot;
            self.count += 1;
            return Ok(insert_index)
        } 

        Err(anyhow::Error::new(QuotientFilterError::InvalidQuotientAccess(quotient)))
    }

    pub fn get_index(&self, fingerprint: u32) -> Option<usize> {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint).unwrap_or_default();
        if quotient == usize::default() && remainder == u32::default() { return None; }

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
    fn fingerprint_destruction(&self, fingerprint: u32) -> Result<(usize, u32)> {
        let quotient = fingerprint / u32::pow(2, self.remainder as u32);
        let remainder = fingerprint % u32::pow(2, self.remainder as u32);       
        let quotient_usize = usize::try_from(quotient)?;
        Ok((quotient_usize, remainder))
    }
    
    fn get_start_of_the_cluster(&self, start_index: usize) -> usize {
        let mut index = start_index;
        while let Some(slot) = self.table.get(index) {
            if slot.is_shifted() { index = self.index_down(index); }
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

    fn get_next_anchor(&self, index: usize) -> Option<usize> {
        for i in index..self.size {
            if self.table[i].is_cluster_start() { return Some(i); }
        }
        None
    }

    fn get_next_occupied(&self, cache: usize) -> Option<usize> {
        let mut index = self.index_up(cache);
        while let Some(slot) = self.table.get(index) {
            // if looped and returned back to old cache, it shouldn't happen, error
            if index == cache { return None; }
            // we loop until we find next occupied slot
            else if slot.is_occupied() { return Some(index); }
            else { index = self.index_up(cache); }
        }
        None
    }

    /// Collects map of quotient and collection of fingerprints
    fn collect_fingerprint_map(&self) -> Result<BTreeMap<usize, Vec<u32>>> {
        let mut map: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
        let mut is_first = false;
        let mut first_anchor = usize::default();
        let mut index: usize = 0;

        let mut insertion = |index: usize, fingerprint: u32| {
            if let Some(value) = map.get_mut(&index) { value.push(fingerprint); } else { map.insert(index, vec![fingerprint]); }
        };

        while let Some(anchor_idx) = self.get_next_anchor(index) {
            if anchor_idx == first_anchor { break; }
            if !is_first { first_anchor = anchor_idx; is_first = true; }
            let mut quotient_cache = anchor_idx;
            let mut slot_idx = anchor_idx;
            // an anchor's fingerprint is just its quotient and its remainder side by side
            let mut fingerprint = self.table[anchor_idx].reconstruct_fingerprint(anchor_idx, self.remainder);
            insertion(quotient_cache, fingerprint);
            slot_idx = self.index_up(slot_idx);
            while !self.table[slot_idx].is_empty() {
                while self.table[slot_idx].is_run_continued() {
                    fingerprint = self.table[slot_idx].reconstruct_fingerprint(quotient_cache, self.remainder);
                    insertion(quotient_cache, fingerprint);
                    slot_idx = self.index_up(slot_idx);
                }
                if !self.table[slot_idx].is_empty() {
                    quotient_cache = self.get_next_occupied(quotient_cache).ok_or(anyhow::Error::new(QuotientFilterError::NotAbleToFindOccupied))?;
                    if self.table[slot_idx].is_run_start() {
                        fingerprint = self.table[slot_idx].reconstruct_fingerprint(quotient_cache, self.remainder);
                        insertion(quotient_cache, fingerprint);
                        slot_idx = self.index_up(slot_idx);
                      }
                } else {
                    break;
                }
            }
            index = anchor_idx;
        } 
        for value in map.iter_mut() {
            value.1.sort_unstable();
        }
        Ok(map)
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

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_read_one_success() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        let result = filter.lookup_value(&1_u8.to_be_bytes());

        assert!(result);
    }

    #[test]
    fn insert_and_read_multiple_success() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        _ = filter.insert_value(&2_u8.to_be_bytes());
        _ = filter.insert_value(&3_u8.to_be_bytes());
        let result = filter.lookup_value(&2_u8.to_be_bytes());

        assert!(result);
    }

    #[test]
    fn insert_and_read_one_failure() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        let result = filter.lookup_value(&2_u8.to_be_bytes());

        assert!(!result);
    }

    #[test]
    fn insert_and_read_multiple_failure() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        _ = filter.insert_value(&2_u8.to_be_bytes());
        _ = filter.insert_value(&3_u8.to_be_bytes());
        let result = filter.lookup_value(&4_u8.to_be_bytes());
        
        assert!(!result);
    }

    #[test]
    fn delete_read_one_success() {
        let mut filter = QuotientFilter::new(5).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        filter.delete_value(&1_u8.to_be_bytes());
        let result = filter.lookup_value(&1_u8.to_be_bytes());

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
        let result = filter.lookup_value(&2_u8.to_be_bytes());

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
        let result1 = filter.lookup_value(&2_u8.to_be_bytes());
        let result2 = filter.lookup_value(&3_u8.to_be_bytes());
        let result3 = filter.lookup_value(&6_u8.to_be_bytes());

        assert!(!result1);
        assert!(!result2);
        assert!(!result3);
    }

    #[test]
    fn read_after_resize_one_element() {
        let mut filter = QuotientFilter::new(2).unwrap();
        _ = filter.insert_value(&1_u8.to_be_bytes());
        _ = filter.resize();
        assert!(filter.lookup_value(&1_u8.to_be_bytes()));
    }
    
}