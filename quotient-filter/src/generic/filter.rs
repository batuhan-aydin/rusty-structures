use std::collections::BTreeMap;

use crate::QuotientFilterError;
use anyhow::Result;
use num_traits::{Unsigned, Zero, PrimInt, One};
use super::slot::{Bucket, MetadataType};

pub struct QuotientFilter<T> where T: Unsigned + Zero + One + PrimInt + TryFrom<usize> {
    count: usize,
    remainder_size: u8,
    table_size: usize,
    table: Vec<Bucket<T>>  
}


impl<T> QuotientFilter<T> where T: Unsigned + Zero + One + PrimInt + TryFrom<usize> + Default, usize: TryFrom<T>{
    pub fn new(quotient_size: u8) -> Result<Self> {
        let hash_size = std::mem::size_of::<T>();
        match hash_size {
            1 => if quotient_size > 7 {return Err(anyhow::Error::new(QuotientFilterError::InvalidQuotientSize))},
            2 => if quotient_size > 15 {return Err(anyhow::Error::new(QuotientFilterError::InvalidQuotientSize))},
            4 => if quotient_size > 31 {return Err(anyhow::Error::new(QuotientFilterError::InvalidQuotientSize))},
            8 => if quotient_size > 61 {return Err(anyhow::Error::new(QuotientFilterError::InvalidQuotientSize))} 
            _ => return Err(anyhow::Error::new(QuotientFilterError::InvalidQuotientSize))
        }
        let table_size = usize::pow(2, quotient_size as u32);
        let remainder_size = 64 - quotient_size;
        
        Ok(Self {
            count: 0,
            remainder_size,
            table_size,
            table: vec![Bucket::new(); table_size]
        })
    }

    /// How much space are we spending
    pub fn space(&self) -> T {
        T::pow(<T as TryFrom<usize>>::try_from(2_usize).map_err(|_| anyhow::Error::new(QuotientFilterError::ConvertingError)).unwrap(), 
        64 - self.remainder_size as u32) * <T as TryFrom<usize>>::try_from(self.remainder_size as usize + 8).map_err(|_| anyhow::Error::new(QuotientFilterError::ConvertingError)).unwrap()
    }

    /// Doubles the size of the table
    // We have to get its fingerprint back then insert again
    pub fn resize(&mut self) -> anyhow::Result<()>{
        // do cluster by cluster. 
        let mut is_first = false;
        let mut first_anchor = usize::default();
        let mut index: usize = 0;
        let mut fingerprints: Vec<T> = Vec::with_capacity(self.count as usize);
        while let Some(anchor_idx) = self.get_next_anchor(index) {
            if anchor_idx == first_anchor { break; }
            if !is_first { first_anchor = anchor_idx; is_first = true; }
            let mut quotient_cache = anchor_idx;
            let mut slot_idx = anchor_idx;
            // an anchor's fingerprint is just its quotient and its remainder side by side
            let mut fingerprint = self.table[anchor_idx].reconstruct_fingerprint(anchor_idx, self.remainder_size)?;
        
            fingerprints.push(fingerprint);
            slot_idx = self.index_up(slot_idx);
            while !self.table[slot_idx].is_empty() {
                while self.table[slot_idx].is_run_continued() {
                    fingerprint = self.table[slot_idx].reconstruct_fingerprint(quotient_cache, self.remainder_size)?;
                    fingerprints.push(fingerprint);
                    slot_idx = self.index_up(slot_idx);
                }
                if !self.table[slot_idx].is_empty() {
                    quotient_cache = self.get_next_occupied(quotient_cache).ok_or(anyhow::Error::new(QuotientFilterError::NotAbleToFindOccupied))?;
                    if self.table[slot_idx].is_run_start() {
                        fingerprint = self.table[slot_idx].reconstruct_fingerprint(quotient_cache, self.remainder_size)?;
                        fingerprints.push(fingerprint);
                        slot_idx = self.index_up(slot_idx);
                      }
                } else {
                    break;
                }
            }
            index = anchor_idx;
        } 

        let mut old_table = std::mem::replace(&mut self.table, vec![Bucket::new(); self.table_size * 2]);
        self.table_size *= 2;
        self.remainder_size -= 1;
        self.count = 0;

        for fingerprint in fingerprints {
            // If any error happens during insertion, we're taking back everything
            if let Err(e) = self.insert(fingerprint) {
                std::mem::swap(&mut self.table, &mut old_table);
                self.table_size /= 2;
                self.remainder_size += 1;
                return Err(e);
            }
        }
        Ok(())
    }

    /// Merges a second filter into original one and doubles its original size. They have to have the same size.
    pub fn merge(&mut self, other: &QuotientFilter<T>) -> Result<()> {
        if self.table_size != other.table_size { return Err(anyhow::Error::new(QuotientFilterError::NotEqualSize)); }

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
        let mut old_table = std::mem::replace(&mut self.table, vec![Bucket::new(); self.table_size * 2]);
        self.table_size *= 2;
        self.remainder_size -= 1;
        self.count = 0;

        for (_, fingerprints) in map_1 {
            for fingerprint in fingerprints {
                if let Err(e) = self.insert(fingerprint) {
                    std::mem::swap(&mut self.table, &mut old_table);
                    self.table_size /= 2;
                    self.remainder_size += 1;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, fingerprint: T)  {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint).unwrap_or_default();

        if quotient == usize::default() && remainder == T::default() { return;}

        if let Some(bucket) = self.table.get(quotient) {
            if !bucket.is_occupied() { return;}
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
            if bucket.get_remainder() != remainder {
                if !clear_head { head_of_run_index = s; }
                clear_head = true;
                s = self.index_up(s);
                clear_bucket_occupied = false;
                if !self.table[s].is_run_continued() { return; }
            } else {
                if self.table[self.index_up(s)].is_run_continued(){ clear_head = false; clear_bucket_occupied = false; }
                break;
            }
        }  
        
        if clear_head { self.table[head_of_run_index].clear_metadata(MetadataType::BucketOccupied) }

        self.table[s].set_metadata(MetadataType::Tombstone);
        self.count -= 1;
        if clear_bucket_occupied { self.table[s].clear_metadata(MetadataType::BucketOccupied); }
    }


    /// Inserts the element by using custom fingerprint and returns the index
    pub fn insert(&mut self, fingerprint: T) -> Result<usize> {
    //if self.table_size - self.count as usize - 1 == 0 { self.resize()?; }
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint)?;
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
            let away_from_anchor = if quotient < s { quotient + 1 + self.table_size - 1 } else { quotient } - s;
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
                    if !self.table[s].is_empty() { 
                        s = self.index_up(s) 
                    } else { break; }
                }
            }
                
            // Find the insert spot
            // s is here at the start of the run, if first of its run, first empty slot
            let is_part_of_existing_run = !self.table[s].is_empty();
            while let Some(bucket) = self.table.get(s) {
                if !bucket.is_empty() 
                && remainder > bucket.get_remainder() { s  = self.index_up(s) }
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
                let idx = if last_run > insert_index { insert_index + self.table_size } else { insert_index };
                if idx - last_run != away_from_anchor {
                    extra_shift = true;
                    for _ in 0..away_from_anchor { last_run = self.index_up(last_run); }
                    insert_index = last_run;
                }
            }
            //  If it came to here, the quotient's place must be full. So it has to be shifted.
            let mut new_slot = Bucket::new_with_remainder(remainder);
            if quotient != insert_index { new_slot.set_metadata(MetadataType::IsShifted) };
            if is_part_of_existing_run { new_slot.set_metadata(MetadataType::RunContinued); }
            // shift other ones
            // while we are shifting buckets, is_shifted should be updated as 1
            // however we shouldn't shift bucket_occupied bits
            let mut tmp_bucket = Bucket::default();
            while let Some(bucket) = self.table.get_mut(s) {
                if bucket.is_empty() { break; }
                if tmp_bucket.is_occupied() { tmp_bucket.set_metadata(MetadataType::BucketOccupied); }
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
                let mut tmp_bucket = Bucket::default();
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


    pub fn get_index(&self, fingerprint: T) -> Option<usize> {
        let (quotient, remainder) = self.fingerprint_destruction(fingerprint).unwrap_or_default();
        if quotient == usize::default() && remainder == T::default() { return None; }

        // The buckets are quotient-indexed. Remember, we have number of 2^quotient buckets.
        if let Some(bucket) = self.table.get(quotient) {
            if !bucket.is_occupied() { return None; }
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
            if bucket.get_remainder() != remainder {
                s = self.index_up(s);
                if !self.table[s].is_run_continued(){ return None; }
            } else {
                break;
            }
        }  
        Some(s)
    }


    /// Collects map of quotient and collection of fingerprints
    fn collect_fingerprint_map(&self) -> Result<BTreeMap<usize, Vec<T>>> {
        let mut map: BTreeMap<usize, Vec<T>> = BTreeMap::new();
        let mut is_first = false;
        let mut first_anchor = usize::default();
        let mut index: usize = 0;

        let mut insertion = |index: usize, fingerprint: T| {
            if let Some(value) = map.get_mut(&index) { value.push(fingerprint); } else { map.insert(index, vec![fingerprint]); }
        };

        while let Some(anchor_idx) = self.get_next_anchor(index) {
            if anchor_idx == first_anchor { break; }
            if !is_first { first_anchor = anchor_idx; is_first = true; }
            let mut quotient_cache = anchor_idx;
            let mut slot_idx = anchor_idx;
            // an anchor's fingerprint is just its quotient and its remainder side by side
            let mut fingerprint = self.table[anchor_idx].reconstruct_fingerprint(anchor_idx, self.remainder_size)?;
            insertion(quotient_cache, fingerprint);
            slot_idx = self.index_up(slot_idx);
            while !self.table[slot_idx].is_empty() {
                while self.table[slot_idx].is_run_continued() {
                    fingerprint = self.table[slot_idx].reconstruct_fingerprint(quotient_cache, self.remainder_size)?;
                    insertion(quotient_cache, fingerprint);
                    slot_idx = self.index_up(slot_idx);
                }
                if !self.table[slot_idx].is_empty() {
                    quotient_cache = self.get_next_occupied(quotient_cache).ok_or(anyhow::Error::new(QuotientFilterError::NotAbleToFindOccupied))?;
                    if self.table[slot_idx].is_run_start() {
                        fingerprint = self.table[slot_idx].reconstruct_fingerprint(quotient_cache, self.remainder_size)?;
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


    /// Gets the fingerprint(hashed value), returns quotient and remainder
    fn fingerprint_destruction(&self, fingerprint: T) -> Result<(usize, T)> {
        let quotient = fingerprint / T::pow(<T as TryFrom<usize>>::try_from(2_usize).map_err(|_| anyhow::Error::new(QuotientFilterError::ConvertingError)).unwrap(), self.remainder_size as u32);
        let remainder = fingerprint % T::pow(<T as TryFrom<usize>>::try_from(2_usize).map_err(|_| anyhow::Error::new(QuotientFilterError::ConvertingError)).unwrap(), self.remainder_size as u32);       
        let quotient_usize = usize::try_from(quotient).map_err(|_| anyhow::Error::new(QuotientFilterError::ConvertingError)).unwrap();
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
            if slot.is_run_continued() { index = self.index_up(index) }
            else { break; }
        }
        index
    }

    fn skip_empty_slots(&self, start_index: usize) -> usize {
        let mut index = start_index;
        while let Some(bucket) = self.table.get(index) {
            if !bucket.is_occupied() { index = self.index_up(index) }
            else { break; }
        }
        index
    }

    fn get_next_anchor(&self, index: usize) -> Option<usize> {
        for i in index..self.table_size {
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

    #[inline(always)]
    fn index_up(&self, old_index: usize) -> usize {
        (old_index + 1) % (self.table_size)
    }

    #[inline(always)]
    fn index_down(&self, old_index: usize) -> usize {
        if old_index == 0 { return self.table_size - 1; }
        old_index - 1
    }
}