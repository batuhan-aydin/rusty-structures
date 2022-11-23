use super::MetadataType;

/// Slot keeps remainder(what's left from quotient), and 3 bits metadata.
/// Metadata bits are, bucket_occupied, run_continued and is_shifted
/// However, we can't use anything smaller than a byte, so we'll use a byte and waste 5 bits.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Slot {
    pub(super) remainder: u64,
    metadata: u8
}

impl Slot {
    pub(super) fn new() -> Self {
        Self { remainder: 0, metadata: 0}
    }

    pub(super) fn new_with_all(remainder: u64, metadata: u8) -> Self {
        Self { remainder, metadata }
    }

    pub(super) fn new_with_remainder(remainder: u64) -> Self {
        Self { remainder, metadata: 0}
    }

    pub(super) fn is_empty(&self) -> bool {
        self.remainder == 0 || self.get_metadata(MetadataType::Tombstone)
    }

    pub(super) fn reconstruct_fingerprint_64(&self, quotient: usize, remainder_size: u8) -> u64 {
        let quotient = quotient as u64;
        let new_value = quotient;
        let bit_mask = quotient << remainder_size;
        (self.remainder &  !(bit_mask)) | (new_value << remainder_size)
    }

    pub(super) fn get_left_most_remainder_bit(&self, remainder_size: u8) -> bool {
        self.remainder >> (remainder_size - 1) == 1
    }

    pub(super) fn get_new_quotient(&self, old_quotient: u32, bit: bool) -> usize {
        (old_quotient << 1 | if bit { 1 } else { 0 }) as usize
    }

    pub(super) fn get_new_remainder(&self, remainder_size: u8) -> u64 {
        self.remainder & !(1 << remainder_size - 1)
    }

    pub(super) fn is_run_start(&self) -> bool {
        !self.get_metadata(MetadataType::RunContinued) && 
        (self.get_metadata(MetadataType::BucketOccupied) || self.get_metadata(MetadataType::IsShifted))
    }

    pub(super) fn is_cluster_start(&self) -> bool {
        self.get_metadata(MetadataType::BucketOccupied) 
        && !self.get_metadata(MetadataType::RunContinued)
        && !self.get_metadata(MetadataType::IsShifted)
    }

    pub(super) fn is_occupied(&self) -> bool {
        self.metadata >> 2 == 1
    }

    pub(super) fn is_run_continued(&self) -> bool {
        (self.metadata >> 1) & 1 == 1
    }

    pub(super) fn is_shifted(&self) -> bool {
        self.metadata & 1 == 1
    }

    pub(super) fn get_new_slot(&self, old_index: usize, remainder_size: u8, size: usize) -> (usize, Slot) {
        let left_remainder_bit = self.get_left_most_remainder_bit(remainder_size);
        let mut new_index = self.get_new_quotient(old_index as u32, left_remainder_bit);
        let new_remainder = self.get_new_remainder(remainder_size);
        let mut new_slot = Slot::new_with_remainder(new_remainder);

        if self.get_metadata(MetadataType::BucketOccupied) {
            new_slot.set_metadata(MetadataType::BucketOccupied);
        }

        if self.get_metadata(MetadataType::RunContinued) { 
            new_slot.set_metadata(MetadataType::RunContinued);
        }

        if self.get_metadata(MetadataType::IsShifted) {
            new_index = if new_index == 0 { size - 1 } else { new_index - 1 };
            new_slot.set_metadata(MetadataType::IsShifted);
        }

        (new_index, new_slot)
    }

    /// Get metadata info. 0 is false, 1 is true.
    // right-most 3 bits are being used. The rest 5 bits are unused.
    // The most right bit is IsShifted. Middle one RunContinued
    pub(super) fn get_metadata(&self, data: MetadataType) -> bool {
        let result = match data {
            MetadataType::Tombstone => self.metadata >> 3,
            MetadataType::BucketOccupied => self.metadata >> 2,
            MetadataType::RunContinued => (self.metadata >> 1) & 1,
            MetadataType::IsShifted => self.metadata & 1
        };

        result == 1
    }

    /// Sets the selected metadata to 1
    pub(super) fn set_metadata(&mut self, data: MetadataType) {
        match data {
            MetadataType::Tombstone => self.metadata |= 1 << 3,
            MetadataType::BucketOccupied => self.metadata |= 1 << 2,
            MetadataType::RunContinued => self.metadata |= 1 << 1,
            MetadataType::IsShifted => self.metadata |= 1
        }
    }

    /// Sets the selected metadata to 0
    pub(super) fn clear_metadata(&mut self, data: MetadataType) {
        match data {
            MetadataType::Tombstone => self.metadata &= !(1 << 3),
            MetadataType::BucketOccupied => self.metadata &= !(1 << 2),
            MetadataType::RunContinued => self.metadata &= !(1 << 1),
            MetadataType::IsShifted => self.metadata &= !1
        }
    }

    pub(super) fn set_remainder(&mut self, remainder: u64) {
        self.remainder = remainder;
    }
}
