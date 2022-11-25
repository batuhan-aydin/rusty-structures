use super::MetadataType;

/// Slot keeps remainder(what's left from quotient), and 4 bits metadata.
/// Metadata bits are, Tombstone, bucket_occupied, run_continued and is_shifted
/// However, we can't use anything smaller than a byte, so we'll use a byte and waste 4 bits.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Slot {
    pub(super) remainder: u64,
    metadata: u8
}

impl Slot {
    pub(super) fn new() -> Self {
        Self { remainder: 0, metadata: 0}
    }

    pub(super) fn new_with_remainder(remainder: u64) -> Self {
        Self { remainder, metadata: 0}
    }

    pub(super) fn is_empty(&self) -> bool {
        self.remainder == 0 || self.get_metadata(MetadataType::Tombstone)
    }

    pub(super) fn reconstruct_fingerprint(&self, quotient: usize, remainder_size: u8) -> u64 {
        let quotient = quotient as u64;
        let new_value = quotient;
        let bit_mask = quotient << remainder_size;
        (self.remainder &  !(bit_mask)) | (new_value << remainder_size)
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

    /// Get metadata info. 0 is false, 1 is true.
    // right-most 4 bits are being used. The rest 3 bits are unused.
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
