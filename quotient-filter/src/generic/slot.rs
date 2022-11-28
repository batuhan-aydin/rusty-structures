use num_traits::{Unsigned, Zero, PrimInt, One};
use anyhow::Result;

use crate::QuotientFilterError;

pub(crate) enum MetadataType { 
    Tombstone,
    BucketOccupied,
    RunContinued,
    IsShifted
}

type Metadata = u8;

pub(super) fn is_run_continued(metadata: Metadata) -> bool {
    (metadata >> 1) & Metadata::one() == Metadata::one()
}

pub(super) fn is_shifted(metadata: Metadata) -> bool {
    metadata & Metadata::one() == Metadata::one()
}

pub(super) fn is_occupied(metadata: Metadata) -> bool {
    metadata >> 2 == Metadata::one()
}

pub(super) fn is_tombstone(metadata: Metadata) -> bool {
    (metadata >> 3) == Metadata::one()
}

pub(super) fn is_empty<T>(remainder: T, metadata: Metadata) -> bool 
where T : Unsigned + Zero + One + PrimInt
{
    remainder.is_zero() || super::slot::is_tombstone(metadata)
}

pub(super) fn is_run_start(metadata: Metadata) -> bool {
    !super::slot::is_run_continued(metadata) && 
    (super::slot::is_occupied(metadata)|| super::slot::is_shifted(metadata))
}

pub(super) fn is_cluster_start(metadata: Metadata) -> bool {
    super::slot::is_occupied(metadata)
    && !super::slot::is_run_continued(metadata)
    && !super::slot::is_shifted(metadata)
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Bucket<T> where T : Unsigned + Zero + One + PrimInt + TryFrom<usize> {
    remainder: T,
    metadata: Metadata
}

impl<T> Bucket<T> where T : Unsigned + Zero + One + PrimInt + TryFrom<usize> {    
    pub(super) fn new() -> Self {
        Self { remainder: T::zero(), metadata: u8::zero() }
    }

    pub(super) fn new_with_remainder(remainder: T) -> Self {
        Self { remainder, metadata: Metadata::zero() }
    }

    pub(super) fn reconstruct_fingerprint(&self, quotient: usize, remainder_size: u8) -> Result<T> {
        let quotient = T::try_from(quotient).map_err(|_| anyhow::Error::new(QuotientFilterError::ConvertingError))?;
        let new_value = quotient;
        let bit_mask = quotient << remainder_size.into();
        let result = (self.remainder & !(bit_mask)) | (new_value << remainder_size.into());
        Ok(result)
    }

    pub(super) fn set_remainder(&mut self, remainder: T) {
        self.remainder = remainder;
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

    pub(super) fn get_metadata(&self) -> Metadata {
        return self.metadata
    }

    pub(super) fn get_remainder(&self) -> T {
        return self.remainder
    }

}
