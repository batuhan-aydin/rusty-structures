pub mod quotient_filter;
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
