# Quotient Filter

A quotient filter is a space-efficient probabilistic data structure used for set membership queries. It works by dividing the hash values of the items in the set into "buckets" based on their quotient when divided by the filter's size. This allows the filter to store a large number of items using relatively little memory, while still providing a fast way to check whether a given item is in the set. Quotient filters are often used in applications where memory is a limiting factor, such as in-memory databases and real-time streaming systems.

The implemantation is based on the book named [Algorithms and Data Structures for Massive Datasets](https://www.manning.com/books/algorithms-and-data-structures-for-massive-datasets).  

## Usage

To use this crate, simply add the following string to your `Cargo.toml`:
```
quotient-filter = "0.2.3"
```

```rust
    // there is QuotientFilter32 and QuotientFilter16 as well
    // let mut filter = QuotientFilter16::new(5).unwrap();
    // let mut filter = QuotientFilter32::new(5).unwrap();
    // The input (here 5) means the table size will be 2^5 and 5 bits will be used for indexing.
    let mut filter = QuotientFilter::new(5).unwrap();
    // if method names end with 'value', it uses fnv1a as default
    let idx = filter.insert_value(&1_u8.to_be_bytes()).unwrap(); // returns Result<location of insert>
    // if you want to use something else than fnv1a
    let your_hash_result = your_hash_function(&1_u8.to_be_bytes());
    let idx2 = filter.insert(your_hash_result);
```

Supports insertion, deletion, lookup, merging and resizing.

# Implementation

The quotient filter is essentially a wrapper around vector of remainder(u64) and a metadata(u8). While initializing we provide the size. However, it's able to resize its size and merge with others.

A lot of bitwise operations happens under the hood. For instance, you initialize QuotientFilter with the quotient size 4. It uses left-most 4 bits for indexing(not saved anywhere), and the rest 60 bits are being saved to the table with the metadata. Using metadata you're able to do other operations, you can even bring back original u64 fingerprint, even though 4 bits weren't saved. Resize and merging work through by bit stealing. We steal one bit from remainder, so quotient size becomes 5 bit, remainder size 59 bit, and the table size 2^5 which is double of previous.

