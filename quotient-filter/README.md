# Quotient Filter

An implemantation of quotient filter. Based on the book named [Algorithms and Data Structures for Massive Datasets](https://www.manning.com/books/algorithms-and-data-structures-for-massive-datasets).   

## Usage

To use this crate, simply add the following string to your `Cargo.toml`:
```
quotient-filter = "0.2.2"
```

```rust
    // there is QuotientFilter32 and QuotientFilter16 as well
    // The input (here 5) means the table size will be 2^5 and 5 bits will be used for indexing.
    let mut filter = QuotientFilter::new(5).unwrap();
    // if method names end with 'value', it uses fnv1a as default
    let idx = filter.insert_value(&1_u8.to_be_bytes()).unwrap(); // returns Result<location of insert>
    // if you want to use something else than fnv1a
    let your_hash_result = your_hash_function(&1_u8.to_be_bytes());
    let idx2 = filter.insert(your_hash_result);
```

Supports insertion, deletion, lookup, merging and resizing.

Under extra module, u32 and u16 versions exists. The quotient filter is essentially a wrapper around vector of remainder(u64, u32 or u16) and a metadata(u8). While initializing we provide the size. However, it's able to resize its size and merge with others.

A lot of bitwise operations happens under the hood. For instance, you initialize QuotientFilter32 with the quotient size 4. It uses left-most 4 bits for indexing(not saved anywhere), and the rest 28 bits are being saved to the table with the metadata. Using metadata you're able to do other operations. You can even bring back original u32 fingerprint, even though 4 bits weren't saved. Resize and merging works through bit stealing. We take one bit from remainder, so quotient size becomes 5 bit, remainder 27 bit, and the table size 2^5.

