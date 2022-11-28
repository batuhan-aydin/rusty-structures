# Quotient Filter

An implemantation of quotient filter. Based on the book named [Algorithms and Data Structures for Massive Datasets](https://www.manning.com/books/algorithms-and-data-structures-for-massive-datasets).   

## Usage

To use this crate, simply add the following string to your `Cargo.toml`:
```
quotient-filter = "0.2.0"
```

```rust
    let mut filter = QuotientFilter::new(5).unwrap();
    // if method names end with 'value', it uses fnv1a as default
    let idx = filter.insert_value(&1_u8.to_be_bytes()).unwrap(); // returns Result<location of insert>
    // if you want to use something else than fnv1a
    let your_hash_result = your_hash_function(&1_u8.to_be_bytes());
    let idx2 = filter.insert(your_hash_result);

    // The generic one doesn't have default hashes. 
    let mut filter_generic = QuotientFilter::<u64>::new(2).unwrap();
    let idx3 = filter.insert(your_u64_hash_result).unwrap();
```

Supports insertion, deletion, lookup, merging and resizing.

The generic version exists under the generic module. It was first implemented for u64, then mostly copy pasted u32 version under extra module. To avoid of copy-paste, the generic version is created but decided not to delete old ones. It supports u64, u32, u16 and u8.

