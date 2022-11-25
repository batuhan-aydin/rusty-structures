# Priority Queue

An implemantation of priority queue. Based on the book named [Advanced Algorithms and Data Structures](https://www.manning.com/books/advanced-algorithms-and-data-structures).   

## Usage

To use this crate, simply add the following string to your `Cargo.toml`:
```
quotient-filter = "0.1.0"
```

```rust
    // Parameters are capacity and branching factor(default 4).
    // It's a vector of pairs behind the scenes, so defined capacity is a good idea.
    let mut queue = PriorityQueue::new(Some(20), None);
    // The second parameter is the priority
    queue.insert_value("My important task", 1)
    // Get the highest priority value
    let top = queue.top();
    // There is also peek which doesn't take the value out of the queue
    let peek = queue.peek();
    // Check if queue contains
    let is_exists = queue.contains(&"The droid that we were searching for");
    // Remove
    let old_task = queue.remove("Time to go");
    // Update its priority
    queue.update_priority("Go to gym", 1000);
```

