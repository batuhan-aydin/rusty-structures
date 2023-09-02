# rusty-structures
An experimental data structures and algorithms library. The implementations are usually reading from books/blogs/papers and rewriting them in Rust. The credits can be find below.

## Existing structures

### Count-Min-Sketch

A data structure for frequency analysis.

![Count-Min-Sketch](count-min-sketch/count-min-sketch.png)

Credits: [1](https://github.com/mlarocca/AlgorithmsAndDataStructuresInAction)


### Priority Queue

As the name implies, to order elements based on their priority. Binary heap also would work, but this one is based on Vec and some calculations, so it would give you a better performance.

Credits: [1](https://github.com/mlarocca/AlgorithmsAndDataStructuresInAction)

### Bloom Filter

A hash based, probabilistic data structure. It's based on bit array, so uses very little memory. Best use time is when you are doing look operations, and a lot of elements you're looking for actually doesn't exist. Sometimes, it may give you false-positives like returning it exists, but actually it isn't so you need to have something non-probabilistic behind of it to make sure. 

Credits: [1](https://github.com/mlarocca/AlgorithmsAndDataStructuresInAction), [2](https://www.manning.com/books/algorithms-and-data-structures-for-massive-datasets)

### Quotient Filter

Another hash based data structure that has features between hash-map and bloom-filter. Unlike bloom-filter, it can grow, unlike hash-map it cannot give back the elements. Also it allocates a memory more than bloom-filter, less than hash-map. 

Credits: [1](https://www.manning.com/books/algorithms-and-data-structures-for-massive-datasets), [2](https://www.gakhov.com/articles/quotient-filters.html)