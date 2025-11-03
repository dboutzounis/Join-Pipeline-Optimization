# Join Pipeline Optimization

## First Assignment: Hash Algorithms

### Overview

This assignment focuses on the implementation and comparison of three advanced open-addressing hashing algorithms **Robin Hood**, **Hopscotch**, and **Cuckoo Hashing** commonly used in high-performance data structures and database join operations. The goal of the project is to design, implement, and evaluate these algorithms under a unified interface that allows consistent insertion, search, and rehashing operations while maintaining efficient memory usage and constant-time average complexity. Each algorithm is templated in C++ to support generic key value types and is tested through a unit testing framework based on Catch2. The performance of each algorithm will be compared based on the given SQL queries.

### Robin Hood

### Hopscotch

The Hopscotch Hashing algorithm is a highly efficient hash table scheme that provides O(1) average lookup time while maintaining strong locality of reference for cache-friendly performance.

The Hopscotch class is organized around an internal Bucket struct, which stores a single key value pair, an occupancy flag to check whether a bucket is empty, and a 64-bit bitmap representing the neighborhood of displaced elements. The hash table itself is implemented as a vector containing Buckets, ensuring fast random access and cache-friendly memory layout. Each Bucket acts as both a storage slot and a small metadata node, allowing efficient management of collisions and element movement within the local neighborhood. This design keeps the implementation compact, minimizes pointer overhead, and aligns closely with the original Hopscotch Hashing algorithm's focus on locality and bitwise efficiency.

#### Implementation Details

The Hopscotch class is built around an internal Bucket struct that holds a key, value, occupancy flag, and a 64-bit bitmap. The hash table is a vector of buckets, using power-of-two sizing so that index calculations use fast bitmasking (index & mask).

- **Insertion (emplace):**

  When inserting, the algorithm computes the key's home index `i` and checks whether its neighborhood (of size `H`) is full using the bucket's bitmap. If full, the table is rehashed (doubled in size). Next, it scans linearly for the nearest empty slot `j`. If `j` lies outside the allowed neighborhood, the algorithm repeatedly hops nearby elements forward (using `move_payload`) to bring the empty slot closer to `i`. This process updates the affected bitmaps with `set_bit` and `clear_bit`, maintaining accurate neighborhood information. Finally, the new element is placed in position `j`, and the home bucket's bitmap marks its offset.

- **Lookup (find):**

  For lookups, the algorithm reads the home bucket's bitmap and iterates only over bits that are set. Each bit represents a valid offset to a bucket that might contain the key. The loop uses fast bit operations (`count_trail_zeros`, `bitmap &= bitmap - 1`) to test those positions efficiently. This bit-guided probing avoids unnecessary scans, ensuring O(1) average search time and excellent cache locality.

- **Design Highlights:**

  - Fast index arithmetic: `mask = size - 1` replaces modulo.
  - Efficient bit operations: built-in intrinsics manage hop bitmaps.
  - Cache-friendly layout: contiguous storage in vector.

### Cuckoo Hashing

The Cuckoo Hashing algorithm is a high-performance, open-addressing hash scheme that guarantees O(1) worst-case lookup time. It achieves this by utilizing two separate hash tables and two independent hash functions. The key principle is that every element must reside in one of its two possible locations.

The Cuckoo class is organized around a simple Bucket struct that stores the element's key, value, and a boolean occupied flag. The two conceptual tables, T1 and T2, are physically stored within a single contiguous vector of buckets, ensuring optimal memory access and cache locality.

#### Implementation Details

The Cuckoo class uses a single vector of buckets (hash_table) to represent the two tables (T1 and T2). Table indexing is handled via bitwise operations: `size` is maintained as a power of two, and `mask` (`size - 1`) enables fast modulo operations for index calculation.

- **Hash Functions:**

  - Table 0 (T1): The index is calculated as the standard `std::hash<Key>{}(key)` masked by `mask`.

  - Table 1 (T2): The index uses the standard hash value XORed with a large, fixed constant `(0x9e3779b97f4a7c15ULL)`, then masked. The XOR operation with this specific constant (derived from the golden ratio) acts as a powerful secondary scramble, ensuring that keys that collide in T1 are highly unlikely to collide again in T2, which is crucial for Cuckoo Hashing's efficiency.

- **Insertion (emplace):**

  The insertion follows the "cuckoo" eviction process:

  1. The new element is initially placed in its T1 position.
  2. If that position is occupied, the existing element is kicked out and moved to its alternate location in the other table.
  3. This process continues, toggling between T1 and T2 for the evicted element, until an empty slot is found.
  4. Cycle Detection: The implementation tracks the number of swaps against the total active elements. If the swap count exceeds this limit, a cycle is assumed, triggering an immediate `rehash()`.

- **Lookup (find):**

  A lookup only requires checking the two possible locations for the key its position in T1 and its position in T2. This ensures that the lookup is extremely fast, achieving O(1) worst-case complexity when a key is present.

- **Rehashing (rehash):**

  Rehashing involves doubling the table size and re-inserting all elements. It is triggered under two conditions:

  1.  The Load Factor exceeds 70%, to maintain a low collision probability.
  2.  An Insertion Cycle is detected, which is necessary to resolve unresolvable key paths in the current table configuration.

### Authors

- Boutzounis Dimitrios - Nikolaos 1115202200112
- Stephanou Iasonas 1115202200246
- Stavrou Spyridon 1115202200172
