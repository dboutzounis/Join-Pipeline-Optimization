# Join Pipeline Optimization

## First Assignment: Hash Algorithms

### Authors

- Boutzounis Dimitrios - Nikolaos 1115202200112 sdi2200112@di.uoa.gr
- Stephanou Iasonas 1115202200246 sdi2200246@di.uoa.gr
- Stavrou Spyridon 1115202200172 sdi2200172@di.uoa.gr

### Overview

This assignment focuses on the implementation and comparison of three advanced open-addressing hashing algorithms **Robin Hood**, **Hopscotch**, and **Cuckoo Hashing** commonly used in high-performance data structures and database join operations. The goal of the project is to design, implement, and evaluate these algorithms under a unified interface that allows consistent insertion, search, and rehashing operations while maintaining efficient memory usage and constant-time average complexity. Each algorithm is templated in C++ to support generic key value types and is tested through a unit testing framework based on Catch2. The performance of each algorithm will be compared based on the given SQL queries.

### Robin Hood Hashing (Stephanou Iasonas)

The Robin Hood Hashing algorithm is an open-addressing hash table scheme that improves lookup uniformity and minimizes probe sequence variance by stealing slots from entries with shorter probe distances. This ensures that no element is significantly farther from its home position than others, achieving a balanced and predictable lookup performance across the table. The Robin_Hood class implements this scheme with a compact, contiguous memory layout for cache efficiency, while maintaining clear separation of responsibilities for insertion (`emplace`), search (`find`), and resizing (`rehash`) operations. The implementation supports templated key value types, enabling usage with both primitive and compound structures such as `std::vector`.

#### Implementation Details

The core data structure is an internal `Bucket` struct that holds: a key (`Key key`), an associated value (`T value`), and a `tsl` (distance-to-slot) integer that tracks how far the element has been displaced from its original hash index. An empty bucket is denoted by `tsl = -1`. The hash table itself is implemented as a `std::vector<Bucket>`, with a power-of-two capacity to enable efficient modular arithmetic using bitmasking (`index & (capacity - 1)`).

- **Insertion (emplace)**

  When inserting, the algorithm computes the key's home index using a masked hash value and probes forward linearly: If an empty bucket is found, the new element is placed there. If an occupied bucket has a smaller tsl value than the element being inserted, the two swap positions the "Robin Hood" step giving priority to the element that has probed farther. This continues until the element is placed, guaranteeing that probe distances remain balanced across the table. When the table exceeds 50% occupancy, it is automatically rehash-resized (×2) to maintain efficiency.

- **Lookup (find)**

  The lookup starts from the home index and probes forward while the current bucket's tsl value is greater or equal to the current probe distance. If the key is found, its corresponding value reference is returned. If the probe distance exceeds the stored tsl, the key is known to be absent (early termination). Otherwise, a static dummy object is returned to represent a missing key. This ensures O(1) average lookup time and predictable cache behavior.

- **Rehashing (rehash)**

  The table doubles in capacity and reinserts all elements in their new positions. Each element's new index and distance are recomputed using the updated mask, maintaining Robin Hood invariants after resizing.

- **Design Highlights**

  - Power-of-two indexing: allows modulo-free hash masking.
  - Probe balancing: reduces variance in lookup chain lengths.
  - Automatic growth: triggered at 50% load factor.
  - Contiguous storage: improves CPU cache performance.
  - Template flexibility: supports arbitrary key and value types (e.g., `std::vector<int>`).

#### Test Cases

1. Manual Insertion and Lookup Verification Directly assigns elements to known buckets, verifying that: lookup returns the correct vectors, missing keys yield an empty dummy vector, internal indices remain consistent.

2. Emplace Operation Checks that inserting single and multiple key value pairs works correctly, including: primitive (`int`) and pointer (`const char*`) key types, multi-element value vectors.

3. Rehash Growth and Preservation Stress-tests table expansion by inserting thousands of odd-numbered keys, verifying: keys remain retrievable after each rehash, no value corruption occurs, table size doubles as expected.

4. Collision Handling Forces collisions by inserting keys mapping to identical hash indices, confirming: all colliding keys coexist correctly, lookup correctness and insertion order are preserved, final table printout visualizes slot displacement and balancing behavior.

#### Statistics

| Metric                        |                         Value                          |
| :---------------------------- | :----------------------------------------------------: |
| **Average Total Runtime**     |                     **210,915 ms**                     |
| **Average Total Runtime (s)** |              **210.9 s (≈ 3 min 30.9 s)**              |
| **Fastest Run**               |                 Run 1 - **210,222 ms**                 |
| **Slowest Run**               |                 Run 4 - **211,520 ms**                 |
| **Runtime Range**             | **1,298 ms** (difference between fastest and slowest)  |
| **Runtime Variation**         | ±0.3% from the mean (extremely consistent performance) |

The Robin Hood algorithm achieved an average total runtime of 210.9 seconds (≈ 3 minutes 31 seconds) across five independent runs. Performance was remarkably consistent, with a standard deviation under 0.3% of the mean total runtime. The most computationally expensive queries were 8c, 8d, and 17a, each contributing significantly to total execution time, while smaller queries (e.g. 1-5) executed in under 700 ms. This indicates the implementation scales well but is sensitive to large join sizes or skewed key distributions.

- Most Expensive Queries:

  - Query 17a consistently dominates the total runtime, taking ~53-54 seconds alone. This represents ~25% of the total runtime.

  - Query 8c & 8d (≈ 13-17 seconds each)
  - Queries in group 16 (≈ 3-4.5 seconds each)
  - Queries 20a 20c and 26a 26c (≈ 2-3 seconds each)

  These spikes suggest certain join workloads or data distributions stress the Robin Hood hash table more likely due to higher probe sequences or table occupancy during join operations.

- Light Queries

  Queries in groups 1, 5 and 8b, 18b typically complete in < 300 ms, showing very fast small-scale join performance.

### Hopscotch (Boutzounis Dimitrios - Nikolaos, Stavrou Spyridon)

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

#### Test Cases

1. Manual Insertion and Find Verification

   Manually assigns keys to specific buckets to verify that lookups work correctly for:

   - Keys in their base positions and in neighborhood (collision) slots.
   - Missing keys returning empty dummy vectors. Confirms bitmap tracking and lookup integrity.

2. Emplace Operation: Basic Behavior

   Tests single and multiple insertions using both integer and string keys. Ensures that `emplace` correctly stores and retrieves multi-element value vectors and that table output remains consistent.

3. Emplace with Collisions

   Forces collisions to test hopscotch displacement logic. Verifies that all colliding keys are preserved, retrievable, and correctly relocated within their neighborhood.

4. Rehash Growth and Preservation

   Inserts multiple keys to trigger automatic table rehashing. Confirms that capacity expansion maintains key-value integrity and scales table size predictably.

#### Statistics

| Metric                        |                         Value                         |
| :---------------------------- | :---------------------------------------------------: |
| **Average Total Runtime**     |                    **134,655 ms**                     |
| **Average Total Runtime (s)** |             **134.7 s (≈ 2 min 14.7 s)**              |
| **Fastest Run**               |                Run 1 - **130,081 ms**                 |
| **Slowest Run**               |                Run 4 - **135,966 ms**                 |
| **Runtime Range**             | **5,885 ms** (difference between fastest and slowest) |
| **Runtime Variation**         |          ±2.2% from the mean - (very stable)          |

The Hopscotch Join Hash algorithm achieved an average total runtime of 134.7 seconds (≈ 2 minutes 15 seconds) across five independent runs. Performance was highly consistent, with a standard deviation of approximately 2% across runs. The most time-consuming queries were 8c, 16a, 16b, and 17a - 17f, while smaller joins (queries 1-5) consistently finished in under 700 ms. This indicates that Hopscotch Join Hash efficiently manages hash collisions and maintains robust performance under varied workloads.

- Most Expensive Queries
  - Query 8c is consistently the heaviest, taking around 6.6-6.9 seconds in every run. This single query contributes roughly 5% of the total runtime.
  - Query groups 6a-6f, 7a-7c, 16a-16c, 17a-17c, 20a-20c, and 26a-26c, each taking 1-4 seconds on average. These queries typically involve larger hash joins or higher collision rates.
- Light Queries

  Early queries (e.g., 1a-5c, 11a-12a, 18b-19b) complete extremely quickly <700 ms in most runs. These results highlight the algorithm's low constant-time behavior for small or well-distributed key sets.

### Cuckoo Hashing (Boutzounis Dimitrios - Nikolaos, Stavrou Spyridon)

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

#### Test Cases Manual

1. Insertion and Find Verification

   Manually places keys into specific hash table slots across both tables, ensuring:

   - Each key is retrievable from its correct location.
   - Dual-table lookups (`table 0` and `table 1`) return accurate values.
   - Manual setup of hash indices mirrors internal behavior for controlled validation.

2. Emplace Operation: Basic Behavior

   Tests standard insertion through `emplace` to verify:

   - Single key-value pairs are stored and retrieved correctly.
   - Multiple insertions with both integer and string keys work without collisions.
   - Table printouts confirm proper placement and stability across buckets.

3. Rehash Growth and Preservation

   Inserts a series of growing keys to trigger automatic rehashing, confirming that:

   - Table expansion increases capacity in both sub-tables.
   - All previously inserted keys remain accessible post-rehash.
   - Growth follows expected proportional scaling and load balancing.

4. Rehash Timeout and Cycle Detection

   Simulates a circular insertion scenario to test non-blocking safety:

   - Detects potential infinite relocation loops during insertion.
   - Uses a background thread and timeout mechanism to ensure responsiveness.

#### Statistics

| Metric                        |                          Value                           |
| :---------------------------- | :------------------------------------------------------: |
| **Average Total Runtime**     |                      **138,509 ms**                      |
| **Average Total Runtime (s)** |               **138.5 s (≈ 2 min 18.5 s)**               |
| **Fastest Run**               |                  Run 3 - **137,825 ms**                  |
| **Slowest Run**               |                  Run 4 - **139,003 ms**                  |
| **Runtime Range**             |  **1,178 ms** (difference between fastest and slowest)   |
| **Runtime Variation**         | ±0.4% from the mean - (extremely consistent performance) |

The Cuckoo algorithm achieved an average total runtime of 138.5 seconds (≈ 2 minutes 18 seconds) across five independent runs. Runtime variation was under 0.5%, showing excellent stability and repeatability. The most computationally expensive queries were 8c, 8d, 16b and 20a, corresponding to larger join workloads that likely triggered rehashing and key relocation. Smaller queries (most in groups 1-5) completed in under 300 ms, showcasing efficient performance under lighter workloads. Overall, Cuckoo Join Hash exhibits predictable, stable, and correct performance, with minimal runtime variance and excellent scalability across diverse query types.

- Most Expensive Queries

  - Queries 8c and 8d are the heaviest, consistently taking ≈ 6.8 7.0 seconds each. These contribute roughly 10% of the total runtime.
  - Other high-cost queries include: 6a-6f, 7a-7c, 16a-16d, 17a-17f, 20a-20c, and 26a-26c each taking 1-4 seconds.

  These spikes typically occur when table occupancy triggers rehashing or key displacement chains, a known characteristic of Cuckoo Hashing.

- Light Queries Queries in groups 1-5, 9-12, and 21-25 are consistently fast, completing in under 1 second. This shows that Cuckoo Hashing maintains O(1) efficiency for simple lookups and inserts under moderate load.
