# Join Pipeline Optimization

## Second Assignment: Column store - Unchained Hash

### Authors

- Boutzounis Dimitrios - Nikolaos 1115202200112 sdi2200112@di.uoa.gr
- Stephanou Iasonas 1115202200246 sdi2200246@di.uoa.gr
- Stavrou Spyridon 1115202200172 sdi2200172@di.uoa.gr

### Overview

This project implements the second assignment on the query join operation. The goal is to extend the database execution engine with more efficient data handling, memory usage, and join performance. The implementation includes:

- Optimized VARCHAR Handling (Late Materialization)

  Strings are represented as compact 64-bit references instead of full copies. The new `value_t` type stores both integers and smart string references, reducing memory movement and improving performance.

- Column-store Intermediate Results

  Intermediate data is stored in paginated columnar format (`vector<column_t>`), replacing the previous row-store layout. This improves cache locality and prepares the system for parallel join processing.

- Unchained Hash Table for Joins

  We implement the unchained hash table, using:

  - contiguous tuple storage
  - a directory with embedded Bloom filters
  - fast CRC32-based hashing
    This design supports efficient and robust hash joins.

- Columnar Final Output
  The root join produces a `ColumnarTable` directly, with string materialization performed only at the end when needed.

### Late Materialization and value_t Type (All)

The materialization introduces a compact and efficient way to represent and recover string (VARCHAR) values during query execution. Smart_string encodes the location of a VARCHAR inside the original column-store input using a single 64-bit value. The encoding stores: table ID, column ID, page ID and offset index (position of the string within the page). This avoids copying string data during intermediate execution. The actual string is reconstructed only when needed through `get_value()`, which:

- handles regular pages with offsets

- correctly assembles multi-page long strings (0xffff / 0xfffe markers)

`Value_t` is a lightweight tagged union for execution It stores INT32 or SMART_STRING using bit-packed metadata, uses two low bits to encode type and supports null representation without variant overhead. It provides: `from_int32()` / `from_string()` constructors, safe accessors (`get_int32()`, `get_string()`) and `is_null()` for null checks.

Together, Smart_string and value_t enable late materialization, minimizing string copies and improving performance throughout the execution engine.

#### Implementation Details

- Smart_string Encoding

  Smart_string packs four identifiers into a single 64-bit integer using bit shifts:

  1. table_id

  2. column_id

  3. page_id

  4. offset_idx (position of the string inside the page)

  These fields are combined with bitwise OR operations, ensuring encoding/decoding is constant-time and requires no heap allocation.

- String Reconstruction

  `get_value()` retrieves the original VARCHAR by navigating the column-store pages:

  - For normal pages: it uses the offset table to compute the string's byte range directly.

  - For long strings: it detects the 0xffff/0xfffe markers and reconstructs the string by concatenating the segmented page contents.
    This allows the engine to store only references during execution while still recovering the full string at output time.

- value_t Bit Layout

  value_t embeds the type tag in the lowest 2 bits:

  | Type Tag (binary) | Meaning      |
  | ----------------- | ------------ |
  | **00**            | NULL         |
  | **01**            | INT32        |
  | **10**            | SMART_STRING |

  The remaining 62 bits hold either:

  - a 32-bit integer (shifted left), or

  - the full 64-bit Smart_string reference (with type bits masked out)

  This design removes the need for `std::variant`, avoids dynamic memory, and ensures predictable CPU-friendly layouts.

- Null Handling

  A null value contains only the tag `NONE` with no payload.
  All operators check is_null() before attempting extraction, allowing efficient propagation of NULLs across joins and scans.

#### Test Cases

The materialization tests verify the correctness of the `Smart_string` and `value_t` implementations:

- `Smart_string` Field Encoding/Decoding Ensures that `encode()` correctly packs `table_id`, `column_id`, `page_id`, and `offset_idx` into 64 bits and that the corresponding getters return the original values.

- Default `value_t` Behavior Confirms that a default-constructed `value_t` represents a NULL value and reports its type as `NONE`.

- INT32 Storage and Retrieval Validates that `from_int32()` stores a 32-bit integer using the bit-packed format and that `get_int32()` recovers the exact original value.

- String Reference Handling Checks that wrapping a `Smart_string` inside `value_t` preserves all encoded fields and that `get_string()` returns a proper `Smart_string` instance.

- Type Tag Bitmasking Ensures the lower two bits of `value_t::data` correctly encode the type tag for both INT32 and SMART_STRING values.

These tests confirm that the materialization layer behaves predictably, preserves data integrity, and maintains the correctness of the bit-packed encoding scheme.

### Column - store (Stephanou Iasonas)

This part of the assignment replaces the intermediate row-store representation with a paginated column-store layout to improve cache locality and join performance.

The `Column_t` structure stores values column-by-column across fixed-size pages:

- Paged Storage

  Each column consists of multiple `Page_t` blocks. The page index and offset inside the page are computed via bit shifting (`PAGE_SHIFT`, `PAGE_MASK`), enabling fast indexing.

- Efficient Appends

  `push_back()` appends values sequentially. When a page is full, a new `Page_t` is allocated. This reduces allocation overhead in tight join loops.

- Fast Random Access

  `get_at()` computes the correct page and offset to retrieve any value in O(1) time. Out-of-range accesses safely return NULL.

- Predictable Memory Layout

  Values of the same column are stored contiguously, improving cache usage and preparing the executor for parallel join processing in later assignments.

This columnar format becomes the backbone for intermediate results produced by scans and joins, replacing all row-store structures in the execution engine.

#### Implementation Details

- Paged Column Layout

  - Each `Column_t` consists of a vector of fixed-size `Page_t` objects.
  - Pages store values in a small, contiguous array (`values[]`), enabling cache-friendly sequential access.

- Indexing via Bit Operations

  - Logical row indices are mapped to physical page locations using:
    ```
    pageIndex = index >> PAGE_SHIFT
    offset = index & PAGE_MASK
    ```
  - These bit operations are faster than division/modulo and allow constant-time lookup.

- Dynamic Page Allocation

  - `push_back()` inserts values sequentially.
  - When the current page fills (`offset == 0`), a new page is added automatically.
  - This reduces memory reallocations inside tight join loops.

- Fast Retrieval

  - `get_at()` returns the value at a given index by directly resolving the page and offset.
  - Out-of-bounds access yields a NULL `value_t`, ensuring safe behavior during join probing.

- Preallocation for Scans

  - When a scan node knows its expected row count, the constructor preallocates all required pages:
    ```
    pages_needed = (expected_rows + PAGE_T_SIZE - 1) / PAGE_T_SIZE;
    ```
  - This avoids dynamic growth when the size is known, improving performance.

- Cache-friendly Columnar Organization

  - Values of a single column are laid out contiguously across pages.
  - This improves cache locality during joins, filters, and scans, as operators touch one column at a time.

#### Test Cases

The tests verify the correctness, robustness, and performance behavior of the `Column_t` column-store implementation:

- Basic push/get behavior

  Ensures values inserted with `push_back()` can be retrieved accurately with `get_at()` when stored within a single page.

- Page boundary handling

  Confirms that new pages are allocated automatically once the current page is full, and that values across page boundaries are retrieved correctly.

- Out-of-bounds safety

  Validates that any invalid index (negative, too large, or empty column) returns a NULL `value_t` instead of causing undefined behavior.

- NULL value handling

  Checks that `Column_t` stores and retrieves NULLs correctly, preserving type information throughout.

- Multi-page stress tests

  Exercises insertion and retrieval across many pages (e.g., 3+ full pages), verifying correctness of indexing logic and bit-shift calculations.

- Randomized tests

  Inserts random mixes of integers and NULLs, comparing results against a ground truth vector to ensure reliability under non-uniform workloads.

- High-volume tests (millions of rows)

  Ensures that the column store scales properly, allocating dozens of pages and still maintaining constant-time, correct access.

These tests collectively ensure that the column-store implementation is safe, correct, and performant under realistic database workloads.

### Unchained Hashing (Boutzounis Dimitrios - Nikolaos, Stavrou Spyridon)

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

### Comparison

| Query | Unordered Map (ms) | Robin Hood (ms) | Hopscotch (ms) | Cuckoo (ms) | Fastest       | Speedup vs Unordered (×) |
| :---- | :----------------- | :-------------- | :------------- | :---------- | :------------ | :----------------------- |
| 1a    | 296.6              | 284.6           | 284.4          | 292.4       | Hopscotch     | 1.04                     |
| 1b    | 259.8              | 256.6           | 242.2          | 247.8       | Hopscotch     | 1.07                     |
| 1c    | 89.6               | 88.6            | 86.2           | 95.6        | Hopscotch     | 1.04                     |
| 1d    | 283.8              | 261.4           | 262.2          | 264.2       | Robin Hood    | 1.09                     |
| 2a    | 650.2              | 625.6           | 638.2          | 650.2       | Robin Hood    | 1.04                     |
| 2b    | 592.2              | 558.4           | 552.6          | 548         | Cuckoo        | 1.08                     |
| 2c    | 505.8              | 471.8           | 511.2          | 492.8       | Robin Hood    | 1.07                     |
| 2d    | 671                | 646.6           | 650.6          | 660.2       | Robin Hood    | 1.04                     |
| 3a    | 340.6              | 300.8           | 299.2          | 287.2       | Cuckoo        | 1.19                     |
| 3b    | 304.6              | 266             | 255.2          | 276.6       | Hopscotch     | 1.19                     |
| 3c    | 424.6              | 396.8           | 402.4          | 394.2       | Cuckoo        | 1.08                     |
| 4a    | 386.6              | 361.8           | 333.2          | 359.8       | Hopscotch     | 1.16                     |
| 4b    | 293.4              | 241.4           | 248.8          | 250.8       | Robin Hood    | 1.22                     |
| 4c    | 454.8              | 448             | 413.8          | 449.4       | Hopscotch     | 1.1                      |
| 5a    | 66                 | 65.4            | 66.2           | 66          | Robin Hood    | 1.01                     |
| 5b    | 50.2               | 50.4            | 46.4           | 51          | Hopscotch     | 1.08                     |
| 5c    | 175.4              | 167.6           | 156            | 151.2       | Cuckoo        | 1.16                     |
| 6a    | 2410.6             | 2347.6          | 2284.6         | 2366        | Hopscotch     | 1.06                     |
| 6b    | 2401.6             | 2337.2          | 2275.2         | 2434.8      | Hopscotch     | 1.06                     |
| 6c    | 1987.8             | 1856.8          | 1869.2         | 1921.2      | Robin Hood    | 1.07                     |
| 6d    | 2422.6             | 2376.2          | 2286.4         | 2432        | Hopscotch     | 1.06                     |
| 6e    | 2019.8             | 1934.8          | 1845.2         | 1915        | Hopscotch     | 1.09                     |
| 6f    | 3550.2             | 3369.8          | 3056.8         | 3565.4      | Hopscotch     | 1.16                     |
| 7a    | 4571.6             | 3976.2          | 3406.2         | 3670.6      | Hopscotch     | 1.34                     |
| 7b    | 2117.6             | 2122.8          | 2060.2         | 2152.4      | Hopscotch     | 1.03                     |
| 7c    | 3554.8             | 3750.2          | 3523.4         | 3755.8      | Hopscotch     | 1.01                     |
| 8a    | 281.2              | 323.4           | 309.4          | 269.4       | Cuckoo        | 1.04                     |
| 8b    | 67.6               | 137             | 77.8           | 72.8        | Unordered Map | 1                        |
| 8c    | 7353.2             | 16612           | 6762.8         | 6839.2      | Hopscotch     | 1.09                     |
| 8d    | 4920.2             | 13184.8         | 4340.8         | 4399        | Hopscotch     | 1.13                     |
| 9a    | 447.6              | 409.4           | 413            | 409.6       | Robin Hood    | 1.09                     |
| 9b    | 357                | 316.4           | 309.2          | 311         | Hopscotch     | 1.15                     |
| 9c    | 1005.2             | 1362.2          | 1096.4         | 1257.2      | Unordered Map | 1                        |
| 9d    | 1348               | 1753.8          | 1501.4         | 1555        | Unordered Map | 1                        |
| 10a   | 462.2              | 389             | 392.8          | 388         | Cuckoo        | 1.19                     |
| 10b   | 739.8              | 978.8           | 831.2          | 880.6       | Unordered Map | 1                        |
| 10c   | 1166               | 2005            | 1245.6         | 1298.2      | Unordered Map | 1                        |
| 11a   | 468.2              | 410.8           | 415.4          | 434.2       | Robin Hood    | 1.14                     |
| 11b   | 336.6              | 282.6           | 290.8          | 291.4       | Robin Hood    | 1.19                     |
| 11c   | 698.2              | 515.4           | 513.4          | 509.4       | Cuckoo        | 1.37                     |
| 11d   | 603                | 550.6           | 547.4          | 555.2       | Hopscotch     | 1.1                      |
| 12a   | 209.6              | 209.8           | 202            | 203.6       | Hopscotch     | 1.04                     |
| 12b   | 1366.6             | 1290.6          | 1506.6         | 1342        | Robin Hood    | 1.06                     |
| 12c   | 292.4              | 299.6           | 305.6          | 313.2       | Unordered Map | 1                        |
| 13a   | 1756               | 1766            | 1707.2         | 1735.2      | Hopscotch     | 1.03                     |
| 13b   | 1207.8             | 1153.2          | 1124           | 1150.4      | Hopscotch     | 1.07                     |
| 13c   | 1072               | 1057.2          | 1008.8         | 1073.4      | Hopscotch     | 1.06                     |
| 13d   | 2851.8             | 2757.4          | 2571           | 2572.2      | Hopscotch     | 1.11                     |
| 14a   | 483.2              | 463.4           | 407.6          | 474.4       | Hopscotch     | 1.19                     |
| 14b   | 364                | 344.6           | 326.6          | 343.6       | Hopscotch     | 1.11                     |
| 14c   | 487.8              | 469.2           | 426            | 499.8       | Hopscotch     | 1.15                     |
| 15a   | 396.2              | 377.2           | 391.8          | 358.4       | Cuckoo        | 1.11                     |
| 15b   | 307.6              | 281.8           | 301            | 274.2       | Cuckoo        | 1.12                     |
| 15c   | 551.6              | 560.4           | 512            | 510         | Cuckoo        | 1.08                     |
| 15d   | 553.2              | 545.2           | 505.6          | 474.8       | Cuckoo        | 1.17                     |
| 16a   | 3886.4             | 3504.6          | 3637.6         | 3700.2      | Robin Hood    | 1.11                     |
| 16b   | 4792.6             | 4634.6          | 4538           | 4785.6      | Hopscotch     | 1.06                     |
| 16c   | 3150.8             | 3472.8          | 2810           | 3000        | Hopscotch     | 1.12                     |
| 16d   | 3276.2             | 3016.4          | 2864           | 2901.8      | Hopscotch     | 1.14                     |
| 17a   | 2899.6             | 53947           | 2767           | 2820.8      | Hopscotch     | 1.05                     |
| 17b   | 2703.2             | 3518.6          | 2512.2         | 2434.2      | Cuckoo        | 1.11                     |
| 17c   | 2448.8             | 2456.8          | 2420.4         | 2465.2      | Hopscotch     | 1.01                     |
| 17d   | 2575.4             | 2775.8          | 2473.4         | 2446.8      | Cuckoo        | 1.05                     |
| 17e   | 3864               | 3598.8          | 3522           | 3771.2      | Hopscotch     | 1.1                      |
| 17f   | 3533.6             | 3246.4          | 3389.2         | 3524.2      | Robin Hood    | 1.09                     |
| 18a   | 1826.8             | 1714.6          | 1713.4         | 1705.2      | Cuckoo        | 1.07                     |
| 18b   | 192.8              | 173.6           | 181.6          | 172.6       | Cuckoo        | 1.12                     |
| 18c   | 589.4              | 634             | 612            | 636.8       | Unordered Map | 1                        |
| 19a   | 380                | 364.8           | 332            | 338.4       | Hopscotch     | 1.14                     |
| 19b   | 236.2              | 211.8           | 202.2          | 221.2       | Hopscotch     | 1.17                     |
| 19c   | 560                | 675.4           | 494.6          | 542.6       | Hopscotch     | 1.13                     |
| 19d   | 2748.2             | 3099.6          | 2905           | 2924        | Unordered Map | 1                        |
| 20a   | 4380.2             | 3791            | 3741.2         | 4066.4      | Hopscotch     | 1.17                     |
| 20b   | 2558.8             | 2148.2          | 2215.2         | 2091.4      | Cuckoo        | 1.22                     |
| 20c   | 2783.6             | 2496.6          | 2347.4         | 2421.4      | Hopscotch     | 1.19                     |
| 21a   | 462.2              | 408.6           | 401.4          | 404         | Hopscotch     | 1.15                     |
| 21b   | 449.4              | 414             | 446.4          | 419.4       | Robin Hood    | 1.09                     |
| 21c   | 563.2              | 530.8           | 501.8          | 516.6       | Hopscotch     | 1.12                     |
| 22a   | 530                | 516             | 487.4          | 521.2       | Hopscotch     | 1.09                     |
| 22b   | 516.8              | 472             | 467.8          | 494.2       | Hopscotch     | 1.1                      |
| 22c   | 559.6              | 537.6           | 527.2          | 625.2       | Hopscotch     | 1.06                     |
| 22d   | 712.8              | 676.8           | 664.2          | 711.4       | Hopscotch     | 1.07                     |
| 23a   | 475.6              | 458.6           | 487.6          | 471.6       | Robin Hood    | 1.04                     |
| 23b   | 548.8              | 469.8           | 471.6          | 458.2       | Cuckoo        | 1.2                      |
| 23c   | 515.8              | 492.6           | 494.8          | 507.8       | Robin Hood    | 1.05                     |
| 24a   | 861.6              | 944.2           | 778.6          | 763.4       | Cuckoo        | 1.13                     |
| 24b   | 713                | 851.4           | 681.8          | 693         | Hopscotch     | 1.05                     |
| 25a   | 785                | 731.6           | 741.6          | 733.2       | Robin Hood    | 1.07                     |
| 25b   | 545.2              | 480.2           | 470.6          | 478.4       | Hopscotch     | 1.16                     |
| 25c   | 813                | 752.4           | 777            | 803.6       | Robin Hood    | 1.08                     |
| 26a   | 3238.2             | 3071.2          | 3067           | 3342        | Hopscotch     | 1.06                     |
| 26b   | 2590.2             | 2384            | 2433           | 2374.6      | Cuckoo        | 1.09                     |
| 26c   | 3398.4             | 3212.8          | 3081.4         | 3199        | Hopscotch     | 1.1                      |
| 27a   | 433.8              | 422.8           | 450.4          | 464.2       | Robin Hood    | 1.03                     |
| 27b   | 364.8              | 345.2           | 355.2          | 357.8       | Robin Hood    | 1.06                     |
| 27c   | 537                | 512.8           | 507.8          | 512.2       | Hopscotch     | 1.06                     |
| 28a   | 600.6              | 583.8           | 565.2          | 601.6       | Hopscotch     | 1.06                     |
| 28b   | 436.2              | 404.4           | 409.6          | 425.8       | Robin Hood    | 1.08                     |
| 28c   | 545.6              | 519             | 521.4          | 548.6       | Robin Hood    | 1.05                     |
| 29a   | 767                | 999.2           | 773.4          | 740.6       | Cuckoo        | 1.04                     |
| 29b   | 722                | 982             | 735.4          | 723.2       | Unordered Map | 1                        |
| 29c   | 1162.2             | 1095.8          | 1111.4         | 1066        | Cuckoo        | 1.09                     |
| 30a   | 677.6              | 669             | 677            | 688.2       | Robin Hood    | 1.01                     |
| 30b   | 557.6              | 530.4           | 557.4          | 555         | Robin Hood    | 1.05                     |
| 30c   | 808.4              | 795.4           | 810.4          | 801.4       | Robin Hood    | 1.02                     |
| 31a   | 934                | 847.4           | 849.6          | 846.4       | Cuckoo        | 1.1                      |
| 31b   | 564                | 496.8           | 503.4          | 491.6       | Cuckoo        | 1.15                     |
| 31c   | 1089.2             | 1022.4          | 1036.4         | 1132        | Robin Hood    | 1.07                     |
| 32a   | 571.8              | 544.6           | 548.2          | 550.6       | Robin Hood    | 1.05                     |
| 32b   | 668.2              | 591             | 573.4          | 583         | Hopscotch     | 1.17                     |
| 33a   | 737.6              | 667.2           | 706.8          | 681.4       | Robin Hood    | 1.11                     |
| 33b   | 676.2              | 660.6           | 634            | 648.8       | Hopscotch     | 1.07                     |
| 33c   | 1010.8             | 969             | 870.2          | 863.2       | Cuckoo        | 1.17                     |

#### Robin Hood Observations

**Strong points**

- Works best on balanced workloads where the data is spread evenly it gives steady performance and predictable lookup times.
- Good in read-heavy queries where the same data is accessed often (like joins on columns such as `info_type` or `kind_type`).
- Often strong in small-medium-sized joins (e.g., `1d`, `2a-2d`, `4b`, `6c`), sometimes outperforming Hopscotch.

**Weak points**

- Has one extreme slowdown (query `17a`) due to long probe chains when the table gets crowded.
- Insertions are slower because it moves elements around to keep the table balanced.
- Tends to slow down slightly in very large joins because of its relocation cost when many insertions occur.
- Average speedup is smaller (×1.07), meaning it's generally reliable but rarely the top performer.

#### Hopscotch Observations

**Strong points**

- It was the fastest in most queries, especially in the big, complex ones with many joins (like when several tables are connected at once).
- Works very well in queries that touch many rows, because it keeps related keys close together in memory, which speeds up lookups.
- Also performs very well in smaller queries, showing consistently low latency even when only a few keys are involved.
- Achieved the second highest speedup overall (up to ×1.34 faster than the unordered map).

**Weak points**

- Occasionally slightly slower in some very specific high-collision or irregular access patterns, but the difference is minor.
- Uses a small bitmap per bucket, which adds a bit of extra memory cost compared to Robin Hood.

#### Cuckoo Hahsing Observations

**Strong points**

- Shows the most consistent performance it may not always be the fastest, but it's rarely slow.
- Performs very well in dense joins where many tables are connected through common keys (like `movie_info`, `movie_info_idx`, `movie_keyword`, and `movie_companies`).
- Very stable under heavy load, with almost no performance variation between runs.
- Achieved the highest individual speedup, like ×1.37 faster in query `11c`.

**Weak points**

- When the table becomes full, insertions can cause many reinsertions (relocations) and slow down.
- A bit slower in small, simple queries because it computes two hash functions for each lookup.
- Uses more memory since it needs two hash tables internally.

## Compile and Run

> [!TIP]
> Run all commands in the **root directory** of this project.

### 1. Download the IMDB dataset

```
./download_imdb.sh
```

### 2. Build the project

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev
cmake --build build -- -j $(nproc)
```

### 3. Build the DuckDB database

```
./build/build_database imdb.db
```

### 4. Build the cache (required)

> [!TIP]
> If you are using `Linux x86_64` you can download our prebuilt cache with:
>
> ```
> wget http://share.uoa.gr/protected/all-download/sigmod25/sigmod25_cache_x86.tar.gz
> ```
>
> If you are using `macOS arm64` you can download our prebuilt cache with:
>
> ```
> wget http://share.uoa.gr/protected/all-download/sigmod25/sigmod25_cache_arm.tar.gz
> ```
>
> For all other systems you will need to build the cache on your own.

```
./build/build_cache plans.json
```

### 5. Build all optimized executables

Also after you have built the cache you no longer need to build the `run` executable
every time (which depends on duckdb and can be slow to compile). Just compile
the executable that uses the cache:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev
cmake --build build -- -j $(nproc) fast
cmake --build build -- -j $(nproc) unit_tests
```

### 6. Run the algorithms

```
./build/fast plans.json
./build/unit_tests
```
