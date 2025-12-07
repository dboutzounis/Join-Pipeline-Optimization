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

  - the full Smart_string reference (with type bits masked out)

  This design removes the need for `std::variant`, avoids dynamic memory, and ensures predictable CPU-friendly layouts.

- Null Handling

  A null value contains only the tag `NONE` with no payload.
  All operators check `is_null()` before attempting extraction, allowing efficient propagation of NULLs across joins and scans.

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

This part implements the Unchained Hash Table, a high-performance structure optimized for database join processing.

Key Ideas:

- Contiguous buffer storage

  All `(key, row_id)` pairs are stored in one linear array, grouped by hash prefix. This avoids pointer chasing and improves cache efficiency.

- Directory with Bloom-style tags

  Each directory entry stores a prefix range and a 16-bit tag that quickly filters out non-matching probe keys. Tags are precomputed bit patterns with four bits set.

- Fast hashing

  Uses hardware-accelerated CRC32 when available (`fast_crc32_u32`), producing high-quality 64-bit hashes (`hash32`).

Build & Lookup:

- Build phase:

  1. `key_count()` counts how many keys fall into each prefix.

  2. `build()` allocates the contiguous buffer and computes prefix boundaries.

  3. `insert()` writes each tuple and updates the tag for its slot.

- Lookup:

  `lookup()` checks the Bloom tag via `could_contain()`; if it passes, it scans only the small contiguous range belonging to that prefix.

This design delivers fast, predictable, and memory-efficient join performance, especially for selective queries.

#### Implementation Details

**Hashing**

- Uses `fast_crc32_u32()` to compute CRC32-based hashes.

- `hash32()` mixes the CRC result with a fixed 64-bit constant to produce a stable, high-entropy hash value.

- The upper bits of the hash determine the slot (prefix group), while lower bits are used to select a Bloom tag.

**Directory Layout**

- The `directory` vector stores a 64-bit entry per slot:

  - Upper 48 bits: cumulative count of keys up to this slot (defines the tuple range).
  - Lower 16 bits: Bloom-style tag used for early filtering.

- Prefix ranges are created during `build()` using prefix sums on the counted keys.

**Bloom Tags**

- `tags[2048]` holds a set of precomputed 16-bit bitmasks, each with exactly four bits set.

- During insertion, the tag corresponding to the hash s tag index is OR ed into the directory entry.

- `could_contain()` checks:
  ```
  !(tag & ~entry_tag)
  ```
  allowing extremely fast rejection of keys that cannot match.

**Count, Build, Insert Pipeline**

1. `key_count()`

   Computes the hash prefix and increments its counter; also tracks total tuple count.

2. `build()`

   Allocates the contiguous `buffer` sized to `total_count` and computes prefix boundaries in the directory.

3. `insert()`

   Places each `(key, row_id)` into its exact location within the buffer, while updating the directory s Bloom tag.

**Lookup Path**

- `lookup()` computes the hash, identifies the slot, and loads the directory entry.

- If `could_contain()` fails, lookup exits immediately (fast negative path).

- Otherwise, `produce_matches()` scans only the small contiguous region belonging to that hash prefix and returns matching row IDs.

**Design Benefits**

- No pointer chasing; all tuples stored contiguously in `buffer`.

- Prefix ranges guarantee tight, cache-friendly scans.

- Tag filtering minimizes unnecessary comparisons during probing.

- Structure remains robust even under skewed or repetitive join keys.

#### Test Cases

The unit tests validate the correctness, stability, and filtering behavior of the Unchained hash table implementation:

- Counting and Build Phase

  Tests ensure that `key_count()` correctly tracks prefix frequencies and that build() allocates the buffer and computes directory prefix sums as expected.

- Manual Insertion and Lookup

  Verifies that lookups return the correct row IDs when the directory and buffer are manually constructed, and that Bloom-style tag mismatches prevent false positives.

- Basic Insert and Lookup

  Confirms that `insert()` correctly places keys into the contiguous buffer and that `lookup()` retrieves the correct matches or returns an empty result for missing keys.

- Collision Handling

  Checks that multiple keys hashing to the same slot are stored sequentially in the slot’s range and retrieved in the correct order.

- Tag Filtering Behavior

  Ensures that Bloom tag checks (`could_contain()`) correctly filter out non-matching keys before scanning, minimizing unnecessary buffer probes.

- Insertion Order Preservation

  Validates that keys inserted for the same slot appear in buffer order, enabling deterministic join output ordering within each prefix group.

- Stress Tests

  Inserts and queries dozens of keys to confirm stable behavior across many prefix slots and ensure the implementation handles broader workloads without failure.

### Statistics

- **Optimization Across Tasks vs Base**

| Implementation Stage | Runtime (ms) | Speedup vs Base | Total Time Reduction | Speedup vs Previous Step | Incremental Reduction |
| :------------------- | :----------- | :-------------- | :------------------- | :----------------------- | :-------------------- |
| Base                 | 144565.6     | 1.00x           | 0.00%                | 1.00x                    | 0.00%                 |
| Late Materialization | 96493.8      | 1.50x           | 33.25%               | 1.50x                    | 33.25%                |
| Columnar Joins       | 47579.6      | 3.04x           | 67.09%               | 2.03x                    | 50.69%                |
| Unchained Hashtable  | 38530.8      | 3.75x           | 73.35%               | 1.23x                    | 19.02%                |

- **Optimization Across Tasks vs Robin Hood**

| Implementation Stage | Runtime (ms) | Speedup vs Robin Hood | Total Time Reduction | Speedup vs Previous Step | Incremental Reduction |
| :------------------- | :----------- | :-------------------- | :------------------- | :----------------------- | :-------------------- |
| Robin Hood           | 210915       | 1.00x                 | 0.00%                | 1.00x                    | 0.00%                 |
| Late Materialization | 163745.4     | 1.29x                 | 22.36%               | 1.29x                    | 22.36%                |
| Columnar Joins       | 112482.6     | 1.88x                 | 46.67%               | 1.46x                    | 31.31%                |
| Unchained Hashtable  | 38530.8      | 5.47x                 | 81.73%               | 2.92x                    | 65.75%                |

- **Optimization Across Tasks vs Hopscotch**

| Implementation Stage | Runtime (ms) | Speedup vs Hopscotch | Total Time Reduction | Speedup vs Previous Step | Incremental Reduction |
| :------------------- | :----------- | :------------------- | :------------------- | :----------------------- | :-------------------- |
| Hopscotch            | 134655       | 1.00x                | 0.00%                | 1.00x                    | 0.00%                 |
| Late Materialization | 86432.4      | 1.56x                | 35.81%               | 1.56x                    | 35.81%                |
| Columnar Joins       | 35225.8      | 3.82x                | 73.84%               | 2.45x                    | 59.24%                |
| Unchained Hashtable  | 38530.8      | 3.49x                | 71.39%               | 0.91x                    | -9.38%                |

- **Optimization Across Tasks vs Cuckoo**

| Implementation Stage | Runtime (ms) | Speedup vs Cuckoo | Total Time Reduction | Speedup vs Previous Step | Incremental Reduction |
| :------------------- | :----------- | :---------------- | :------------------- | :----------------------- | :-------------------- |
| Cuckoo               | 138509       | 1.00x             | 0.00%                | 1.00x                    | 0.00%                 |
| Late Materialization | 90346.8      | 1.53x             | 34.77%               | 1.53x                    | 34.77%                |
| Columnar Joins       | 38213.2      | 3.62x             | 72.41%               | 2.36x                    | 57.70%                |
| Unchained Hashtable  | 38530.8      | 3.59x             | 72.18%               | 0.99x                    | -0.83%                |

The results show how each part of the assignment gradually improves the performance of the base solution. Late materialization gives the first noticeable boost, cutting runtime by about 33% by avoiding unnecessary string copies. Switching to columnar joins has the biggest impact, more than doubling the speed compared to the previous step and reducing total runtime by 67% thanks to better cache use and more efficient memory access. The unchained hash table offers another 19% improvement, bringing the total speedup to 3.75× over the original version. Even though this part already helps, it has much more optimization potential, especially once threading and parallel join processing are added in the next assignment.

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
