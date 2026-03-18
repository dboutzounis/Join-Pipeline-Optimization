# Join Pipeline Optimization

## Indexing Optimization and Parallel Joins

### Authors

- Boutzounis Dimitrios - Nikolaos sdi2200112@di.uoa.gr
- Stephanou Iasonas sdi2200246@di.uoa.gr
- Stavrou Spyridon sdi2200172@di.uoa.gr

### Overview

### Index Optimization

This implementation introduces an internal optimization for column storage while **preserving the exact same `Column_t` public API**.  
The goal is to improve indexing and access efficiency without affecting execution logic or column consumers.

**Core Idea:**

> Storage-level optimizations must not leak into the public API.

All optimizations are strictly internal. Any component using `Column_t` continues to operate identically, independent of how indices are translated or how pages are managed.

#### Implementation Details

**Polymorphic Column Architecture**

- `Column_t` acts as a facade and owns a `std::unique_ptr` to an abstract column implementation
- The abstract base class defines the minimal interface (`get_at`, `size`, `page_num`)
- Concrete implementations (`ValueColumn`, `PageColumn`) override all storage-specific behavior

At construction time, `Column_t` selects the concrete implementation based on the requested storage mode.  
From that point onward, **all column operations are dispatched through the abstract interface**, ensuring runtime polymorphism.

As a result:

- Execution code never performs casts
- Execution code never checks the storage type
- Storage-specific logic is fully encapsulated

This guarantees a stable API and isolates execution logic from storage layout decisions.

**Index Translation**

- Index translation is handled internally by each concrete implementation
- Page-based layouts are used to map a global index to `(pageIndex, offset)`
- The mapping strategy differs per implementation but is completely hidden from callers

This enables fast access while keeping indexing logic localized and optimized.

**Memory Ownership**

- In `ValueOwned` storage, the column allocates and owns its pages
- In `PageOwned` storage, the column stores pointers to externally managed pages
- Ownership semantics are enforced by construction and never checked at runtime

**Error Handling**

- Invalid index access returns `null_value()`
- No exceptions are thrown
- Behavior is deterministic and performance-friendly

This design ensures predictable execution and avoids control-flow overhead in hot paths.

#### Test Cases

The provided test validates the correctness of the **PageOwned** storage mode.

Specifically, it:

- Creates two externally allocated `int32_t` pages
- Pushes both pages into a `Column_t` constructed with `PageOwned` storage
- Verifies that:
  - the column reports the correct number of pages
  - the total number of stored rows is correct
  - values can be retrieved correctly across page boundaries using a global index

The test demonstrates that:

- `Column_t` correctly translates global indices to page-local offsets
- No data copying occurs
- Page-based access is transparent to the caller

### Parallel Building

This part of the project focuses on the parallel construction phase. The goal of this phase is to efficiently build the underlying structure from a large set of input elements by exploiting thread-level parallelism, reducing total construction time compared to a purely sequential approach.

The building process is divided among multiple worker threads, each responsible for processing a subset of the input. To minimize synchronization overhead and contention, the implementation carefully manages memory allocation and insertion logic, ensuring that threads can operate largely independently. Once all threads complete their local work, the partial results are combined into a consistent final structure.

#### Implementation Details

The construction process consists of three distinct phases:

**1.** Parallel Tuple Collection & Partitioning

Instead of building a shared global structure immediately (which would require expensive locking), threads first collect and partition tuples locally.

- Three-Level Slab Allocator: To mitigate `malloc` contention and system call overhead, we implemented a custom memory allocator defined in `slab_alloc.h`.

  - Level 1 (Global): A large arena allocator (`GlobalAllocator`) that reserves a massive block of memory upfront.

  - Level 2 (Thread-Local): Each thread requests large chunks (`LARGE_CHUNK`) from the global allocator.

  - Level 3 (Partition-Local): Threads distribute memory from their large chunks into smaller, partition-specific buffers (`SMALL_CHUNK`).

- Partitioning: As tuples are scanned from the input column, they are hashed. The highest bits of the hash determine the partition index. Tuples are materialized into the corresponding Level 3 buffer using a `BuildTuple` structure.

- Outcome: At the end of this phase, all build-side tuples are materialized and grouped by partition across thread-local buffers.

**2.** Global Sizing (Synchronization Point)

Once all threads finish collection, a lightweight synchronization step occurs in `Unchained::counting_per_partition`.

- We calculate the total number of tuples per partition by aggregating counts from all `ThreadAllocator` instances.

- We compute global offsets for each partition to ensure they map to contiguous regions in the final tuple storage.

- A single contiguous memory block (`Unchained::buffer`) is allocated to hold the final hash table entries.

**3.** Parallel Directory Construction & Materialization

The final build phase populates the `directory` (the hash table index) and the buffer (the tuple storage).

- Dynamic Load Balancing: We employ a dynamic task assignment strategy using an `std::atomic<uint32_t>`. Threads fetch the next available partition index to process. This ensures that if some partitions are heavier than others (skew), threads finishing early can "steal" remaining work, keeping all cores utilized.

- Two-Pass Construction (per Partition): inside `post_process_build`:

  - Counting & Histogram: The thread iterates over all tuple chunks for the assigned partition. It updates the `directory` slots to count how many tuples fall into each hash bucket. A prefix sum is applied to these directory entries to convert counts into absolute offsets within the global buffer.

  - Materialization & Tagging: The thread iterates the tuples a second time. It copies the data (`key` and `row_id`) into the global `buffer` at the calculated offsets. Simultaneously, it computes the Bloom Filter Tag (derived from the hash) and embeds it into the upper bits of the directory pointer.

#### Test Cases

The test cases include a comprehensive suite of unit tests in order to ensure the correctness of the parallel build phases:

- Memory Allocator Integrity: Validates the `GlobalAllocator` and `BumpAlloc` to ensure memory chunks are correctly initialized, linked, and handed out to threads without corruption.

- Partitioning Correctness: Verifies that `ThreadAllocator` correctly assigns tuples to partitions based on the highest bits of the hash, and that `collect_build_tuples` accurately aggregates total counts across multiple threads.

- Offset Calculation: Checks `counting_per_partition` to ensure it correctly computes global prefix sums, determining the exact memory offsets for each partition in the final buffer.

- Parallel Materialization: Tests `post_process_build` to confirm that:

  - Tuples are correctly copied from thread-local buffers to the final contiguous storage.
  - Directory entries are updated to point to the correct, disjoint ranges of tuples (handling hash collisions correctly).

### Parallel Probing

This component implements the **probe phase of a parallel hash join** using a **work-stealing execution model**.  
The goal is to maximize parallelism and load balance while avoiding synchronization overhead during result production.

Each worker thread dynamically acquires work and writes results exclusively to thread-local buffers. The final result is materialized only after all workers complete.

**Core Idea**

> No thread owns a fixed partition of the probe input; instead, work is claimed dynamically.

A shared atomic counter represents the next unprocessed probe index. Workers repeatedly claim chunks of work until all probe tuples are processed. This allows the system to naturally balance load even in the presence of data skew.

#### Implementation Details

1. **Thread-Local Result Allocation**  
   Each worker is assigned its own local result buffer. These buffers mirror the output schema and are pre-initialized before execution begins.

2. **Worker Creation**  
   A fixed number of worker threads are spawned. All workers execute the same probe logic.

3. **Dynamic Work Claiming**  
   Workers atomically claim ranges of probe indices. This ensures:

   - no overlapping work
   - no locks
   - minimal scheduling overhead

4. **Probe and Match Expansion**  
   For each claimed probe tuple:

   - the join key is extracted
   - the hash table is probed
   - all matching build-side rows are expanded
   - output rows are written only to the worker’s local buffer

5. **Result Materialization**  
   After all workers finish, their local results are merged into the final output in a single-threaded phase.

**Work-Stealing Characteristics**

- **Dynamic scheduling** avoids idle threads
- **Chunk-based processing** amortizes atomic operations
- **No shared writes** during the probe phase
- **Lock-free hot path**

**Result Handling Strategy**

The merge phase:

- runs after all workers join
- reads immutable local buffers
- produces a deterministic final result

This separation simplifies correctness reasoning and keeps the hot path fast.

### Statistics

#### Performance Optimization Progression

| Implementation Stage              | Runtime (ms) | Speedup vs Baseline | Total Time Reduction | Speedup vs Previous Step | Incremental Reduction |
| --------------------------------- | ------------ | ------------------- | -------------------- | ------------------------ | --------------------- |
| Baseline (Unchained hash default) | 38.668       | 1.00x               | 0.00%                | 1.00x                    | 0.00%                 |
| Index Optimization                | 26.470       | 1.46x               | 31.55%               | 1.46x                    | 31.55%                |
| Parallel Building                 | 26.081       | 1.48x               | 32.55%               | 1.01x                    | 1.47%                 |
| Parallel Probing                  | 11.242       | 3.44x               | 70.93%               | 2.32x                    | 56.90%                |

- Impact of Index Optimization:

  Optimizing the indexing step to avoid unnecessary copying for integer columns without nulls resulted in a significant **31.55%** reduction in runtime. This demonstrates that memory bandwidth and allocation overhead were major initial bottlenecks.

- Parallel Building Efficiency:

  The **1.47%** incremental improvement reflects the asymmetric nature of hash joins, where the build side is typically much smaller than the probe side. While the absolute time reduction is minor for this dataset size, parallelizing the build phase ensures the system remains scalable and prevents the build step from becoming a bottleneck on larger workloads.

- Dominance of Parallel Probing:

  The most dramatic performance leap occurred with Parallel Probing, which slashed the remaining runtime by nearly **57%**. This confirms that the probe phase is the dominant cost in the join operation (the "hot path") and scales excellently with the number of threads, achieving a final **3.44x** speedup over the baseline.

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
