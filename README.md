# Join Pipeline Optimization

## Third Assignment: Indexing Optimization and Parallel Joins

### Authors

- Boutzounis Dimitrios - Nikolaos 1115202200112 sdi2200112@di.uoa.gr
- Stephanou Iasonas 1115202200246 sdi2200246@di.uoa.gr
- Stavrou Spyridon 1115202200172 sdi2200172@di.uoa.gr

### Overview

### Index Optimization (Stephanou Iasonas)

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

### Parallel Building (Boutzounis Dimitrios - Nikolaos, Stavrou Spyridon)

This part of the project focuses on the parallel construction phase. The goal of this phase is to efficiently build the underlying structure from a large set of input elements by exploiting thread-level parallelism, reducing total construction time compared to a purely sequential approach.

The building process is divided among multiple worker threads, each responsible for processing a subset of the input. To minimize synchronization overhead and contention, the implementation carefully manages memory allocation and insertion logic, ensuring that threads can operate largely independently. Once all threads complete their local work, the partial results are combined into a consistent final structure.

#### Implementation Details

The building phase is implemented with an emphasis on parallelism, memory efficiency, and low synchronization overhead. It is primarily realized through the cooperation of three components: the parallel execution logic, a custom slab-based memory allocator, and an unchained hash-based structure.

- Parallel Construction Logic

  The building phase is orchestrated in `execute.cpp`. The input dataset is partitioned among multiple worker threads, with each thread responsible for inserting its assigned elements into the shared structure. Threads operate concurrently during construction, enabling efficient utilization of multi-core systems.

  Work distribution is static, ensuring that each thread processes a disjoint subset of the input. This approach minimizes coordination costs and avoids dynamic scheduling overhead during the build.

- Unchained Data Structure

  Each bucket grows independently, allowing concurrent insertions across different buckets with minimal contention. This design is particularly well-suited for parallel builds, as it avoids fine-grained locking on individual elements and reduces pointer chasing during construction.

- Slab-Based Memory Allocation

  Memory management during the build phase is handled by a custom slab allocator (`slab_alloc.h`,`slab_alloc.cpp`). Rather than allocating memory per element using standard heap allocation, the slab allocator pre-allocates large memory blocks (slabs) and serves fixed-size objects from them.

  This strategy offers several advantages during parallel construction:

  - Constant-time allocations for new elements
  - Improved cache locality due to contiguous memory layout
  - Reduced allocator contention across threads

  Each thread can obtain memory from slabs without frequent synchronization, making allocation scalable as the number of threads increases.

- Synchronization Strategy

  Synchronization during the building phase is deliberately kept minimal. Threads insert elements independently, and shared state is only accessed when necessary (e.g., when extending bucket storage). By combining unchained buckets with slab-based allocation, the implementation **avoids locks** and relies on coarse-grained coordination at well-defined points.

  This design ensures that the final structure is deterministic and equivalent to a sequential build, while achieving significantly better performance under parallel execution.

#### Test Cases

The building phase is validated using a focused set of unit tests covering memory allocation, parallel tuple collection, and hash table construction.

Allocator tests verify correct chunk initialization, bump allocation behavior, and proper cleanup, ensuring safe and efficient memory management during the build.

Thread-level tests confirm that tuples are consistently assigned to the correct hash partitions, and that per-thread and global tuple counts are accurate.

Partition-level tests validate the computation of prefix sums and buffer offsets, guaranteeing that each partition writes into a disjoint region of the global tuple buffer.

Finally, post-processing tests ensure that tuples from multiple threads are copied correctly, directory entries define valid non-overlapping ranges per hash slot, and the final unchained structure matches the expected build output.

Together, these tests ensure that the parallel building phase is correct, deterministic, and safe for subsequent probe operations.

### Parallel Probing (Stephanou Iasonas)

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

#### Test Cases

### Statistics

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
