#pragma once

#include <materialization.h>

#include <cmath>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>

struct BuildTuple {
    uint64_t hash;
    int32_t key;
    size_t row_id;
};

struct Chunk {
    Chunk* next;
    uint32_t used;
    uint32_t capacity;
    alignas(64) uint8_t data[];
};

static Chunk* allocate_chunk(uint32_t bytes);

struct GlobalAllocator {
    Chunk* allocateLarge(uint32_t bytes);
};

struct BumpAlloc {
    Chunk* head = nullptr;
    Chunk* tail = nullptr;

    bool freeSpace(uint32_t bytes) const;
    void addSpace(Chunk* c);
    void* allocate(uint32_t bytes);
};

struct ThreadAllocator {
    GlobalAllocator& level1;
    BumpAlloc level2;
    std::vector<BumpAlloc> level3;
    std::vector<uint64_t> counts;
    uint32_t log2_partitions;

    ThreadAllocator(GlobalAllocator& g, uint32_t numPartitions);

    void consume(uint64_t hash, int32_t key, size_t row_id);
};

struct CollectedTuples {
    std::vector<std::unique_ptr<ThreadAllocator>> threads;
    uint32_t num_partitiions;
};

template <typename Column>
CollectedTuples collect_build_tuples(const Column& build_column, size_t num_rows, uint32_t num_threads, uint32_t num_partitions) {
    GlobalAllocator global;
    CollectedTuples result;
    result.num_partitiions = num_partitions;
    result.threads.resize(num_threads);

    std::vector<std::thread> workers;

    for (uint32_t tid = 0; tid < num_threads; tid++) {
        workers.emplace_back([&, tid]() {
            auto alloc = std::make_unique<ThreadAllocator>(global, num_partitions);

            size_t begin = (num_rows * tid) / num_threads;
            size_t end = (num_rows * (tid + 1)) / num_threads;

            for (size_t i = begin; i < end; i++) {
                value_t v = build_column.get_at(i);
                if (v.is_null()) continue;

                int32_t key = v.get_int32();
                uint64_t hash = hash32(static_cast<uint32_t>(key), 0);

                alloc->consume(hash, key, i);
            }

            result.threads[tid] = std::move(alloc);
        });
    }
    for (auto& t : workers) t.join();

    return result;
}

struct MergedPartition {
    Chunk* head;
    Chunk* tail;
    uint64_t tuple_count;
};

MergedPartition merge_partition(const CollectedTuples collected, uint32_t partition);
