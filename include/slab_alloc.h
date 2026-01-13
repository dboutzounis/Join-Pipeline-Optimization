#pragma once

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define USE_X86_CRC 1
#elif defined(__aarch64__)
#include <arm_acle.h>
#define USE_ARM_CRC 1
#endif

#include <materialization.h>

#include <cmath>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

static constexpr uint32_t SMALL_CHUNK = 1u << 14;
static constexpr uint32_t LARGE_CHUNK = 1u << 20;

inline uint32_t fast_crc32_u32(uint32_t seed, uint32_t key) {
#ifdef USE_X86_CRC
    return _mm_crc32_u32(seed, key);
#elif defined(USE_ARM_CRC)
    return __crc32w(seed, key);
#endif
}

inline uint64_t hash32(uint32_t key, uint32_t seed) {
    uint64_t k = 0x8648DBDB;
    uint32_t crc = fast_crc32_u32(seed, key);
    return crc * ((k << 32) + 1);
}

struct BuildTuple {
    uint64_t hash;
    int32_t key;
    size_t row_id;
};

struct alignas(64) Chunk {
    Chunk* next;
    uint32_t used;
    uint32_t capacity;
    uint8_t data[];
};

BuildTuple* chunk_begin(Chunk* c);
BuildTuple* chunk_end(Chunk* c);

struct GlobalAllocator {
    uint8_t* base;
    uint64_t size;
    uint64_t offset;

    GlobalAllocator(uint8_t* b, uint64_t s);
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
    std::vector<uint32_t> slot_counts;
    uint64_t shift;

    ThreadAllocator(GlobalAllocator& g, uint32_t numPartitions);

    void consume(uint64_t hash, int32_t key, size_t row_id);
};

struct CollectedTuples {
    std::vector<std::unique_ptr<ThreadAllocator>> threads;
    uint32_t num_partitiions;
    uint8_t* global_base = nullptr;
};

template <typename Column>
CollectedTuples collect_build_tuples(const Column& build_column, size_t num_rows, uint32_t num_threads, uint32_t num_partitions) {
    uint64_t tuples_per_thread = (num_rows + num_threads - 1) / num_threads;
    uint64_t l3_bytes = tuples_per_thread * sizeof(BuildTuple) * 2;
    uint64_t l2_bytes = num_partitions * (sizeof(Chunk) + SMALL_CHUNK);
    uint64_t per_thread_bytes = l3_bytes + l2_bytes + LARGE_CHUNK;
    uint8_t* global_base = static_cast<uint8_t*>(std::malloc(per_thread_bytes * num_threads));

    CollectedTuples result;
    result.num_partitiions = num_partitions;
    result.threads.resize(num_threads);
    result.global_base = global_base;

    std::vector<std::thread> workers;

    for (uint32_t tid = 0; tid < num_threads; tid++) {
        workers.emplace_back([&, tid]() {
            uint8_t* thread_base = global_base + tid * per_thread_bytes;
            GlobalAllocator thread_global(thread_base, per_thread_bytes);

            auto alloc = std::make_unique<ThreadAllocator>(thread_global, num_partitions);

            size_t begin = (num_rows * tid) / num_threads;
            size_t end = (num_rows * (tid + 1)) / num_threads;

            for (size_t i = begin; i < end; i++) {
                value_t v = build_column.get_at(i);
                if (v.is_null()) continue;

                int32_t key = v.get_int32();
                uint64_t hash = hash32(static_cast<uint32_t>(key), 0L);

                alloc->consume(hash, key, i);
            }

            result.threads[tid] = std::move(alloc);
        });
    }
    for (auto& t : workers) t.join();

    return result;
}

struct PartitionParams {
    uint64_t buffer_offset;
    uint64_t tuple_count;
    size_t dir_begin;
    size_t dir_end;
};
