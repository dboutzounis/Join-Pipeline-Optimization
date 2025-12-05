#pragma once

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define USE_X86_CRC 1
#elif defined(__aarch64__)
#include <arm_acle.h>
#define USE_ARM_CRC 1
#endif

#include <cstdint>
#include <functional>
#include <iostream>
#include <random>
#include <vector>

uint32_t fast_crc32_u32(uint32_t seed, uint32_t key);

uint64_t hash32(uint32_t key, uint32_t seed);

class Unchained {
    struct Bucket {
        int32_t key;
        size_t row_id;
    };

    static constexpr uint32_t TAG_SHIFT = 32 - 11;

    std::vector<uint64_t> directory;
    std::vector<Bucket> buffer;
    uint64_t shift;
    uint16_t tags[2048];
    std::vector<std::vector<size_t>> count;
    size_t total_count;

    bool could_contain(uint16_t entry, uint64_t hash);

    std::vector<size_t> produce_matches(int32_t key, uint64_t slot, uint64_t entry);

   public:
    Unchained(uint64_t shift = 48);

    inline std::vector<uint64_t>& get_directory() { return directory; }

    inline std::vector<Bucket>& get_buffer() { return buffer; }

    inline uint16_t* get_tags() { return tags; }

    void key_count(int32_t key);

    void build();

    void insert(int32_t key, size_t row_id);

    std::vector<size_t> lookup(int32_t key);
};