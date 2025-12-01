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

class Unchained {
    struct Bucket {
        int32_t key;
        size_t row_id;
    };

    std::vector<uint64_t> directory;
    std::vector<Bucket> buffer;
    uint64_t shift;
    uint16_t tags[2048];
    std::vector<std::vector<size_t>> count;
    size_t total_count;

    inline bool could_contain(uint16_t entry, uint64_t hash) {
        uint16_t slot = static_cast<uint32_t>(hash) >> (32 - 11);
        uint16_t tag = tags[slot];
        return !(tag & ~entry);
    }

    inline std::vector<size_t> produce_matches(int32_t key, uint64_t slot, uint64_t entry) {
        std::vector<size_t> matches;
        size_t start = slot != 0 ? directory[slot - 1] >> 16 : 0;
        size_t end = entry >> 16;
        for (size_t i = start; i < end; i++)
            if (buffer[i].key == key) matches.push_back(std::move(buffer[i].row_id));
        return matches;
    }

   public:
    Unchained(uint64_t shift = 48) : total_count(0) {
        this->shift = shift;
        directory.assign(1 << (64 - shift), 0);
        count.assign(1 << (64 - shift), std::vector<size_t>(2, 0));
        size_t k = 0;
        for (uint32_t i = 0; i < 65536; i++) {
            if (__builtin_popcount(i) == 4) {
                tags[k++] = static_cast<uint16_t>(i);
                if (k == 1820) break;
            }
        }

        std::mt19937_64 rng(0xBADF00D123456789ULL);
        std::uniform_int_distribution<int> dist(0, 1819);
        while (k < 2048) tags[k++] = tags[dist(rng)];
    }

    inline void key_count(int32_t key) {
        uint64_t hash_value = hash32(key, 0L);
        hash_value >>= shift;
        count[hash_value][0]++;
        total_count++;
    }

    void build() {
        buffer.assign(total_count, Bucket{});
        directory[0] |= static_cast<uint64_t>(count[0][0]) << 16;
        for (size_t i = 1; i < count.size(); i++) directory[i] |= ((directory[i - 1] >> 16) + count[i][0]) << 16;
    }

    inline void insert(int32_t key, size_t row_id) {
        uint64_t hash = hash32(key, 0L);
        uint64_t slot = hash >> shift;
        size_t start = slot != 0 ? directory[slot - 1] >> 16 : 0;
        size_t end = directory[slot] >> 16;
        buffer[start + count[slot][1]].key = std::move(key);
        buffer[start + count[slot][1]].row_id = std::move(row_id);
        count[slot][1]++;
        uint16_t tag_slot = static_cast<uint32_t>(hash) >> (32 - 11);
        uint16_t tag = tags[tag_slot];
        directory[slot] |= tag;
    }

    std::vector<size_t> lookup(int32_t key) {
        uint64_t hash = hash32(key, 0L);
        uint64_t slot = hash >> shift;
        uint64_t entry = directory[slot];
        if (!could_contain(static_cast<uint16_t>(entry), hash)) {
            static std::vector<size_t> dummy{};
            return dummy;
        }
        return produce_matches(key, slot, entry);
    }
};