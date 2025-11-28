#pragma once

#include <zlib.h>

#include <cstdint>
#include <functional>
#include <iostream>
#include <random>
#include <vector>

class Unchained {
    struct Bucket {
        int32_t key;
        size_t row_id;
    };

    std::vector<uint64_t> directory;
    std::vector<Bucket> buffer;
    uint64_t shift;
    uint16_t tags[2048];

    uint64_t hash32(const int32_t& key, const int32_t& seed) {
        uint64_t k = 0x8648DBDB;
        uint32_t crc = crc32(seed, reinterpret_cast<const Bytef*>(&key), sizeof(key));
        return crc * ((k << 32) + 1);
    }

    bool couldContain(const uint16_t& entry, const uint64_t& hash) {
        uint16_t slot = static_cast<uint32_t>(hash) >> (32 - 11);
        uint16_t tag = tags[slot];
        return !(tag & ~entry);
    }

    void produceMatches(const int32_t& key, const uint64_t& slot, const uint64_t& entry) {
        size_t start = slot != 0 ? directory[slot - 1] >> 16 : 0;
        size_t end = entry >> 16;
        for (size_t i = start; i < end; i++) {
            // if(buffer[i].key == key)                                                 ????????
        }
    }

   public:
    Unchained(uint64_t shift = 48) {
        this->shift = shift;
        directory.assign(1 << (64 - shift), 0);

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

    void lookup(const int32_t& key, const uint64_t& hash) {
        uint64_t slot = hash >> shift;
        uint64_t entry = directory[slot];
        if (!couldContain(entry, hash)) return;
        produceMatches(key, slot, entry);
    }
};
