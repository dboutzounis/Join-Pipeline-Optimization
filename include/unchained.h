#pragma once

#include <zlib.h>

#include <cstdint>
#include <functional>
#include <iostream>
#include <random>
#include <vector>

uint64_t hash32(const uint32_t& key, const uint32_t& seed) {
    uint64_t k = 0x8648DBDB;
    uint32_t crc = crc32(seed, reinterpret_cast<const Bytef*>(&key), sizeof(key));
    return crc * ((k << 32) + 1);
}

class Unchained {
    struct Bucket {
        int32_t key;
        size_t row_id;
        bool occupied = false;
    };

    std::vector<uint64_t> directory;
    std::vector<Bucket> buffer;
    uint64_t shift;
    uint16_t tags[2048];

    bool could_contain(const uint16_t& entry, const uint64_t& hash) {
        uint16_t slot = static_cast<uint32_t>(hash) >> (32 - 11);
        uint16_t tag = tags[slot];
        return !(tag & ~entry);
    }

    std::vector<size_t> produce_matches(const int32_t& key, const uint64_t& slot, const uint64_t& entry) {
        std::vector<size_t> matches;
        size_t start = slot != 0 ? directory[slot - 1] >> 16 : 0;
        size_t end = entry >> 16;
        for (size_t i = start; i < end; i++)
            if (buffer[i].key == key) matches.push_back(std::move(buffer[i].row_id));
        return matches;
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

    void init_buffer(const size_t& size) { buffer.assign(size, Bucket{}); }

    void init_directory(const std::vector<size_t>& count_vec, const size_t& vec_size) {
        directory[0] |= static_cast<uint64_t>(count_vec[0]) << 16;
        for (size_t i = 1; i < vec_size; i++) directory[i] |= ((directory[i - 1] >> 16) + count_vec[i]) << 16;
    }

    void insert(const int32_t& key, const size_t& row_id) {
        uint64_t hash = hash32(key, 0L);
        uint64_t slot = hash >> shift;
        size_t start = slot != 0 ? directory[slot - 1] >> 16 : 0;
        size_t end = directory[slot] >> 16;
        for (size_t i = start; i < end; i++) {
            if (!buffer[i].occupied) {
                buffer[i].key = std::move(key);
                buffer[i].row_id = std::move(row_id);
                buffer[i].occupied = true;
                uint16_t tag_slot = static_cast<uint32_t>(hash) >> (32 - 11);
                uint16_t tag = tags[tag_slot];
                directory[slot] |= tag;
                break;
            }
        }
    }

    std::vector<size_t> lookup(const int32_t& key, const uint64_t& hash) {
        uint64_t slot = hash >> shift;
        uint64_t entry = directory[slot];
        if (!could_contain(static_cast<uint16_t>(entry), hash)) {
            static std::vector<size_t> dummy{};
            return dummy;
        }
        return produce_matches(key, slot, entry);
    }
};