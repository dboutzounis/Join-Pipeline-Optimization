#include <unchained.h>

Unchained::Unchained(uint64_t shift) : total_count(0), shift(shift) {
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

Unchained::~Unchained() {
    if (buffer) std::free(buffer);
}

void Unchained::allocate_tuple_storage(uint64_t total_tuples) {
    buffer = static_cast<Bucket*>(std::malloc(total_tuples * sizeof(Bucket)));
    total_count = total_tuples;
}

bool Unchained::could_contain(uint16_t entry, uint64_t hash) {
    uint16_t slot = static_cast<uint32_t>(hash) >> TAG_SHIFT;
    uint16_t tag = tags[slot];
    return !(tag & ~entry);
}

std::vector<size_t> Unchained::produce_matches(int32_t key, uint64_t slot, uint64_t entry) {
    std::vector<size_t> matches;
    size_t start = slot != 0 ? directory[slot - 1] >> 16 : 0;
    size_t end = entry >> 16;
    for (size_t i = start; i < end; i++)
        if (buffer[i].key == key) matches.push_back(buffer[i].row_id);
    return matches;
}

void Unchained::key_count(int32_t key) {
    uint64_t hash_value = hash32(key, 0L);
    hash_value >>= shift;
    count[hash_value][0]++;
    total_count++;
}

void Unchained::build() {
    buffer = static_cast<Bucket*>(std::malloc(total_count * sizeof(Bucket)));
    directory[0] |= static_cast<uint64_t>(count[0][0]) << 16;
    for (size_t i = 1; i < count.size(); i++) directory[i] |= ((directory[i - 1] >> 16) + count[i][0]) << 16;
}

void Unchained::insert(int32_t key, size_t row_id) {
    uint64_t hash = hash32(key, 0L);
    uint64_t slot = hash >> shift;
    size_t start = slot != 0 ? directory[slot - 1] >> 16 : 0;
    size_t end = directory[slot] >> 16;
    buffer[start + count[slot][1]].key = key;
    buffer[start + count[slot][1]].row_id = row_id;
    count[slot][1]++;
    uint16_t tag_slot = static_cast<uint32_t>(hash) >> TAG_SHIFT;
    uint16_t tag = tags[tag_slot];
    directory[slot] |= tag;
}

std::vector<PartitionParams> Unchained::counting_per_partition(const CollectedTuples& collected) {
    std::vector<uint64_t> partition_counts(collected.num_partitiions, 0);
    for (const auto& t : collected.threads)
        for (uint32_t p = 0; p < collected.num_partitiions; ++p) partition_counts[p] += t->counts[p];

    std::vector<uint64_t> partition_offsets(collected.num_partitiions + 1, 0);
    for (uint32_t p = 0; p < collected.num_partitiions; ++p) partition_offsets[p + 1] = partition_offsets[p] + partition_counts[p];

    size_t log2_partitions = __builtin_ctz(collected.num_partitiions);
    size_t slots_per_partition = directory.size() >> log2_partitions;

    std::vector<PartitionParams> params_per_partition(collected.num_partitiions);
    for (uint32_t p = 0; p < collected.num_partitiions; ++p) {
        params_per_partition[p].buffer_offset = partition_offsets[p];
        params_per_partition[p].tuple_count = partition_counts[p];
        params_per_partition[p].dir_begin = p * slots_per_partition;
        params_per_partition[p].dir_end = (p + 1) * slots_per_partition;
    }

    return params_per_partition;
}

void Unchained::post_process_build(const CollectedTuples& collected, const PartitionParams& params, uint32_t partition) {
    if (params.tuple_count == 0) return;

    for (const auto& t : collected.threads) {
        Chunk* c = t->level3[partition].head;
        while (c) {
            for (BuildTuple* it = chunk_begin(c); it != chunk_end(c); ++it) {
                uint64_t slot = it->hash >> shift;
                directory[slot] += static_cast<uint64_t>(1) << 16;
                uint16_t tag_slot = static_cast<uint32_t>(it->hash) >> TAG_SHIFT;
                uint16_t tag = tags[tag_slot];
                directory[slot] |= tag;
            }
            c = c->next;
        }
    }

    uint64_t cur = params.buffer_offset;

    for (size_t i = params.dir_begin; i < params.dir_end; ++i) {
        uint64_t val = directory[i] >> 16;
        directory[i] = (cur << 16) | static_cast<uint16_t>(directory[i]);
        cur += val;
    }

    for (const auto& t : collected.threads) {
        Chunk* c = t->level3[partition].head;
        while (c) {
            for (BuildTuple* it = chunk_begin(c); it != chunk_end(c); ++it) {
                uint64_t slot = it->hash >> shift;
                size_t pos = directory[slot] >> 16;
                buffer[pos].key = it->key;
                buffer[pos].row_id = it->row_id;
                directory[slot] += static_cast<uint64_t>(1) << 16;
            }
            c = c->next;
        }
    }
}

std::vector<size_t> Unchained::lookup(int32_t key) {
    uint64_t hash = hash32(key, 0L);
    uint64_t slot = hash >> shift;
    uint64_t entry = directory[slot];
    if (!could_contain(static_cast<uint16_t>(entry), hash)) {
        static std::vector<size_t> dummy{};
        return dummy;
    }
    return produce_matches(key, slot, entry);
}