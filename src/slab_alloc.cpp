#include <slab_alloc.h>

GlobalAllocator::GlobalAllocator(uint8_t* b, uint64_t s) : base(b), size(s), offset(0) {}

Chunk* GlobalAllocator::allocateLarge(uint32_t bytes) {
    uint64_t off = offset;
    offset += sizeof(Chunk) + bytes;
    return reinterpret_cast<Chunk*>(base + off);
}

BuildTuple* chunk_begin(Chunk* c) { return reinterpret_cast<BuildTuple*>(c->data); }
BuildTuple* chunk_end(Chunk* c) { return reinterpret_cast<BuildTuple*>(c->data + c->used); }

bool BumpAlloc::freeSpace(uint32_t bytes) const { return tail && (tail->used + bytes <= tail->capacity); }

void BumpAlloc::addSpace(Chunk* c) {
    if (!head) {
        head = tail = c;
    } else {
        tail->next = c;
        tail = c;
    }
}

void* BumpAlloc::allocate(uint32_t bytes) {
    uint8_t* ptr = tail->data + tail->used;
    tail->used += bytes;
    return ptr;
}

ThreadAllocator::ThreadAllocator(GlobalAllocator& g, uint32_t numPartitions)
    : level1(g), level3(numPartitions), counts(numPartitions, 0), log2_partitions(__builtin_ctz(numPartitions)) {}

void ThreadAllocator::consume(uint64_t hash, int32_t key, size_t row_id) {
    uint64_t part = hash >> (64 - log2_partitions);

    auto alloc_l3_chunk_from_level2 = [&]() -> Chunk* {
        const uint32_t need = static_cast<uint32_t>(sizeof(Chunk) + SMALL_CHUNK);
        if (!level2.freeSpace(need)) {
            Chunk* c = level1.allocateLarge(LARGE_CHUNK);
            c->next = nullptr;
            c->used = 0;
            c->capacity = LARGE_CHUNK;
            level2.addSpace(c);
        }

        auto* c = static_cast<Chunk*>(level2.allocate(need));

        c->next = nullptr;
        c->used = 0;
        c->capacity = SMALL_CHUNK;

        return c;
    };

    if (!level3[part].freeSpace(sizeof(BuildTuple))) {
        level3[part].addSpace(alloc_l3_chunk_from_level2());
    }

    auto* t = static_cast<BuildTuple*>(level3[part].allocate(sizeof(BuildTuple)));
    t->hash = hash;
    t->key = key;
    t->row_id = row_id;

    counts[part]++;
}