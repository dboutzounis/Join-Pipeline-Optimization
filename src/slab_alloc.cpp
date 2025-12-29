#include <slab_alloc.h>

uint32_t fast_crc32_u32(uint32_t seed, uint32_t key) {
#ifdef USE_X86_CRC
    return _mm_crc32_u32(seed, key);
#elif defined(USE_ARM_CRC)
    return __crc32w(seed, key);
#endif
}

uint64_t hash32(uint32_t key, uint32_t seed) {
    uint64_t k = 0x8648DBDB;
    uint32_t crc = fast_crc32_u32(seed, key);
    return crc * ((k << 32) + 1);
}

Chunk* allocate_chunk(uint32_t bytes) {
    auto* c = static_cast<Chunk*>(std::malloc(sizeof(Chunk) + bytes));
    c->next = nullptr;
    c->used = 0;
    c->capacity = bytes;
    return c;
}

BuildTuple* chunk_begin(Chunk* c) { return reinterpret_cast<BuildTuple*>(c->data); }
BuildTuple* chunk_end(Chunk* c) { return reinterpret_cast<BuildTuple*>(c->data + c->used); }

Chunk* GlobalAllocator::allocateLarge(uint32_t bytes) { return allocate_chunk(bytes); }

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
    : level1(g), level3(numPartitions), counts(numPartitions, 0), log2_partitions(std::log2(numPartitions)) {}

void ThreadAllocator::consume(uint64_t hash, int32_t key, size_t row_id) {
    uint64_t part = hash >> (64 - log2_partitions);

    auto alloc_l3_chunk_from_level2 = [&]() -> Chunk* {
        const uint32_t need = static_cast<uint32_t>(sizeof(Chunk) + SMALL_CHUNK);
        if (!level2.freeSpace(need)) {
            level2.addSpace(level1.allocateLarge(LARGE_CHUNK));
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

void free_bump_alloc(BumpAlloc& alloc) {
    Chunk* c = alloc.head;
    while (c) {
        Chunk* next = c->next;
        std::free(c);
        c = next;
    }
    alloc.head = alloc.tail = nullptr;
}