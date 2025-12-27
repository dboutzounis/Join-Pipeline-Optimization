#include <slab_alloc.h>

static Chunk* allocate_chunk(uint32_t bytes) {
    auto* c = static_cast<Chunk*>(std::malloc(sizeof(Chunk) + bytes));
    c->next = nullptr;
    c->used = 0;
    c->capacity = bytes;
    return c;
}

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

    if (!level3[part].freeSpace(sizeof(BuildTuple))) {
        if (!level2.freeSpace(sizeof(Chunk))) {
            level2.addSpace(level1.allocateLarge(1 << 20));
        }
        level3[part].addSpace(static_cast<Chunk*>(level2.allocate(1 << 14)));
    }

    auto* t = static_cast<BuildTuple*>(level3[part].allocate(sizeof(BuildTuple)));
    t->hash = hash;
    t->key;
    t->row_id = row_id;

    counts[part]++;
}

MergedPartition merge_partition(const CollectedTuples collected, uint32_t partition) {
    Chunk* merged_head = nullptr;
    Chunk* merged_tail = nullptr;
    uint64_t total = 0;

    for (const auto& thread_alloc : collected.threads) {
        BumpAlloc& alloc = thread_alloc->level3[partition];
        Chunk* h = alloc.head;

        if (!h) continue;

        if (!merged_head) {
            merged_head = h;
            merged_tail = alloc.tail;
        } else {
            merged_tail->next = h;
            merged_tail = alloc.tail;
        }

        total += thread_alloc->counts[partition];
    }

    return {merged_head, merged_tail, total};
}