#include "block_cache.h"

namespace leveldb {

static void DeleteCachedBlock(const Slice& key, void* value) {
    BlockMeta* block_meta = reinterpret_cast<BlockMeta*>(value);
    delete block_meta;
}

BlockCache::BlockCache(size_t capacity) {
    cache = NewLRUCache(capacity);
}

void BlockCache::SetCapacity(size_t cap) {
    cache->SetCapacity(cap);
}


void BlockCache::Insert(const BlockId & bid, const BlockMeta * block_meta) {
    Slice key((const char*)&bid, sizeof(bid));
    Cache::Handle * handle = cache->Insert(key, (void*)block_meta, block_meta->Length(), DeleteCachedBlock);
    Put(handle);
}

Cache::Handle* BlockCache::Get(const BlockId & bid, BlockMeta *& block_meta) {
    Slice key((const char*)&bid, sizeof(bid));
    Cache::Handle * handle = cache->Lookup(key);
    if (handle != nullptr) {
        block_meta = reinterpret_cast<BlockMeta*>(cache->Value(handle));
        return handle;
    }
    return nullptr;
}

void BlockCache::Put(Cache::Handle * handle) {
    cache->Release(handle);
}

void BlockCache::Clear() {
    cache->Prune();
}

BlockCache * NewBlockCache(size_t capacity) {
    return new BlockCache(capacity);
}

}  // namespace leveldb

