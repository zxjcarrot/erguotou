// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUFFER_POOL_H_
#define STORAGE_LEVELDB_DB_BUFFER_POOL_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "db/indexed_flat_array.h"
#include "util/arena.h"
#include "leveldb/cache.h"

namespace leveldb {

struct BlockId {
    uint32_t table_id;
    uint64_t block_offset;
};

BlockId MakeBlockId(uint32_t table_id, uint64_t block_offset) {
    return {table_id, block_offset};
}

struct BlockMeta {
    uint32_t block_len;
    char block[0];
    const size_t Length() const {
        return block_len + sizeof(block_len);
    }
};

class BlockCache {
public:
    BlockCache(size_t cap);

    void SetCapacity(size_t cap);

    void Insert(const BlockId & bid, const BlockMeta * block_meta);

    Cache::Handle* Get(const BlockId & bid, BlockMeta *& block_meta);

    void Put(Cache::Handle * handle);

    void Clear();

private:
    Cache * cache;
};

BlockCache * NewBlockCache(size_t capacity);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_BUFFER_POOL_H_
