// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_INDEXED_FLAT_ARRAY_H_
#define STORAGE_LEVELDB_DB_INDEXED_FLAT_ARRAY_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include <stdint.h>

#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"
#include "skiplist.h"

#include <functional>
namespace leveldb {

class Arena;

template<class Key,
        class Comparator>
class IndexedFlatArray {
public:
    // Create a new SkipList object that will use "cmp" for comparing keys,
    // and will allocate memory using "*arena".  Objects allocated in the arena
    // must remain allocated for the lifetime of the skiplist object.
    explicit IndexedFlatArray(Comparator cmp, Arena * arena) : compare_(cmp), arena_(arena) {}

    static IndexedFlatArray *
    BuildFromSkipList(const SkipList<Key, Comparator> *list,
                      std::function<size_t(char *buf, const Key & payload)> serialize_payload,
                      size_t list_size, Comparator cmp, Arena *arena) {
        IndexedFlatArray *array = new IndexedFlatArray<Key, Comparator>(cmp, arena);
        array->buffer_start = arena->Allocate(list_size);
        if (array->buffer_start == nullptr)
            return nullptr;
        array->buffer_end = array->buffer_start + list_size;
        array->num_keys = 0;
        array->buffer_size = list_size;

        size_t off = 0;
        size_t i = 0;

        typename SkipList<Key, Comparator>::Iterator iter(list);
        iter.SeekToFirst();
        while (iter.Valid()) {
            array->num_keys++;
            iter.Next();
        }

        array->index_array = (uint64_t*)arena->Allocate(array->num_keys * sizeof(uint64_t));
        iter.SeekToFirst();
        while (iter.Valid()) {
            size_t serialized_size = serialize_payload(array->buffer_start + off, iter.key());
            array->index_array[i++] = off;
            off += serialized_size;
            iter.Next();
        }
        assert(i <= array->num_keys);
        assert(off <= list_size);
        fprintf(stderr, "Built IndexedFlat array, buffer size %lu, num keys %lu\n", array->buffer_size, array->num_keys);
        return array;
    }

    // Returns true iff an entry that compares equal to key is in the array.
    bool Contains(const char * key) const {
        Iterator it(this);
        it.Seek(key);
        if (it.Valid() == false)
            return false;
        return compare_(key, it.key()) == 0;
    }

    // Returns ith key where 0 <= i < num_keys
    const char * At(int idx) const {
        assert(0 <= idx && idx < num_keys);
        size_t off = index_array[idx];
        assert(0 <= off && off < buffer_size);
        const char * ret = &buffer_start[off];
        return ret;
    }

    // Iteration over the contents of a skip list
    class Iterator {
    public:
        // Initialize an iterator over the specified list.
        // The returned iterator is not valid.
        explicit Iterator(const IndexedFlatArray *list) : array(list), idx(-1) {}

        // Returns true iff the iterator is positioned at a valid node.
        bool Valid() const {
            return 0 <= idx && idx < array->num_keys;
        }

        // Returns the key at the current position.
        // REQUIRES: Valid()
        const Key key() const {
            return array->At(idx);
        }

        // Advances to the next position.
        // REQUIRES: Valid()
        void Next() {
            idx = idx + 1;
        }

        // Advances to the previous position.
        // REQUIRES: Valid()
        void Prev() {
            idx = idx - 1;
        }

        // Advance to the first entry with a key >= target
        void Seek(const Key &target) {
            int lo = 0;
            int hi = array->num_keys;
            while (lo < hi) {
                int mid = (lo + hi) / 2;
                auto key_mid = array->At(mid);
                if (array->compare_(target, key_mid)) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            idx = lo;
        }

        // Position at the first entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        void SeekToFirst() {
            idx = 0;
        }

        // Position at the last entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        void SeekToLast() {
            idx = array->num_keys - 1;
        }

    private:
        const IndexedFlatArray *array;
        int idx;
        // Intentionally copyable
    };

private:
    enum {
        kMaxHeight = 12
    };

    // Immutable after construction
    Comparator const compare_;
    Arena *const arena_;    // Arena used for allocations of nodes

    char *buffer_start = nullptr;
    const char *buffer_end = nullptr;
    uint64_t *index_array = nullptr;
    size_t num_keys = 0;
    size_t buffer_size = 0;

    // No copying allowed
    IndexedFlatArray(const IndexedFlatArray &);

    void operator=(const IndexedFlatArray &);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_INDEXED_FLAT_ARRAY_H_
