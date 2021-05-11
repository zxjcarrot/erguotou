// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "db/indexed_flat_array.h"
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;

class AbstractMemTable {
public:
    // Return an iterator that yields the contents of the memtable.
    //
    // The caller must ensure that the underlying MemTable remains live
    // while the returned iterator is live.  The keys returned by this
    // iterator are internal keys encoded by AppendInternalKey in the
    // db/format.{h,cc} module.
    virtual Iterator* NewIterator() = 0;
    virtual void Ref() = 0;
    virtual void Unref() = 0;
    virtual size_t ApproximateMemoryUsage() = 0;
    virtual bool Get(const LookupKey& key, std::string* value, Status* s) = 0;
};

class MemTable : public AbstractMemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTable(const InternalKeyComparator& comparator);

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() override {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

 private:
  ~MemTable();  // Private since only Unref() should be used to delete it

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    int operator()(const char* a, const char* b) const;
  };
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;
  friend class CompactConstMemTable;
  typedef SkipList<const char*, KeyComparator> Table;

  KeyComparator comparator_;
  int refs_;
  Arena arena_;
  Table table_;

  // No copying allowed
  MemTable(const MemTable&);
  void operator=(const MemTable&);
};


class CompactConstMemTable : public AbstractMemTable {
public:
    // MemTables are reference counted.  The initial reference count
    // is zero and the caller must call Ref() at least once.
    explicit CompactConstMemTable(MemTable& memtable);

    // Increase reference count.
    void Ref() { ++refs_; }

    // Drop reference count.  Delete if no more references exist.
    void Unref() {
      --refs_;
      assert(refs_ >= 0);
      if (refs_ <= 0) {
        delete this;
      }
    }

    // Returns an estimate of the number of bytes of data in use by this
    // data structure. It is safe to call when MemTable is being modified.
    size_t ApproximateMemoryUsage();

    // Return an iterator that yields the contents of the memtable.
    //
    // The caller must ensure that the underlying MemTable remains live
    // while the returned iterator is live.  The keys returned by this
    // iterator are internal keys encoded by AppendInternalKey in the
    // db/format.{h,cc} module.
    Iterator* NewIterator();

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in *status and return true.
    // Else, return false.
    bool Get(const LookupKey& key, std::string* value, Status* s);

private:
    ~CompactConstMemTable();  // Private since only Unref() should be used to delete it

    typedef MemTable::KeyComparator KeyComparator;

    friend class CompactConstMemTableIterator;
    friend class CompactConstMemTableBackwardIterator;

    typedef IndexedFlatArray<const char*, KeyComparator> Table;

    KeyComparator comparator_;
    int refs_;
    Arena arena_;
    Table * table_;

    // No copying allowed
    CompactConstMemTable(const CompactConstMemTable&);
    void operator=(const CompactConstMemTable&);
};
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
