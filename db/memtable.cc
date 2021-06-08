// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "table/merger.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len = 0;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  table_.Insert(buf);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}


CompactConstMemTable::CompactConstMemTable(MemTable& memtable)
        : comparator_(memtable.comparator_.comparator),
          refs_(0),
          table_(nullptr) {
  size_t memtable_size = memtable.ApproximateMemoryUsage();
  auto serialize_payload = [&](char *buf, const char * const & payload) -> size_t {
      // Decode the length of the payload
      const char* p = payload;
      uint32_t key_len, val_len;
      p = GetVarint32Ptr(p, p + 5, &key_len);  // +5: we assume "p" is not corrupted
      p = GetVarint32Ptr(p, p + 5, &val_len);  // +5: we assume "p" is not corrupted
      uint32_t payload_len = p + val_len - payload;
      memcpy(buf, payload, payload_len);
      memtable_size += payload_len;
      return payload_len;
  };
  table_ = Table::BuildFromSkipList(&memtable.table_, serialize_payload,
                                    memtable_size, memtable.comparator_, &this->arena_);
}

CompactConstMemTable::~CompactConstMemTable() {
  assert(refs_ == 0);
}

// Returns an estimate of the number of bytes of data in use by this
// data structure. It is safe to call when MemTable is being modified.
size_t CompactConstMemTable::ApproximateMemoryUsage() {
  return arena_.MemoryUsage();
}


class CompactConstMemTableIterator: public Iterator {
public:
    explicit CompactConstMemTableIterator(CompactConstMemTable::Table* table) : iter_(table) { }

    virtual bool Valid() const { return iter_.Valid(); }
    virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
    virtual void SeekToFirst() { iter_.SeekToFirst(); }
    virtual void SeekToLast() { iter_.SeekToLast(); }
    virtual void Next() { iter_.Next(); }
    virtual void Prev() { iter_.Prev(); }
    virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
    virtual Slice value() const {
      Slice key_slice = GetLengthPrefixedSlice(iter_.key());
      return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
    }

    virtual Status status() const { return Status::OK(); }

private:
    CompactConstMemTable::Table::Iterator iter_;
    std::string tmp_;       // For passing to EncodeKey

    // No copying allowed
    CompactConstMemTableIterator(const CompactConstMemTableIterator&);
    void operator=(const CompactConstMemTableIterator&);
};


Iterator* CompactConstMemTable::NewIterator() {
  return new CompactConstMemTableIterator(table_);
}

bool CompactConstMemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
              return true;
      }
    }
  }
  return false;
}


Iterator* MemTableGroup::NewIterator() {
  std::vector<Iterator *> list;
  for (size_t i = 0; i < tables_.size(); ++i) {
    list.push_back(tables_[i]->NewIterator());
  }
  return NewMergingIterator(&internal_comparator_, &list[0], list.size());
}

void MemTableGroup::Ref() {
  ++refs_;
}

void MemTableGroup::Unref() {
  --refs_;
  assert(refs_ >= 0);
  if (refs_ <= 0) {
    for (size_t i = 0; i < tables_.size(); ++i) {
      tables_[i]->Unref();
    }
    delete this;
  }
}

size_t MemTableGroup::ApproximateMemoryUsage() {
  size_t usage = 0;
  for (size_t i = 0; i < tables_.size(); ++i) {
    usage += tables_[i]->ApproximateMemoryUsage();
  }
  return usage;
}

bool MemTableGroup::Get(const LookupKey& key, std::string* value, Status* s) {
  for (int i = (int)tables_.size() - 1; i >= 0; --i) {
    auto imm = tables_[i];
    if (imm->Get(key, value, s)) {
      return true;
    }
  }
  return false;
}

bool MemTableGroup::GetField(const LookupKey& key, int ith, std::string *value, Status *s) {
  for (size_t i = (int)tables_.size() - 1; i >= 0; --i) {
    auto imm = tables_[i];
    if (imm->GetField(key, ith, value, s)) {
      return true;
    }
  }
  return false;
}

void MemTableGroup::AddMemTable(AbstractMemTable* memtable) {
  tables_.push_back(memtable);
}

}  // namespace leveldb
