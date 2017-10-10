// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_FORMAT_H_
#define STORAGE_LEVELDB_DB_FORMAT_H_

#include <stdio.h>
#include "include/comparator.h"
#include "include/db.h"
#include "include/slice.h"
#include "include/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
enum ValueType {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1,
  kTypeLargeValueRef = 0x2,
};
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeLargeValueRef;

// kValueTypeForSeek 的语义大概了解了, 可以从 iterator.Seek() 操作的大概实现, InternalKey 的 total
// order 来理解当使用 kTypeDeletion, kTypeValue 作为 kValueTypeForSeek 时会导致哪些 bug, 从而只能使用
// kTypeLargeValueRef 作为 kValueTypeForSeek.
//
// Q: 按我理解 ValueType 是 value 的 metainfo, 为啥要把 ValueType 编码到 InternalKey 中?

// Q: 为啥需要 SequenceNumber, 按我理解是为了实现 mvcc.
typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
//
// leveldb 使用 64 bit 来存放 seqence# 与 valueType. 其中 valuetype 需要 8bit, 所以 sequence# 可以使用
// 56 bit, 所以 max sequence# 是 2 ^ 56 - 1. valuetype, sequence# 具体 layout 参见
// PackSequenceAndType().
static const SequenceNumber kMaxSequenceNumber =
    ((0x1ull << 56) - 1);


// InternalKey, UserKey; 按我理解: InternalKey 与 UserKey 一一对应, InternalKey 在 UserKey 基础之上添加了
// 若干与 UserKey 相关的元信息.
struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;

  ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) { }
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
// 完全可以作为 ParsedInternalKey 的成员函数么这个.
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
extern void AppendInternalKey(std::string* result,
                              const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
extern bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result);

// Returns the user key portion of an internal key.
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

inline ValueType ExtractValueType(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  const size_t n = internal_key.size();
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  return static_cast<ValueType>(c);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_;
 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) { }
  virtual const char* Name() const;
  virtual int Compare(const Slice& a, const Slice& b) const;
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const;
  virtual void FindShortSuccessor(std::string* key) const;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
class InternalKey {
 private:
  std::string rep_;
 public:
  InternalKey() { }   // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  void DecodeFrom(const Slice& s) { rep_.assign(s.data(), s.size()); }
  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  Slice user_key() const { return ExtractUserKey(rep_); }

  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }
};

inline int InternalKeyComparator::Compare(
    const InternalKey& a, const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

// LargeValueRef is a 160-bit hash value (20 bytes), plus an 8 byte
// uncompressed size, and a 1 byte CompressionType code.  An
// encoded form of it is embedded in the filenames of large value
// files stored in the database, and the raw binary form is stored as
// the iter->value() result for values of type kTypeLargeValueRef in
// the table and log files that make up the database.
//
// 按我理解在对 table file, log file 进行遍历时, iter->key() 的类型是 ParsedInternalKey, iter->value()
// 的类型根据 iter->key().type 来定; 如当 iter->key().type 是 kTypeLargeValueRef 时, iter->value() 是
// LargeValueRef 的 raw binary form.
struct LargeValueRef {
  // 按我理解我觉得这里应该把 LargeValueRef 的数据成员拆开放置. 而不是直接放到序列化后的字节数组中.
  char data[29];

  // Initialize a large value ref for the given data
  static LargeValueRef Make(const Slice& data,
                            CompressionType compression_type);

  // Initialize a large value ref from a serialized, 29-byte reference value
  static LargeValueRef FromRef(const Slice& ref) {
    LargeValueRef result;
    assert(ref.size() == sizeof(result.data));
    memcpy(result.data, ref.data(), sizeof(result.data));
    return result;
  }

  // Return the number of bytes in a LargeValueRef (not the
  // number of bytes in the value referenced).
  static size_t ByteSize() { return sizeof(LargeValueRef().data); }

  // Return the number of bytes in the value referenced by "*this".
  uint64_t ValueSize() const { return DecodeFixed64(&data[20]); }

  CompressionType compression_type() const {
    return static_cast<CompressionType>(data[28]);
  }

  bool operator==(const LargeValueRef& b) const {
    return memcmp(data, b.data, sizeof(data)) == 0;
  }
  // 按我理解 operator< 语义上没啥意义.
  bool operator<(const LargeValueRef& b) const {
    return memcmp(data, b.data, sizeof(data)) < 0;
  }
};

// Convert the large value ref to a human-readable string suitable
// for embedding in a large value filename.
// 应该作为成员函数.
extern std::string LargeValueRefToFilenameString(const LargeValueRef& h);

// Parse the large value filename string in "input" and store it in
// "*h".  If successful, returns true.  Otherwise returns false.
// 应该作为类的 static 成员函数.
extern bool FilenameStringToLargeValueRef(const Slice& in, LargeValueRef* ref);

inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);
  // 这里应该使用类似 IsValidValueType() 来判断的.
  return (c <= static_cast<unsigned char>(kTypeLargeValueRef));
}

}

#endif  // STORAGE_LEVELDB_DB_FORMAT_H_
