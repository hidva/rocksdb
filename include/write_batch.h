// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// 本来我以为 WriteBatch 的 entry 具有相同的 sequence number, 后来发现这样是不行的, 如下:
//
//    batch.Put("key", "v1"); // #1
//    batch.Delete("key");  // #2
//
// 如果 #1, #2 具有相同的 sequence number, 那么当该 WriteBatch 应用到 Memtable 时是没有办法区分出 #1, #2
// 谁先谁后的!
//
// 所以应用 WriteBatch 之后对应 snapshot 的 sequence number 应该是 WriteBatch 中 max sequence number.

#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string>

namespace leveldb {

class Slice;

class WriteBatch {
 public:
  WriteBatch();
  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  void Put(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(const Slice& key);

  // Clear all updates buffered in this batch.
  void Clear();

 private:
  friend class WriteBatchInternal;

  std::string rep_;  // See comment in write_batch.cc for the format of rep_

  // Intentionally copyable
};

}

#endif  // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_