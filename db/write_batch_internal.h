// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
#define STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

#include "include/write_batch.h"

namespace leveldb {

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
// 真是强迫症啊, 作为 WriteBatch 的 private 方法有何不可?
class WriteBatchInternal {
 public:
  static void PutLargeValueRef(WriteBatch* batch,
                               const Slice& key,
                               const LargeValueRef& large_ref);

  // Return the number of entries in the batch.
  static int Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(WriteBatch* batch, int n);

  // Return the seqeunce number for the start of this batch.
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the seqeunce number for the start of
  // this batch.
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  static Slice Contents(const WriteBatch* batch) {
    return Slice(batch->rep_);
  }

  static size_t ByteSize(const WriteBatch* batch) {
    return batch->rep_.size();
  }

  static void SetContents(WriteBatch* batch, const Slice& contents);

  // 就是将 batch 中记录的更改应用到 memtable 中.
  static Status InsertInto(const WriteBatch* batch, MemTable* memtable);

  // Iterate over the contents of a write batch. 可以参考 write_batch.cc 中 write batch 的格式.
  class Iterator {
   public:
    explicit Iterator(const WriteBatch& batch);
    // 为啥不叫作 Valid(), 与 include/iterator.h 中的接口保持一致嘛.
    bool Done() const { return done_; }
    void Next();
    ValueType op() const { return op_; }
    const Slice& key() const { return key_; }
    const Slice& value() const { return value_; }
    SequenceNumber sequence_number() const { return seq_; }
    Status status() const { return status_; }

   private:
    void GetNextEntry();

    // input_ 存放着 write batch 剩下的未被遍历的内容. input_ 要么为空, 要么处在 write batch record 的开始.
    Slice input_;
    // 若为 true, 则表明已经到达 write batch 末尾, 此时 op_, key_, value_ 无意义.
    // 若为 false, 则表明仍可继续遍历, 此时 op_, key_, value_ 有意义.
    bool done_;
    ValueType op_;
    Slice key_;
    Slice value_;
    SequenceNumber seq_;
    Status status_;
  };
};

}


#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
