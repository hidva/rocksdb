// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <stdint.h>
#include "include/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
struct Options;
class RandomAccessFile;
struct ReadOptions;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.
// QA: 一个 Table 对应着一个 SST 文件?
// A: YES
class Table {
 public:
  // Attempt to open the table that is stored in "file", and read the
  // metadata entries necessary to allow retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to NULL and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime. 按我理解这里的 source 应该是指参数中的 file 了.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options,
                     RandomAccessFile* file,
                     Table** table);

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // QA: 此时 iter->key() 对应着 InternalKey, iter->value() 根据 InternalKey.valueType 来解析.
  // A: 具体如何解析 iter->key(), iter->value() 由 caller 决定, Table 仅是提供遍历的功能!
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  struct Rep;
  Rep* rep_;

  explicit Table(Rep* rep) { rep_ = rep; }
  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  // No copying allowed
  Table(const Table&);
  void operator=(const Table&);
};

}

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
