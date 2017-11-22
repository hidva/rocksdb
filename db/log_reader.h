// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include "db/log_format.h"
#include "include/slice.h"
#include "include/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

/* 当 ReadRecord() 返回 false 时, 若 Corruption() 从未被掉用过, 那么表明这是一次非常成功的 read, 文件的内容全
 * 部被读取消化了. 若 Corruption() 被调用过, 那么表明读取遇到了错误, 可能是底层 io 出错也可能是文件内容不合法,
 * 此时文件内容可能未被消耗完.
 */
class Reader {
 public:
  // Interface for reporting errors.
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    // status 为 Reader 构建的实例用来表明 corruption 的具体情况.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "reporter" is non-NULL, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  // 我本来纳闷 if available 啥意思, checksum 不是总是存在的么! 参见 ReadPhysicalRecord() 的实现可以理解.
  Reader(SequentialFile* file, Reporter* reporter, bool checksum);

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  // 根据实现, 可以看出任何 ioerror 都会被认为是 hit end of the input.
  bool ReadRecord(Slice* record, std::string* scratch);

 private:
  SequentialFile* const file_;
  Reporter* const reporter_;
  bool const checksum_;

  /*
   * 根据 log format 可知, log file 是由 block 组成的. 所以 Reader 的实现也是一次读取一个 block, 然后解析
   * 这个 block.
   *
   * backing_store_ 指向着大小为 kBlockSize 的缓存, 用来存放一个 block 的内容. buffer_ 指向着
   * backing_store_, 为尚未被解析的内容.
   */
  char* const backing_store_;
  Slice buffer_;
  bool eof_;   // Last Read() indicated EOF by returning < kBlockSize

  // Extend record types with the following special values
  enum {
    // kEof 表明 hit the end of file. 本来我以为 kEof 用来标识 block 中最后 7 bytes 形成的 record 呢,
    kEof = kMaxRecordType + 1,
    kBadRecord = kMaxRecordType + 2
  };

  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(Slice* result);
  void ReportDrop(size_t bytes, const char* reason);

  // No copying allowed
  Reader(const Reader&);
  void operator=(const Reader&);
};

}
}

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
