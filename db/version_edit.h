// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
  // 严格意义上说, refs 并不是任何 file meta data, 只是用来内存管理的.
  int refs;

  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  FileMetaData() : refs(0), file_size(0) { }
};


// VersionEdit, 参见 version.README.md 中的记录.
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  // Record that a large value with the specified large_ref was
  // written to the output file numbered "fnum"
  void AddLargeValueRef(const LargeValueRef& large_ref,
                        uint64_t fnum,
                        const Slice& internal_key) {
    large_refs_added_.resize(large_refs_added_.size() + 1);
    Large* large = &(large_refs_added_.back());
    large->large_ref = large_ref;
    large->fnum = fnum;
    large->internal_key.DecodeFrom(internal_key);
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  std::string comparator_;
  // log_number_ 是指当前正在使用着的 log file 的 file number, 在打开一个已经存在的 db 时, 会打开
  // log_number_ 指定的 log file 来 redo log. 由于不存在 file number 为 0 的 log file, 所以当
  // log_number_ 为 0 时表明没有对应的日志文件.
  uint64_t log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector< std::pair<int, FileMetaData> > new_files_;
  struct Large {
    LargeValueRef large_ref;
    uint64_t fnum;
    InternalKey internal_key;
  };
  std::vector<Large> large_refs_added_;
};

}

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
